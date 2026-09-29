"""Row buffer: accumulate rows, hand over an Arrow table at a size threshold.

Column-wise on purpose. `pa.table({name: pa.array(col, type=…)})` builds each
column in one vectorised pass; building a dict per row and handing it to
`Table.from_pylist` was measured several times slower on the real corpus, and
this is the hot path (~40k lines/s across the three tables).

Size tracking is an estimate, not an exact Arrow footprint: it only decides
when to commit, and a wrong guess costs a slightly smaller or larger file, never
correctness. Counting string lengths as we go avoids serialising each row twice.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

# Rough per-value byte weights for the estimate. Timestamps and 64-bit ints are
# 8; bools 1; variable-length values are measured. Nested structs and lists use
# a flat estimate per child — they are rare enough (order_statuses' `order`,
# fills' `liquidation`) that precision buys nothing.
FIXED_BYTES = 8
BOOL_BYTES = 1
NESTED_ESTIMATE = 96


def weigh(value: Any) -> int:
    if value is None:
        return 1
    if isinstance(value, bool):
        return BOOL_BYTES
    if isinstance(value, int):
        return FIXED_BYTES
    if isinstance(value, float):
        return 8
    if isinstance(value, str):
        return len(value) + 16          # value + Arrow offset overhead
    if isinstance(value, (dict, list, tuple)):
        return NESTED_ESTIMATE
    return FIXED_BYTES


class RowBuffer:
    """Accumulates rows for one table, flushed as a whole Arrow table."""

    def __init__(self, schema: pa.Schema, target_bytes: int):
        self.schema = schema
        self.target_bytes = max(target_bytes, 1)
        self._columns: dict[str, list] = {name: [] for name in schema.names}
        self._rows = 0
        self._bytes = 0

    def __len__(self) -> int:
        return self._rows

    @property
    def rows(self) -> int:
        return self._rows

    @property
    def approx_bytes(self) -> int:
        return self._bytes

    @property
    def full(self) -> bool:
        return self._rows > 0 and self._bytes >= self.target_bytes

    def add(self, row: dict) -> None:
        """Append one contract row. Unknown keys are dropped, not merged.

        A row carrying a field the contract does not declare would be a
        contract violation upstream; silently dropping it here would hide that.
        The normaliser is responsible for emitting exactly the contract's keys.
        """
        for name, values in self._columns.items():
            value = row.get(name)
            values.append(value)
            self._bytes += weigh(value)
        self._rows += 1

    def extend(self, rows: Iterator[dict]) -> None:
        for row in rows:
            self.add(row)

    def to_table(self) -> pa.Table:
        """Materialise the buffered rows. The buffer is left empty."""
        if not self._rows:
            return self.schema.empty_table()
        table = pa.table(
            {name: pa.array(values, type=self.schema.field(name).type)
             for name, values in self._columns.items()},
            schema=self.schema,
        )
        self.reset()
        return table

    def reset(self) -> None:
        self._columns = {name: [] for name in self.schema.names}
        self._rows = 0
        self._bytes = 0
