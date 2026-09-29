"""Loading the generated row contract.

The schemas live in `platform/schemas/generated/arrow/hypercore_arrow/`, one
module per table, each exposing `SCHEMA`. They are generated from the .proto
contract and committed, so the service reads them from a mounted path rather
than importing from a sibling repo — a mount is a deployment concern, a
git dependency is not.

This module is the only place that knows where the contract lives and what it
must contain. Everything downstream takes a `pa.Schema`.
"""

from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

# Present in every event row, per the contract's IDENTITY_FIELDS. Checked here
# so a regenerated schema that lost one fails loudly at startup instead of
# producing rows that cannot be traced back to a file.
IDENTITY_FIELDS = (
    "block_number",
    "block_time",
    "local_time",
    "log_index",
    "source_file",
    "ingest_ts",
)


class ContractError(Exception):
    """The mounted contract is missing, incomplete or not the expected shape."""


@dataclass(frozen=True)
class Contract:
    table: str
    arrow: pa.Schema
    block_info: pa.Schema

    @property
    def field_names(self) -> list[str]:
        return list(self.arrow.names)


def load(schema_dir: Path, table: str) -> Contract:
    """Import the generated schemas for `table` from `schema_dir`."""
    package_root = Path(schema_dir)
    if not package_root.is_dir():
        raise ContractError(
            f"schema dir not found: {package_root}. It must contain the "
            "generated package hypercore_arrow/ — mount "
            "platform/schemas/generated/arrow, or set SCHEMA_DIR."
        )
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

    try:
        module = importlib.import_module(f"hypercore_arrow.{table}")
        info = importlib.import_module("hypercore_arrow.block_info")
    except ImportError as exc:
        raise ContractError(
            f"cannot import the generated contract from {package_root}: {exc}"
        ) from exc

    arrow: pa.Schema = module.SCHEMA
    missing = [f for f in IDENTITY_FIELDS if f not in arrow.names]
    if missing:
        raise ContractError(
            f"generated schema for {table} is missing identity column(s): "
            f"{', '.join(missing)}"
        )
    return Contract(table=table, arrow=arrow, block_info=info.SCHEMA)
