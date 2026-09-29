"""Iceberg sink: create the tables from the contract, append to them.

Two things worth knowing about pyiceberg 0.12 that shaped this file:

  * There is NO cross-table transaction. `Catalog` has no `transaction()`; the
    only transaction object is per-table (`Table.transaction()`). So events and
    their block_info rows cannot be committed atomically, and the design leans on
    ordering instead: **events are appended first, block_info last.** A
    block_info row with complete=true is therefore a commit marker that can only
    exist if the events it describes are already durable. A crash in between
    leaves block_info missing for that file, which is exactly what the ledger
    and `has_complete_block_info` use to recover.
  * `Table.append` is append-only and NOT idempotent. Re-appending a file
    duplicates rows, so a re-processed file is only re-appended when we know its
    previous append did not land. The residual duplicate window is the crash
    between the events append and the block_info append; the deduplication for
    that lives downstream in dbt (row_number over
    (source_file, block_number, log_index)), not here.
"""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError

from config import Config

log = logging.getLogger(__name__)

# Hidden partitioning, per the locked design. hours() on the hot tables keeps
# files small enough to stay in the 128-512 MB target at production volume;
# order_statuses is 52 GB/h, so it partitions by day instead.
PARTITION_SPECS = {
    "raw_book_diffs": "hours(block_time)",
    "order_statuses": "days(block_time)",
    "fills": "hours(block_time)",
    "block_info": "days(block_time)",
}


class SinkError(Exception):
    """The warehouse could not be reached or a write failed."""


class IcebergSink:
    def __init__(self, config: Config, schemas: dict[str, pa.Schema]):
        self.config = config
        self.schemas = schemas
        self.catalog: Catalog = self._connect()
        self._ensure_namespace()
        self._ensure_tables()

    def _connect(self) -> Catalog:
        try:
            return load_catalog("hyperdata", **self.config.catalog_properties())
        except Exception as exc:
            raise SinkError(
                f"cannot reach the Iceberg catalog at "
                f"{self.config.catalog_uri}: {exc}"
            ) from exc

    def _ensure_namespace(self) -> None:
        try:
            self.catalog.create_namespace_if_not_exists(self.config.namespace)
        except Exception as exc:
            raise SinkError(
                f"cannot create namespace {self.config.namespace}: {exc}"
            ) from exc

    def _ensure_tables(self) -> None:
        for table, schema in self.schemas.items():
            identifier = f"{self.config.namespace}.{table}"
            try:
                self.catalog.create_table_if_not_exists(
                    identifier,
                    schema=schema,
                    partition_spec=PARTITION_SPECS[table],
                )
                log.info("table ready: %s", identifier)
            except Exception as exc:
                raise SinkError(
                    f"cannot create or verify table {identifier}: {exc}"
                ) from exc

    # -- writes -----------------------------------------------------------
    def append(self, table: str, data: pa.Table) -> int:
        """Append one Arrow table. Returns the number of rows written."""
        if data.num_rows == 0:
            return 0
        identifier = f"{self.config.namespace}.{table}"
        try:
            target = self.catalog.load_table(identifier)
        except NoSuchTableError as exc:
            raise SinkError(f"table {identifier} does not exist") from exc
        try:
            target.append(data)
        except Exception as exc:
            raise SinkError(f"append to {identifier} failed: {exc}") from exc
        log.info("appended %s rows to %s", f"{data.num_rows:,}", identifier)
        return data.num_rows

    # -- recovery ---------------------------------------------------------
    def has_complete_block_info(self, source_file: str) -> bool:
        """True if block_info already holds a completed block for this file.

        Used to close the crash window: block_info is written after the events,
        so a complete row proves the events landed. block_info is one row per
        block — a tiny table — so this check is cheap even though it filters on
        a non-partition column.
        """
        identifier = f"{self.config.namespace}.block_info"
        try:
            target = self.catalog.load_table(identifier)
        except NoSuchTableError:
            return False
        try:
            scanned = target.scan(
                row_filter=f"source_file = '{source_file}' AND complete",
                selected_fields=("block_number", "complete"),
            ).to_arrow()
        except Exception as exc:
            log.warning("block_info recovery check failed for %s: %s",
                        source_file, exc)
            return False
        return scanned.num_rows > 0

    def block_info_rows_for(self, source_file: str) -> int:
        identifier = f"{self.config.namespace}.block_info"
        try:
            target = self.catalog.load_table(identifier)
        except NoSuchTableError:
            return 0
        try:
            return target.scan(
                row_filter=f"source_file = '{source_file}'"
            ).to_arrow().num_rows
        except Exception:
            return 0

    def close(self) -> None:
        close = getattr(self.catalog, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> IcebergSink:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()
