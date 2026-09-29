"""Configuration — environment in, frozen dataclass out.

Every knob is an environment variable so the service runs unchanged under
compose, docker run and a shell. Nothing is read at import time: a bad value
should fail at startup with a clear message, not on the first file.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Node output table -> Iceberg table. One file tree per table, so the indexer
# processes one table per run; they are independent streams with different
# rates (book diffs 15.6k lines/s, fills 200/s).
TABLES = {
    "raw_book_diffs": "node_raw_book_diffs_streaming",
    "order_statuses": "node_order_statuses_streaming",
    "fills": "node_fills_streaming",
}


class ConfigError(Exception):
    """A configuration value is missing or unusable."""


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise ConfigError(f"{name} is required but not set")
    return value


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from None


@dataclass(frozen=True)
class Config:
    # --- source -----------------------------------------------------------
    node_dir: Path
    table: str
    # Hour-files already ingested, keyed by (path, sha256). See ledger.py.
    state_dir: Path
    # Where contract violations go. A rejected line is never dropped silently.
    dlq_dir: Path

    # --- contract ---------------------------------------------------------
    # Directory holding the generated Arrow schema package, i.e.
    # platform/schemas/generated/arrow (mounted read-only in production).
    schema_dir: Path

    # --- warehouse --------------------------------------------------------
    catalog_uri: str
    warehouse: str
    namespace: str = "hypercore"
    s3_endpoint: str | None = None
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None

    # --- write behaviour --------------------------------------------------
    # Commit when the buffer reaches this many bytes. 100-500 MB is the
    # platform-wide target: big enough to avoid thousands of small files,
    # small enough that a failed run loses little work.
    buffer_bytes: int = 256 * 1024 * 1024
    # Only commit files whose seal is known (i.e. the hour is over). Live
    # ingestion of the current hour file is a later mode.
    finalized_only: bool = True
    # Hard stop for a single file, so one pathological hour cannot exhaust
    # memory. 0 disables the cap.
    max_rows_per_file: int = 0

    extra: dict = field(default_factory=dict)

    @property
    def table_dir(self) -> Path:
        """The node's output tree for the configured table."""
        return self.node_dir / TABLES[self.table]

    @property
    def ledger_path(self) -> Path:
        return self.state_dir / "ingested.jsonl"

    def catalog_properties(self) -> dict:
        """Properties for pyiceberg's REST catalog."""
        props = {
            "type": "rest",
            "uri": self.catalog_uri,
            "warehouse": self.warehouse,
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        }
        if self.s3_endpoint:
            props["s3.endpoint"] = self.s3_endpoint
        if self.s3_access_key_id:
            props["s3.access-key-id"] = self.s3_access_key_id
        if self.s3_secret_access_key:
            props["s3.secret-access-key"] = self.s3_secret_access_key
        return props


def from_env() -> Config:
    node_dir = Path(_env("NODE_DIR"))
    table = _env("TABLE")
    if table not in TABLES:
        raise ConfigError(
            f"TABLE must be one of {sorted(TABLES)}, got {table!r}"
        )
    return Config(
        node_dir=node_dir,
        table=table,
        state_dir=Path(os.environ.get("STATE_DIR", "/state")),
        dlq_dir=Path(os.environ.get("DLQ_DIR", "/state/dlq")),
        schema_dir=Path(
            os.environ.get("SCHEMA_DIR", "/schemas/generated/arrow")
        ),
        catalog_uri=_env("CATALOG_URI"),
        warehouse=_env("WAREHOUSE"),
        namespace=os.environ.get("NAMESPACE", "hypercore"),
        s3_endpoint=os.environ.get("S3_ENDPOINT"),
        s3_access_key_id=os.environ.get("S3_ACCESS_KEY_ID"),
        s3_secret_access_key=os.environ.get("S3_SECRET_ACCESS_KEY"),
        buffer_bytes=_int("BUFFER_BYTES", 256 * 1024 * 1024),
        finalized_only=os.environ.get("FINALIZED_ONLY", "true").lower()
        not in ("0", "false", "no"),
        max_rows_per_file=_int("MAX_ROWS_PER_FILE", 0),
    )
