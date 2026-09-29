"""Pipeline: discover hour-files, normalise, buffer, commit, record.

Order of operations per file, and why:

  1. sha256 the file; skip if the ledger says (path, sha256) is committed.
  2. ledger.mark_started — written BEFORE any append, so a crash is visible.
  3. append events (buffered; flushed at 256 MB or at end of file).
  4. append block_info LAST, with complete=true on every block of a sealed
     file. pyiceberg has no cross-table transaction, so ordering is what makes
     this safe: a complete block_info row is the proof that the events it
     describes are already durable.
  5. ledger.mark_committed.

A file whose events appended but whose block_info did not (crash between 3 and
4) is detected on the next run: block_info has no complete row for it, so it is
re-processed. The residual duplicate window is that one, and dbt deduplicates
on (source_file, block_number, log_index).

The log_index counter is per block and spans lines, so it lives in the file
walker, not in the normaliser: a block's events arrive across many lines.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import pathlib
import time
from dataclasses import dataclass, field

import orjson
import pyarrow as pa

from .buffer import RowBuffer
from .config import Config
from .contract import Contract
from .ledger import FileRecord, Ledger
from .normalise import ContractViolation, block_info_row, rows_from_line

log = logging.getLogger(__name__)

READ_CHUNK = 1 << 20          # 1 MiB: hashing reads the file once, streaming


@dataclass
class FileResult:
    path: str
    rows: int
    blocks: int
    rejected: int
    skipped: bool = False
    recovered: bool = False


@dataclass
class RunSummary:
    files: int = 0
    skipped: int = 0
    rows: int = 0
    blocks: int = 0
    rejected: int = 0
    started_at: float = field(default_factory=time.time)

    def as_dict(self) -> dict:
        return {
            "files": self.files,
            "skipped": self.skipped,
            "rows": self.rows,
            "blocks": self.blocks,
            "rejected": self.rejected,
            "seconds": round(time.time() - self.started_at, 2),
        }


class Pipeline:
    def __init__(self, config: Config, contract: Contract, sink, ledger: Ledger):
        self.config = config
        self.contract = contract
        self.sink = sink
        self.ledger = ledger
        self.dlq_dir = config.dlq_dir / config.table
        self._block_index: dict[int, int] = {}

    # -- discovery --------------------------------------------------------
    def discover(self) -> list[pathlib.Path]:
        """Hour-files for this table, oldest first.

        The node writes hourly/<YYYYMMDD>/<HH> and finalises at rollover, so an
        hour directory is either sealed or current. With FINALIZED_ONLY (the
        default) the current hour is skipped: it is still being appended to, and
        ingesting a growing file would mean re-reading it from the ledger's byte
        offset — the live mode, which is a later step.
        """
        root = self.config.table_dir
        if not root.is_dir():
            raise FileNotFoundError(f"no output tree for table: {root}")
        cutoff = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        files: list[pathlib.Path] = []
        for date_dir in sorted(root.iterdir()):
            if not date_dir.is_dir():
                continue
            for hour_dir in sorted(date_dir.iterdir()):
                if not hour_dir.is_dir():
                    continue
                if self.config.finalized_only:
                    stamp = _hour_stamp(date_dir.name, hour_dir.name)
                    if stamp is not None and stamp >= cutoff:
                        log.info("skipping current hour: %s", hour_dir)
                        continue
                files.extend(sorted(p for p in hour_dir.iterdir()
                                    if p.is_file()))
        return files

    # -- one file ---------------------------------------------------------
    def process_file(self, path: pathlib.Path) -> FileResult:
        digest = sha256(path)
        record = FileRecord(
            path=str(path),
            sha256=digest,
            table=self.config.table,
            size=path.stat().st_size,
            hour=path.parent.name,
        )
        if self.ledger.is_committed(record.path, record.sha256):
            log.info("already committed, skipping: %s", path)
            return FileResult(record.path, 0, 0, 0, skipped=True)

        recovered = False
        if self.ledger.uncommitted().get(record.path) and \
                self.sink.has_complete_block_info(record.path):
            # A previous run appended the events and died before block_info
            # was visible... or after. Either way a complete block_info row
            # proves the events are durable, so only the marker is missing.
            log.warning("recovering: events already durable for %s", path)
            recovered = True
        else:
            self.ledger.mark_started(record)

        result = FileResult(record.path, 0, 0, 0, recovered=recovered)
        if not recovered:
            result = self._ingest(path, record)
        self.ledger.mark_committed(record, result.rows, result.blocks)
        return result

    def _ingest(self, path: pathlib.Path, record: FileRecord) -> FileResult:
        self._block_index.clear()
        buffer_ = RowBuffer(self.contract.arrow, self.config.buffer_bytes)
        blocks = _BlockStats()
        rows_written = 0
        rejected = 0
        first_seen = dt.datetime.now(dt.UTC).replace(tzinfo=None)

        with path.open("rb") as handle:
            for number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                try:
                    envelope = orjson.loads(line)
                    block_number = envelope["block_number"]
                    log_index = self._block_index.get(block_number, 0)
                    rows = rows_from_line(
                        line,
                        table=self.config.table,
                        source_file=record.path,
                        ingest_ts=first_seen,
                        log_index=log_index,
                    )
                except (ContractViolation, KeyError, TypeError) as exc:
                    rejected += 1
                    self._dead_letter(path, number, line, exc)
                    continue

                self._block_index[block_number] = log_index + len(rows)
                if self.config.max_rows_per_file and \
                        buffer_.rows >= self.config.max_rows_per_file:
                    log.warning("row cap reached for %s, stopping early",
                                path)
                    break
                for row in rows:
                    blocks.observe(row)
                    buffer_.add(row)
                if buffer_.full:
                    rows_written += self.sink.append(self.config.table,
                                                     buffer_.to_table())

        if buffer_.rows:
            rows_written += self.sink.append(self.contract.table,
                                             buffer_.to_table())
        block_rows = blocks.to_rows(self.config.table, record.path, first_seen,
                                    complete=self.config.finalized_only)
        self.sink.append("block_info",
                         self._table(self.contract.block_info, block_rows))
        return FileResult(record.path, rows_written, len(block_rows), rejected)

    @staticmethod
    def _table(schema: pa.Schema, rows: list[dict]) -> pa.Table:
        return pa.table(
            {name: [r.get(name) for r in rows] for name in schema.names},
            schema=schema,
        )

    def _dead_letter(self, path: pathlib.Path, number: int, line: bytes,
                     exc: Exception) -> None:
        """Never drop a rejected line silently.

        The file is JSONL, so the dead-letter file is JSONL too: one object per
        rejected line, carrying enough to find it again in the source and to
        see why it was rejected.
        """
        self.dlq_dir.mkdir(parents=True, exist_ok=True)
        target = self.dlq_dir / f"{path.parent.parent.name}-{path.parent.name}.jsonl"
        with target.open("a", encoding="utf-8") as handle:
            handle.write(orjson.dumps({
                "source_file": str(path),
                "line_number": number,
                "error": str(exc),
                "line": line.decode("utf-8", errors="replace")[:2000],
            }).decode() + "\n")

    # -- run --------------------------------------------------------------
    def run(self) -> RunSummary:
        summary = RunSummary()
        files = self.discover()
        log.info("discovered %d file(s) for %s", len(files), self.config.table)
        for path in files:
            result = self.process_file(path)
            summary.files += 1
            summary.rows += result.rows
            summary.blocks += result.blocks
            summary.rejected += result.rejected
            if result.skipped:
                summary.skipped += 1
            log.info(
                "file=%s rows=%d blocks=%d rejected=%d skipped=%s",
                result.path, result.rows, result.blocks, result.rejected,
                result.skipped,
            )
        log.info("run summary: %s", orjson.dumps(summary.as_dict()).decode())
        return summary


class _BlockStats:
    """Per-block aggregates, emitted as block_info rows at end of file."""

    def __init__(self) -> None:
        self._blocks: dict[int, dict] = {}

    def observe(self, row: dict) -> None:
        number = row["block_number"]
        entry = self._blocks.get(number)
        if entry is None:
            self._blocks[number] = {
                "block_time": row["block_time"],
                "first_local_time": row["local_time"],
                "last_local_time": row["local_time"],
                "event_count": 1,
                "log_index_min": row["log_index"],
                "log_index_max": row["log_index"],
            }
            return
        entry["last_local_time"] = row["local_time"]
        entry["event_count"] += 1
        entry["log_index_max"] = row["log_index"]

    def to_rows(self, table: str, source_file: str, first_seen: dt.datetime,
                complete: bool) -> list[dict]:
        return [
            block_info_row(
                table=table,
                block_number=number,
                block_time=stats["block_time"],
                first_local_time=stats["first_local_time"],
                last_local_time=stats["last_local_time"],
                event_count=stats["event_count"],
                log_index_min=stats["log_index_min"],
                log_index_max=stats["log_index_max"],
                first_seen_at=first_seen,
                complete=complete,
            )
            for number, stats in sorted(self._blocks.items())
        ]


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _hour_stamp(date_dir: str, hour_dir: str) -> dt.datetime | None:
    try:
        return dt.datetime.strptime(f"{date_dir}{hour_dir}", "%Y%m%d%H")
    except ValueError:
        return None
