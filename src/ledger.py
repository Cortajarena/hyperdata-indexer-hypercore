"""The ingest ledger: which files are already in the warehouse.

Idempotency lives here, at FILE granularity, keyed by (path, sha256). That
choice is the locked design (docs/ingestion.md, "Identity & idempotency"):

  * Re-running over the same file is a no-op, because the ledger says so.
  * A reorg's *new* file is appended rather than deduped away, because the key
    is the immutable artifact, not the mutable chain coordinate.
  * Deduplicating rows on (block_number, log_index) would silently discard the
    canonical block after a reorg — the same block numbers come back with
    different content, and there is no block hash in the L1 output to tell them
    apart.

Two record kinds, appended to one JSONL file:

    {"kind": "started",  "path": …, "sha256": …, "table": …, …}
    {"kind": "committed", "path": …, "sha256": …, "rows": N, …}

A `started` with no matching `committed` means a run died mid-file. Recovery is
the pipeline's job: it either finds the file's block_info already complete (the
events landed, just not the marker) or re-processes it. Writes are flushed and
fsynced, because a ledger that loses its last line silently re-ingests a file.
"""

from __future__ import annotations

import json
import os
import pathlib
import time
from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class FileRecord:
    path: str
    sha256: str
    table: str
    size: int
    hour: str
    started_at: float = field(default_factory=time.time)


class LedgerError(Exception):
    """The ledger file exists but cannot be read."""


class Ledger:
    def __init__(self, path: pathlib.Path):
        self.path = pathlib.Path(path)
        self._committed: set[tuple[str, str]] = set()
        self._started: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            with self.path.open(encoding="utf-8") as handle:
                for number, line in enumerate(handle, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise LedgerError(
                            f"{self.path}:{number} is not valid JSON: {exc}"
                        ) from exc
                    key = (entry.get("path"), entry.get("sha256"))
                    if entry.get("kind") == "started":
                        self._started[entry["path"]] = entry
                    elif entry.get("kind") == "committed":
                        self._committed.add(key)
                        self._started.pop(entry["path"], None)
        except OSError as exc:
            raise LedgerError(f"cannot read ledger {self.path}: {exc}") from exc

    # -- queries ----------------------------------------------------------
    def is_committed(self, path: str, sha256: str) -> bool:
        return (path, sha256) in self._committed

    def uncommitted(self) -> dict[str, dict]:
        """Files whose ingestion started but never committed."""
        return dict(self._started)

    def committed_count(self) -> int:
        return len(self._committed)

    # -- writes -----------------------------------------------------------
    def mark_started(self, record: FileRecord) -> None:
        self._append({"kind": "started", **asdict(record)})
        self._started[record.path] = asdict(record)

    def mark_committed(self, record: FileRecord, rows: int,
                       block_info_rows: int) -> None:
        self._append({
            "kind": "committed",
            **asdict(record),
            "rows": rows,
            "block_info_rows": block_info_rows,
            "committed_at": time.time(),
        })
        self._committed.add((record.path, record.sha256))
        self._started.pop(record.path, None)

    def _append(self, entry: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
