"""Buffer, ledger and contract-loading behaviour."""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest

from conftest import envelope, fill, line, order, order_status
from hypercore_indexer.buffer import RowBuffer, weigh
from hypercore_indexer.contract import IDENTITY_FIELDS, ContractError, load
from hypercore_indexer.ledger import FileRecord, Ledger
from hypercore_indexer.normalise import rows_from_line

INGEST_TS = dt.datetime(2026, 9, 29, 12, 0, 0)


# -- contract --------------------------------------------------------------
def test_contract_loads_and_has_identity_columns(schema_dir):
    for table in ("raw_book_diffs", "order_statuses", "fills"):
        contract = load(schema_dir, table)
        missing = [f for f in IDENTITY_FIELDS if f not in contract.field_names]
        assert not missing, f"{table} is missing {missing}"


def test_contract_missing_dir_is_explicit(tmp_path):
    with pytest.raises(ContractError, match="schema dir not found"):
        load(tmp_path / "nope", "fills")


def test_block_info_carries_source_file(schema_dir):
    """Needed for crash recovery: a complete row identifies its file."""
    assert "source_file" in load(schema_dir, "fills").block_info.names


# -- normalise -> contract round trip --------------------------------------
def test_rows_fit_the_generated_schema(schema_dir):
    """The real test of the contract: wire rows must satisfy the Arrow schema.

    pyarrow raises on a type mismatch, so building the table from rows produced
    by the normaliser is an assertion that the two agree.
    """
    contract = load(schema_dir, "fills")
    rows = rows_from_line(
        line(envelope(fill())), table="fills", source_file="/c/13",
        ingest_ts=INGEST_TS, log_index=0,
    )
    table = pa.table(
        {name: [r.get(name) for r in rows] for name in contract.arrow.names},
        schema=contract.arrow,
    )
    assert table.schema == contract.arrow
    assert table.num_rows == 1


def test_nested_order_fits_the_generated_schema(schema_dir):
    contract = load(schema_dir, "order_statuses")
    child = order(children=[])
    parent = order(children=[child])
    rows = rows_from_line(
        line(envelope(order_status(order_obj=parent))),
        table="order_statuses", source_file="/c/13", ingest_ts=INGEST_TS,
        log_index=0,
    )
    table = pa.table(
        {name: [r.get(name) for r in rows] for name in contract.arrow.names},
        schema=contract.arrow,
    )
    assert table.schema == contract.arrow
    assert table.num_rows == 1


# -- buffer ----------------------------------------------------------------
def _schema() -> pa.Schema:
    return pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])


def test_buffer_round_trips_and_resets():
    buffer = RowBuffer(_schema(), target_bytes=1 << 20)
    buffer.add({"a": 1, "b": "x"})
    buffer.add({"a": 2, "b": "y"})
    table = buffer.to_table()
    assert table.num_rows == 2
    assert table.column("a").to_pylist() == [1, 2]
    assert len(buffer) == 0
    assert buffer.to_table().num_rows == 0


def test_buffer_reports_full_on_the_threshold():
    buffer = RowBuffer(_schema(), target_bytes=64)
    assert not buffer.full
    for i in range(50):
        buffer.add({"a": i, "b": "x" * 16})
    assert buffer.full
    assert buffer.approx_bytes > 64


def test_buffer_ignores_unknown_keys():
    buffer = RowBuffer(_schema(), target_bytes=1 << 20)
    buffer.add({"a": 1, "b": "x", "surprise": "dropped"})
    assert buffer.to_table().column_names == ["a", "b"]


def test_weigh_orders_sizes():
    assert weigh(None) < weigh(1)
    assert weigh("abc") < weigh("a" * 100)


# -- ledger ----------------------------------------------------------------
def _record(path: str = "/corpus/13", sha: str = "abc") -> FileRecord:
    return FileRecord(path=path, sha256=sha, table="fills", size=10, hour="13")


def test_ledger_round_trips(tmp_path):
    path = tmp_path / "ingested.jsonl"
    ledger = Ledger(path)
    record = _record()
    ledger.mark_started(record)
    assert ledger.uncommitted()[record.path]["sha256"] == "abc"
    ledger.mark_committed(record, rows=5, block_info_rows=1)
    assert ledger.is_committed(record.path, record.sha256)
    assert not ledger.uncommitted()


def test_ledger_reloads_from_disk(tmp_path):
    path = tmp_path / "ingested.jsonl"
    Ledger(path).mark_committed(_record(), rows=5, block_info_rows=1)
    assert Ledger(path).is_committed("/corpus/13", "abc")


def test_ledger_is_keyed_by_content_not_path(tmp_path):
    """A reorged hour has the same path and different bytes: not committed."""
    ledger = Ledger(tmp_path / "l.jsonl")
    ledger.mark_committed(_record(sha="old"), rows=1, block_info_rows=1)
    assert ledger.is_committed("/corpus/13", "old")
    assert not ledger.is_committed("/corpus/13", "new")


def test_ledger_ignores_blank_lines(tmp_path):
    path = tmp_path / "l.jsonl"
    path.write_text("\n\n")
    assert Ledger(path).committed_count() == 0


def test_ledger_rejects_corrupt_lines(tmp_path):
    from hypercore_indexer.ledger import LedgerError

    path = tmp_path / "l.jsonl"
    path.write_text("{not json}\n")
    with pytest.raises(LedgerError, match="not valid JSON"):
        Ledger(path)
