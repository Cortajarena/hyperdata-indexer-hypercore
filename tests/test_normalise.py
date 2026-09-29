"""The normaliser is the validator: the variant sets are closed."""

from __future__ import annotations

import json

import pytest

from conftest import (
    BLOCK_TIME,
    USER,
    book_diff,
    envelope,
    fill,
    line,
    order,
    order_status,
)
from hypercore_indexer.normalise import (
    MAX_ORDER_DEPTH,
    ContractViolation,
    parse_line,
    rows_from_line,
    timestamp,
)


def rows(table: str, raw: bytes, **kwargs) -> list[dict]:
    return rows_from_line(
        raw, table=table, source_file="/corpus/13", log_index=0,
        ingest_ts=kwargs.pop("ingest_ts"), **kwargs,
    )


# -- book diffs: the closed variant set ------------------------------------
@pytest.mark.parametrize(
    "raw_diff, expected",
    [
        ({"new": {"sz": "7.0"}}, ("new", None, "7.0")),
        ({"update": {"origSz": "8.0", "newSz": "0.0"}}, ("update", "8.0", "0.0")),
        ("remove", ("remove", None, None)),
    ],
)
def test_book_diff_variants(raw_diff, expected, ingest_ts):
    out = rows("raw_book_diffs",
               line(envelope(book_diff(raw_diff))), ingest_ts=ingest_ts)
    assert len(out) == 1
    assert (out[0]["diff_type"], out[0]["orig_sz"], out[0]["new_sz"]) == expected


def test_book_diff_unknown_variant_is_a_violation(ingest_ts):
    """A fourth variant must never be guessed at or silently dropped."""
    with pytest.raises(ContractViolation, match="unknown raw_book_diff variant"):
        rows("raw_book_diffs", line(envelope(book_diff("cancel"))),
             ingest_ts=ingest_ts)


def test_book_diff_unknown_shape_is_a_violation(ingest_ts):
    with pytest.raises(ContractViolation, match="unknown raw_book_diff shape"):
        rows("raw_book_diffs", line(envelope(book_diff({"replace": {}}))),
             ingest_ts=ingest_ts)


def test_book_diff_non_string_size_is_a_violation(ingest_ts):
    with pytest.raises(ContractViolation, match="decimal string"):
        rows("raw_book_diffs",
             line(envelope(book_diff({"new": {"sz": 7.0}}))),
             ingest_ts=ingest_ts)


def test_book_diff_real_corpus_line(corpus_line, ingest_ts):
    out = rows("raw_book_diffs", corpus_line, ingest_ts=ingest_ts)
    assert out[0]["oid"] == 464679247366
    assert out[0]["diff_type"] == "update"
    assert out[0]["block_number"] == 1030140001
    assert out[0]["block_time"].isoformat() == "2026-06-10T12:56:15.035481"


# -- envelope --------------------------------------------------------------
def test_envelope_missing_field_is_a_violation(ingest_ts):
    payload = envelope(book_diff("remove"))
    del payload["block_number"]
    with pytest.raises(ContractViolation, match="missing 'block_number'"):
        rows("raw_book_diffs", line(payload), ingest_ts=ingest_ts)


def test_invalid_json_is_a_violation(ingest_ts):
    with pytest.raises(ContractViolation, match="invalid JSON"):
        rows("raw_book_diffs", b'{"local_time": ', ingest_ts=ingest_ts)


def test_non_object_line_is_a_violation(ingest_ts):
    with pytest.raises(ContractViolation, match="expected a JSON object"):
        rows("raw_book_diffs", b"[1,2,3]", ingest_ts=ingest_ts)


def test_timestamps_truncate_to_microseconds():
    """Nanoseconds on the wire; Iceberg keeps microseconds."""
    assert timestamp("2026-06-10T12:56:15.035481067").microsecond == 35481


def test_bad_timestamp_is_a_violation(ingest_ts):
    payload = envelope(book_diff("remove"))
    payload["block_time"] = "yesterday"
    with pytest.raises(ContractViolation, match="bad timestamp"):
        rows("raw_book_diffs", line(payload), ingest_ts=ingest_ts)


# -- log_index -------------------------------------------------------------
def test_log_index_is_per_block_across_lines(ingest_ts):
    """log_index is the event's position in its BLOCK, not in the line."""
    first = rows_from_line(
        line(envelope(book_diff("remove"), book_diff("remove"),
                      block_number=42)),
        table="raw_book_diffs", source_file="f", ingest_ts=ingest_ts,
        log_index=0,
    )
    second = rows_from_line(
        line(envelope(book_diff({"new": {"sz": "1.0"}}), block_number=42)),
        table="raw_book_diffs", source_file="f", ingest_ts=ingest_ts,
        log_index=len(first),
    )
    assert [r["log_index"] for r in first + second] == [0, 1, 2]


# -- fills: the positional tuple -------------------------------------------
def test_fill_tuple_is_flattened(ingest_ts):
    out = rows("fills", line(envelope(fill())), ingest_ts=ingest_ts)
    assert out[0]["user"] == USER
    assert out[0]["coin"] == "LIT"
    assert out[0]["fee_token"] == "USDC"        # camelCase -> snake_case
    assert out[0]["closed_pnl"] == "0.321433"
    assert out[0]["liquidation"] is None


def test_fill_sparse_fields_pass_through(ingest_ts):
    out = rows("fills", line(envelope(
        fill(cloid="0xabc", deployerFee="0.1", twapId=1920480))), ingest_ts=ingest_ts)
    assert out[0]["cloid"] == "0xabc"
    assert out[0]["deployer_fee"] == "0.1"
    assert out[0]["twap_id"] == 1920480


def test_fill_tuple_shape_is_enforced(ingest_ts):
    with pytest.raises(ContractViolation, match="2-element"):
        rows("fills", line(envelope({"coin": "LIT"})), ingest_ts=ingest_ts)


def test_fill_liquidation_object(ingest_ts):
    out = rows("fills", line(envelope(fill(liquidation={
        "liquidatedUser": "0xabc", "markPx": "1.0", "method": "isolated",
    }))), ingest_ts=ingest_ts)
    assert out[0]["liquidation"]["method"] == "isolated"


# -- order statuses --------------------------------------------------------
def test_order_status_camel_to_snake(ingest_ts):
    out = rows("order_statuses", line(envelope(order_status())),
               ingest_ts=ingest_ts)
    row = out[0]["order"]
    assert row["limit_px"] == "1.5"
    assert row["is_position_tpsl"] is False
    assert row["order_type"] == "Limit"
    assert row["trigger_condition"] == "mark"
    assert row["children"] == []
    assert "tif" not in out[0]          # the duplicate event `time` is gone


def test_builder_object_and_null(ingest_ts):
    plain = rows("order_statuses", line(envelope(order_status())),
                 ingest_ts=ingest_ts)[0]
    assert plain["builder"] is None
    charged = rows("order_statuses", line(envelope(
        order_status(builder={"b": "0xdef", "f": 7}))), ingest_ts=ingest_ts)[0]
    assert charged["builder"] == {"b": "0xdef", "f": 7}


def test_builder_wrong_shape_is_a_violation(ingest_ts):
    with pytest.raises(ContractViolation, match="builder must be an object"):
        rows("order_statuses", line(envelope(order_status(builder="0xdef"))),
             ingest_ts=ingest_ts)


def test_children_are_nested_to_the_bound(ingest_ts):
    deep = order(children=[order(oid=2, children=[order(oid=3)])])
    out = rows("order_statuses", line(envelope(order_status(order_obj=deep))),
               ingest_ts=ingest_ts)[0]
    child = out["order"]["children"][0]
    assert child["oid"] == 2
    assert child["children"][0]["oid"] == 3


def test_children_beyond_the_bound_go_to_overflow(ingest_ts):
    """Truncated, never dropped: deeper children stay as JSON."""
    node = order(oid=99)
    for depth in range(MAX_ORDER_DEPTH + 2):
        node = order(oid=depth, children=[node])

    def walk(current, level=0):
        kids = current.get("children")
        if not kids:
            return level
        return walk(kids[0], level + 1)

    out = rows("order_statuses", line(envelope(order_status(order_obj=node))),
               ingest_ts=ingest_ts)[0]
    deepest = out["order"]
    for _ in range(MAX_ORDER_DEPTH - 1):
        deepest = deepest["children"][0]
    assert "children" not in deepest
    recovered = json.loads(deepest["children_overflow_json"])
    assert isinstance(recovered, list) and recovered


def test_unknown_table_is_a_violation(ingest_ts):
    with pytest.raises(ContractViolation, match="unknown table"):
        rows("misc_events", line(envelope()), ingest_ts=ingest_ts)


def test_parse_line_returns_envelope():
    parsed = parse_line(line(envelope(book_diff("remove"))))
    assert parsed["block_time"] == BLOCK_TIME
