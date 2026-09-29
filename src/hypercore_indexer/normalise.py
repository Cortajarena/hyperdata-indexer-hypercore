"""Wire line -> contract rows.

The node writes newline-delimited JSON, one object per line:

    {"local_time": …, "block_time": …, "block_number": N, "events": [ … ]}

A LINE is the wire unit; an EVENT is the row unit. This module broadcasts the
envelope onto every event and normalises the three wire shapes that a
declarative validator cannot express:

  * the book-diff union  {"new":{sz}} | {"update":{origSz,newSz}} | "remove"
  * the fills tuple      ["0x…", {payload}]     (user is element 0)
  * recursive child orders, bounded by MAX_ORDER_DEPTH

Why normalise in code rather than validate against a schema: Arrow's JSON
reader rejects the union outright (an object that becomes a string mid-file
fails the whole read) and cannot read the tuple, and draft-07 JSON Schema has
no way to say "array whose element 0 is a string and element 1 is an object".
So this module IS the validator. Its contract obligation is precise: the
variant sets are CLOSED. An unknown book-diff variant is a contract violation
-> ContractViolation -> dead-letter + alert. Never a silent drop, and never a
best-effort guess, because a mis-typed row is worse than a missing one.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import orjson

# Recursion bound for child orders, mirroring MAX_ORDER_DEPTH in the schema
# generator. The contract's Order struct is unrolled to this depth; anything
# deeper is preserved verbatim in children_overflow_json.
MAX_ORDER_DEPTH = 3

DIFF_TYPES = ("new", "update", "remove")

# The order object arrives in the node's camelCase. Mapped explicitly rather
# than by a generic converter: the column names are the contract, so the
# mapping is part of it and should be readable in one place.
ORDER_KEYS = {
    "oid": "oid",
    "coin": "coin",
    "side": "side",
    "limitPx": "limit_px",
    "sz": "sz",
    "origSz": "orig_sz",
    "timestamp": "timestamp",
    "orderType": "order_type",
    "tif": "tif",
    "cloid": "cloid",
    "triggerCondition": "trigger_condition",
    "triggerPx": "trigger_px",
    "isTrigger": "is_trigger",
    "isPositionTpsl": "is_position_tpsl",
    "reduceOnly": "reduce_only",
}

# Keys the node may add later. Unknown keys are reported once, not fatal: a new
# optional field must not dead-letter every row, but it must not pass silently
# either — the contract is a promise, and a breach shows up in the logs.
_reported_unknown: set[tuple[str, str]] = set()


def _report_unknown(kind: str, keys: set) -> None:
    import logging

    for key in sorted(keys):
        if (kind, key) not in _reported_unknown:
            _reported_unknown.add((kind, key))
            logging.getLogger(__name__).warning(
                "contract: %s has unmapped key %r — dropped from the row; add "
                "it to the .proto contract", kind, key,
            )


class ContractViolation(Exception):
    """The data does not match the contract. Goes to the dead-letter queue."""


def parse_line(line: bytes) -> dict:
    """One JSONL line -> envelope dict."""
    try:
        envelope = orjson.loads(line)
    except orjson.JSONDecodeError as exc:
        raise ContractViolation(f"invalid JSON: {exc}") from exc
    if not isinstance(envelope, dict):
        raise ContractViolation(
            f"expected a JSON object per line, got {type(envelope).__name__}"
        )
    for field in ("block_number", "block_time", "local_time", "events"):
        if field not in envelope:
            raise ContractViolation(f"envelope is missing {field!r}")
    if not isinstance(envelope["events"], list):
        raise ContractViolation("envelope 'events' must be a list")
    return envelope


def timestamp(value: str) -> dt.datetime:
    """ISO-8601 (nanoseconds on the wire) -> microsecond datetime.

    Microseconds because that is Iceberg's timestamp precision; the truncated
    sub-microsecond digits are never load-bearing for a 15.6 blocks/s chain.
    Naive on purpose: pyarrow stores these as timestamp[us, tz=UTC].
    """
    if not isinstance(value, str):
        raise ContractViolation(f"timestamp must be a string, got {value!r}")
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ContractViolation(f"bad timestamp {value!r}: {exc}") from exc


def rows_from_line(
    line: bytes,
    table: str,
    source_file: str,
    ingest_ts: dt.datetime,
    log_index: int,
) -> list[dict]:
    """One wire line -> zero or more contract rows.

    `log_index` is the caller's running counter for the block: the position of
    the first event of this line within its block. Returned rows continue from
    there, so a line carrying N events occupies N consecutive indexes.
    """
    envelope = parse_line(line)
    if table == "fills":
        build = _fill_row
    elif table == "raw_book_diffs":
        build = _book_diff_row
    elif table == "order_statuses":
        build = _order_status_row
    else:
        raise ContractViolation(f"unknown table {table!r}")

    block_number = envelope["block_number"]
    block_time = timestamp(envelope["block_time"])
    local_time = timestamp(envelope["local_time"])
    rows: list[dict] = []
    for event in envelope["events"]:
        rows.append(
            build(
                event,
                block_number=block_number,
                block_time=block_time,
                local_time=local_time,
                source_file=source_file,
                ingest_ts=ingest_ts,
                log_index=log_index + len(rows),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# per-table wire shapes
# ---------------------------------------------------------------------------
def _book_diff_row(event: Any, **envelope: Any) -> dict:
    if not isinstance(event, dict):
        raise ContractViolation(
            f"book diff event must be an object, got {type(event).__name__}"
        )
    raw = event.get("raw_book_diff")
    diff_type, orig_sz, new_sz = _normalise_diff(raw)
    return {
        **envelope,
        "user": event.get("user"),
        "oid": event.get("oid"),
        "coin": event.get("coin"),
        "side": event.get("side"),
        "px": event.get("px"),
        "diff_type": diff_type,
        "orig_sz": orig_sz,
        "new_sz": new_sz,
    }


def _normalise_diff(raw: Any) -> tuple[str, str | None, str | None]:
    """The closed variant set: new | update | remove."""
    if isinstance(raw, str):
        if raw not in DIFF_TYPES:
            raise ContractViolation(
                f"unknown raw_book_diff variant {raw!r}; "
                f"expected one of {DIFF_TYPES}"
            )
        return raw, None, None
    if not isinstance(raw, dict):
        raise ContractViolation(
            f"raw_book_diff must be an object or one of {DIFF_TYPES}, "
            f"got {type(raw).__name__}"
        )
    keys = set(raw)
    if keys == {"new"}:
        inner = raw["new"]
        if not isinstance(inner, dict):
            raise ContractViolation("raw_book_diff.new must be an object")
        return "new", None, _sz(inner.get("sz"))
    if keys == {"update"}:
        update = raw["update"]
        if not isinstance(update, dict):
            raise ContractViolation("raw_book_diff.update must be an object")
        return "update", _sz(update.get("origSz")), _sz(update.get("newSz"))
    raise ContractViolation(
        f"unknown raw_book_diff shape {sorted(keys)}; the contract allows "
        "exactly {new}, {update} or \"remove\""
    )


def _sz(value: Any) -> str | None:
    """Sizes are decimal strings on the wire and stay strings in the table."""
    if value is None or isinstance(value, str):
        return value
    raise ContractViolation(
        f"size must be a decimal string, got {type(value).__name__}: {value!r}"
    )


def _order_status_row(event: Any, **envelope: Any) -> dict:
    if not isinstance(event, dict):
        raise ContractViolation(
            f"order status event must be an object, got {type(event).__name__}"
        )
    order = event.get("order")
    if not isinstance(order, dict):
        raise ContractViolation("order status event has no order object")
    builder = event.get("builder")
    if builder is not None and not isinstance(builder, dict):
        raise ContractViolation(
            f"builder must be an object or null, got {type(builder).__name__}"
        )
    return {
        **envelope,
        "user": event.get("user"),
        "status": event.get("status"),
        "hash": event.get("hash"),
        "builder": builder,
        "order": _bound_order(order, depth=0),
    }


def _bound_order(order: dict, depth: int) -> dict:
    """Copy an order as contract columns, bounding the child recursion.

    MAX_ORDER_DEPTH counts the Order structs the contract unrolls (the schema
    generator emits the same number), so a child list that would exceed the
    struct is not expanded: its contents go into children_overflow_json as the
    verbatim structure. Truncated, never dropped — child orders matter for book
    reconstruction, and a JSON read still reaches them.
    """
    out = {column: order.get(key) for key, column in ORDER_KEYS.items()}
    _report_unknown("order", set(order) - set(ORDER_KEYS) - {"children"})

    children = order.get("children") or []
    if not isinstance(children, list):
        raise ContractViolation(
            f"order.children must be a list, got {type(children).__name__}"
        )
    if depth + 1 >= MAX_ORDER_DEPTH:
        if children:
            out["children_overflow_json"] = json.dumps(
                children, separators=(",", ":"), default=str
            )
        return out
    out["children"] = [_bound_order(child, depth + 1) for child in children]
    return out


def _fill_row(event: Any, **envelope: Any) -> dict:
    """Fills arrive as a positional tuple: [user, payload]."""
    if not isinstance(event, (list, tuple)) or len(event) != 2:
        raise ContractViolation(
            f"fill event must be a 2-element [user, payload] tuple, got "
            f"{event!r:.80}"
        )
    user, payload = event
    if not isinstance(payload, dict):
        raise ContractViolation(
            f"fill payload must be an object, got {type(payload).__name__}"
        )
    liquidation = payload.get("liquidation")
    if liquidation is not None and not isinstance(liquidation, dict):
        raise ContractViolation(
            "fill liquidation must be an object or absent"
        )
    return {
        **envelope,
        "user": user,
        "time": payload.get("time"),
        "coin": payload.get("coin"),
        "px": payload.get("px"),
        "sz": payload.get("sz"),
        "side": payload.get("side"),
        "dir": payload.get("dir"),
        "closed_pnl": payload.get("closedPnl"),
        "start_position": payload.get("startPosition"),
        "crossed": payload.get("crossed"),
        "fee": payload.get("fee"),
        "fee_token": payload.get("feeToken"),
        "hash": payload.get("hash"),
        "oid": payload.get("oid"),
        "tid": payload.get("tid"),
        "cloid": payload.get("cloid"),
        "deployer_fee": payload.get("deployerFee"),
        "builder": payload.get("builder"),
        "builder_fee": payload.get("builderFee"),
        "priority_gas": payload.get("priorityGas"),
        "twap_id": payload.get("twapId"),
        "liquidation": liquidation,
    }


# ---------------------------------------------------------------------------
# block info
# ---------------------------------------------------------------------------
def block_info_row(
    table: str,
    block_number: int,
    block_time: dt.datetime,
    first_local_time: dt.datetime,
    last_local_time: dt.datetime,
    event_count: int,
    log_index_min: int,
    log_index_max: int,
    first_seen_at: dt.datetime,
    complete: bool,
) -> dict:
    """One row for hypercore.block_info — the block-boundary record."""
    return {
        "table": table,
        "block_number": block_number,
        "block_time": block_time,
        "first_local_time": first_local_time,
        "last_local_time": last_local_time,
        "event_count": event_count,
        "log_index_min": log_index_min,
        "log_index_max": log_index_max,
        "first_seen_at": first_seen_at,
        "complete": complete,
    }
