"""Test fixtures shaped like the real Jun-10 corpus.

Real-shape matters: these are transcribed from actual lines of
node_raw_book_diffs_streaming / node_order_statuses_streaming /
node_fills_streaming (hour 20260610/13), including the details that a
hand-written fixture would get wrong — the book-diff union in all three
variants, the fills positional tuple, the nested order with child orders, and
the sparse fill fields.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import pytest

ENVELOPE_TIME = "2026-06-10T13:10:56.723695057"
BLOCK_TIME = "2026-06-10T12:56:15.035481067"
BLOCK_NUMBER = 1030140001
USER = "0x7a146ee9d212816c31a0a93531e23d4359aade10"


def envelope(*events, block_number: int = BLOCK_NUMBER,
             local_time: str = ENVELOPE_TIME) -> dict:
    return {
        "local_time": local_time,
        "block_time": BLOCK_TIME,
        "block_number": block_number,
        "events": list(events),
    }


def book_diff(diff, *, user: str = USER, coin: str = "LIT",
              side: str = "B", px: str = "1.5047",
              oid: int = 464679247366) -> dict:
    return {"user": user, "oid": oid, "coin": coin, "side": side, "px": px,
            "raw_book_diff": diff}


def order(**overrides) -> dict:
    base = {
        "coin": "LIT", "side": "B", "limitPx": "1.5", "sz": "7.0",
        "oid": 464679247366, "timestamp": 1781096175035,
        "triggerCondition": "mark", "isTrigger": False, "triggerPx": "0.0",
        "children": [], "isPositionTpsl": False, "reduceOnly": False,
        "orderType": "Limit", "origSz": "7.0", "tif": "Gtc", "cloid": None,
    }
    base.update(overrides)
    return base


def order_status(status: str = "OrderFilled", *, order_obj: dict | None = None,
                 builder=None, tx_hash: str | None = None,
                 **order_overrides) -> dict:
    """An order-status event. Pass order_obj for a hand-built nested order."""
    return {
        "time": BLOCK_TIME,          # duplicate of block_time; not carried
        "user": USER, "hash": tx_hash, "builder": builder,
        "status": status,
        "order": order_obj if order_obj is not None else order(**order_overrides),
    }


def fill(**overrides) -> list:
    payload = {
        "coin": "LIT", "px": "1.5047", "sz": "7.0", "side": "B",
        "time": 1781096175035, "startPosition": "-1776.0",
        "dir": "Close Short", "closedPnl": "0.321433",
        "hash": "0x" + "0" * 64, "oid": 464679247366, "crossed": False,
        "fee": "0.001579", "tid": 684561980289873, "feeToken": "USDC",
        "twapId": None,
    }
    payload.update(overrides)
    return [USER, payload]


def line(envelope_dict: dict) -> bytes:
    return json.dumps(envelope_dict, separators=(",", ":")).encode()


@pytest.fixture
def corpus_line() -> bytes:
    """A verbatim line from the real book-diffs file."""
    return (
        b'{"local_time":"2026-06-10T13:10:56.723695057",'
        b'"block_time":"2026-06-10T12:56:15.035481067",'
        b'"block_number":1030140001,"events":[{"user":"0x7a146ee9d2128'
        b'16c31a0a93531e23d4359aade10","oid":464679247366,"coin":"LIT",'
        b'"side":"B","px":"1.5047","raw_book_diff":{"update":'
        b'{"origSz":"7.0","newSz":"0.0"}}}]}'
    )


def _contract_dir() -> pathlib.Path | None:
    """Find the generated contract, however deep the checkout is.

    In the runtime/test image it is mounted at /schemas. In a monorepo checkout
    it sits at <repo>/platform/schemas/generated/arrow, so walk up rather than
    counting parents — the depth differs between the two.
    """
    mounted = pathlib.Path("/schemas/generated/arrow")
    if (mounted / "hypercore_arrow").is_dir():
        return mounted
    for parent in pathlib.Path(__file__).resolve().parents:
        candidate = parent / "platform" / "schemas" / "generated" / "arrow"
        if (candidate / "hypercore_arrow").is_dir():
            return candidate
    return None


@pytest.fixture
def schema_dir() -> pathlib.Path:
    found = _contract_dir()
    if found is None:
        pytest.skip(
            "generated contract not found: mount platform/schemas/generated/"
            "arrow at /schemas, or run `make schemas` in the monorepo"
        )
    return found


@pytest.fixture
def ingest_ts() -> dt.datetime:
    return dt.datetime(2026, 9, 29, 12, 0, 0)
