"""Command line: `python -m hypercore_indexer <command>`.

Two commands, both read-only against the warehouse unless stated:

    run      ingest hour-files for TABLE (the default command)
    schema   print the contract's columns, for debugging a deployment

Configuration comes from the environment (see config.py); flags exist only for
the things worth overriding in a shell.
"""

from __future__ import annotations

import argparse
import logging
import sys

from config import ConfigError, from_env
from contract import ContractError, load
from ledger import Ledger
from pipeline import Pipeline
from sink import IcebergSink, SinkError
from version import __version__


def _logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
        stream=sys.stderr,
    )


def _schemas(config) -> dict:
    contract = load(config.schema_dir, config.table)
    return {config.table: contract.arrow, "block_info": contract.block_info}


def cmd_run(args: argparse.Namespace) -> int:
    config = from_env()
    contract = load(config.schema_dir, config.table)
    ledger = Ledger(config.ledger_path)
    logging.getLogger(__name__).info(
        "contract: %s (%d columns), ledger holds %d committed file(s)",
        config.table, len(contract.field_names), ledger.committed_count(),
    )
    if args.dry_run:
        for path in Pipeline(config, contract, sink=None, ledger=ledger) \
                .discover():
            print(path)
        return 0
    with IcebergSink(config, _schemas(config)) as sink:
        Pipeline(config, contract, sink, ledger).run()
    return 0


def cmd_schema(args: argparse.Namespace) -> int:
    config = from_env()
    contract = load(config.schema_dir, config.table)
    print(f"hypercore.{config.table}")
    for field in contract.arrow:
        print(f"  {field.name:<28} {field.type}")
    print("\nhypercore.block_info")
    for field in contract.block_info:
        print(f"  {field.name:<28} {field.type}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hypercore-indexer", description=__doc__)
    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command")

    run = sub.add_parser("run", help="ingest hour-files for TABLE")
    run.add_argument("--dry-run", action="store_true",
                     help="list the files that would be ingested, then stop")
    run.set_defaults(func=cmd_run)

    schema = sub.add_parser("schema", help="print the contract's columns")
    schema.set_defaults(func=cmd_schema)

    args = parser.parse_args(argv)
    _logging(args.verbose)
    if not getattr(args, "func", None):
        args = parser.parse_args((argv or []) + ["run"])
    try:
        return args.func(args)
    except (ConfigError, ContractError, SinkError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
