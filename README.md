# hyperdata-indexer-hypercore

Indexes the data coming from the HyperCore node by **batch polling** the output files the node
writes — and, later, by other means (e.g. a gRPC subscription to a QuickNode-style node).

This is the **file-path twin** of the sidecar → Kafka → Flink path. Both write the same
`hypercore.*` tables from the same contract; they differ only in how the bytes arrive. The Kafka
stream from [`hyperdata-node-sidecar`](https://github.com/Cortajarena/hyperdata-node-sidecar) is
ingested by the Flink job with a **direct Iceberg sink**, not by this service.

Design: [docs/ingestion.md](https://github.com/Cortajarena/hyperdata-platform/blob/main/docs/ingestion.md).
Row contract: [`platform/schemas`](https://github.com/Cortajarena/hyperdata-platform/tree/main/platform/schemas).

## What it does

```
node output files ──► normalise ──► Arrow (generated schema) ──► Iceberg
   hourly/<date>/<hour>   per line      buffer → 256 MB         hypercore.*
```

Per file: hash it, skip it if the ledger says it is already in, normalise every line into contract
rows, append them, then append the file's `block_info` rows, then record the commit.

- **Row = event, not line.** The node writes one JSON envelope per line with an `events` array; the
  indexer explodes that into one row per event, carrying the envelope down. `log_index` is the
  event's position within its *block*, across all lines of that block.
- **The contract is not ours.** Schemas are generated from `platform/schemas` (a `.proto` per
  table) and mounted at `/schemas`. This service never writes a column list by hand.
- **The normaliser is the validator.** The wire format has two shapes no declarative validator can
  check — the book-diff union (`new` | `update{…}` | `"remove"`) and the fills positional tuple
  `[user, payload]`. Arrow's JSON reader rejects both. So the normaliser owns them, and its variant
  sets are **closed**: anything else is a contract violation → dead-letter + alert, never a silent
  drop.

## Tables

| Table | Source | Partitioning |
| :--- | :--- | :--- |
| `hypercore.raw_book_diffs` | `node_raw_book_diffs_streaming` | `hours(block_time)` |
| `hypercore.order_statuses` | `node_order_statuses_streaming` | `days(block_time)` (52 GB/h) |
| `hypercore.fills` | `node_fills_streaming` | `hours(block_time)` |
| `hypercore.block_info` | derived, one row per (table, block) | `days(block_time)` |

## Idempotency

The unit of idempotency is the **sealed file**, not the row. The ledger records `(path, sha256)` of
every ingested file, so a re-run is a no-op. Deduplicating rows on `(block_number, log_index)` would
be wrong: a reorg re-emits the same block numbers with different content, and the L1 output carries
no block hash to tell the two apart.

pyiceberg has no cross-table transaction, so ordering carries the guarantee: **events are appended
first, `block_info` last.** A `block_info` row with `complete = true` is therefore proof that the
events it describes are already durable, which is what makes crash recovery decidable (see
`Pipeline.process_file`).

## Running

Requires the generated contract and a warehouse:

```bash
docker build -t hypercore-indexer .
docker run --rm \
  -v "$PWD/platform/schemas/generated/arrow:/schemas/generated/arrow:ro" \
  -v "$DATA_DIR/hl-node-data/data:/node:ro" \
  -v indexer-state:/state \
  -e TABLE=fills \
  -e NODE_DIR=/node \
  -e CATALOG_URI=http://iceberg-catalog:19120 \
  -e WAREHOUSE=s3://hyperdata-warehouse \
  -e S3_ENDPOINT=http://minio:9000 \
  -e S3_ACCESS_KEY_ID=hyperdata -e S3_SECRET_ACCESS_KEY=hyperdata-dev \
  hypercore-indexer run
```

Useful flags: `--dry-run` lists the files that would be ingested and stops; the `schema` subcommand
prints the contract's columns, which is the fastest way to check a mount.

One table per run — the streams have different rates and different files, and a run is a unit of
recovery.

## Configuration

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `TABLE` | *required* | `raw_book_diffs` \| `order_statuses` \| `fills` |
| `NODE_DIR` | *required* | root of the node's output tree |
| `CATALOG_URI` | *required* | Iceberg REST catalog |
| `WAREHOUSE` | *required* | e.g. `s3://hyperdata-warehouse` |
| `NAMESPACE` | `hypercore` | catalog namespace |
| `SCHEMA_DIR` | `/schemas/generated/arrow` | where the generated contract is mounted |
| `STATE_DIR` | `/state` | ledger lives here |
| `DLQ_DIR` | `/state/dlq` | dead-letter files |
| `BUFFER_BYTES` | `268435456` (256 MB) | commit threshold |
| `FINALIZED_ONLY` | `true` | skip the current, still-growing hour file |
| `MAX_ROWS_PER_FILE` | `0` (off) | cap rows per file, as a memory guard |
| `S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | unset | S3-compatible storage (MinIO in dev) |

## Development

```bash
pip install -r requirements-dev.txt
pytest                      # unit tests; contract tests skip if not mounted
ruff check --line-length 90 .
```

Tests run in the image too, which is how CI runs them:

```bash
docker build --target test -t hypercore-indexer:test .
docker run --rm -v "$PWD/../platform/schemas/generated/arrow:/schemas/generated/arrow:ro" \
  hypercore-indexer:test
```

## Status

v0.1.0: batch ingestion of sealed hour-files, with the file ledger, the dead-letter queue and
`block_info` block boundaries. Not yet: live tailing of the current hour file (it needs byte-offset
resume from the ledger rather than whole-file hashing), Prometheus metrics, and the
snapshot-aligned segment mode shared with the Flink backfill path.

## TODO — CI and build

`.github/workflows/ci.yml` is a **mockup**: lint + tests, not wired into the platform's CI and not
required to pass. What it would take to make it real:

- [ ] **Pin the contract.** The test job checks the platform repo out on a moving ref (`main`),
      because the generated schemas are committed there. Pin a tag or a contract version so this
      repo's tests cannot change without a commit here — needs contract versioning in the platform
      repo first.
- [ ] **Actually run it.** The workflow has never executed; its two commands are the ones verified
      locally in the image. A first real run is what proves the `pyarrow`/`pyiceberg` install and the
      contract mount on a clean runner.
- [ ] **Publish the image.** The ingest DAG currently runs `docker build` on every task because
      there is no registry to pull from. Publishing means: tag on release, push to GHCR, and have
      the DAG pull a digest instead of building — which also frees it from the source checkout.
- [ ] **Cut the CI cost.** The `test` image is ~1 GB (pyarrow + pyiceberg). Splitting the suite —
      unit tests needing neither, contract tests needing both — would let the cheap half skip it.
- [ ] **Add the integration test.** Nothing yet proves a row reaches Iceberg. The missing test brings
      up the `warehouse` profile (iceberg-catalog + minio), ingests one hour-file, and asserts the
      row counts plus the ledger's no-op on re-run. It is what would catch a contract that generates
      cleanly but that pyiceberg will not accept.
- [ ] **Coverage and a pre-commit hook**, once there is behaviour worth measuring.
