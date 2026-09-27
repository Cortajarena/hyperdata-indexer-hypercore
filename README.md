# hyperdata-indexer-hypercore

Service in charge of **indexing the data coming from the HyperCore node** (`hyperdata-node`, hl-visor).

## How it gets data

- **v0 — batch polling of node output files (primary):** the node appends raw JSONL outputs
  (`node_fills_streaming`, `node_order_statuses_streaming`, `node_raw_book_diffs_streaming`)
  plus periodic full-state snapshots (`periodic_abci_states/<date>/<height>.rmp`) under its output
  tree. This service pulls those files in batch polling jobs and indexes them.
- **Future — other transports:** e.g. a gRPC subscription to a QuickNode (or similar) node, a
  different streaming transport, etc. Same downstream indexing, different front door.

## Out of scope: the Kafka stream

Indexing the **Kafka stream coming from the sidecar service** (`hyperdata-node-sidecar` —
per-table line-streaming topics + hour-file seals) is **not** this service's job. That path goes
through the **Flink job** (`jobs/flink/parse-node-outputs`) with a **direct Iceberg sink** —
lines → typed rows → Parquet + Iceberg (`hypercore.*`), checkpoint-aligned commits.

So the split is:

| Path | Who indexes it | Sink |
| :--- | :--- | :--- |
| Node output files (batch pull) | **this service** | TBD |
| Sidecar Kafka stream (line streaming) | Flink `parse-node-outputs` | Iceberg (direct sink) |

## Status

Early scaffold — design and implementation land with the ingestion-layer buildout; see
[`docs/ingestion.md`](../../docs/ingestion.md) for the surrounding pipeline and phase plan.
