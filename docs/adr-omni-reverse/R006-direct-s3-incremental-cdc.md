# ADR-R006: Direct-from-S3 incremental CDC for streaming sinks, bypassing Omni

**Status:** Accepted · **Date:** 2026-07-21

## Context

[R003](R003-materialize-for-native-consumers.md) established (and this POC
[verified](../../scripts/omni_storage_read_test.py)) that Dataflow/Spark cannot
read an Omni table via the Storage Read API — so feeding a **streaming sink**
like Pub/Sub or Bigtable through Omni means materializing a GCP-side copy. For
an incremental/continuous load, re-materializing the whole dataset on every run
is wasteful, and it drags BigQuery into a path that only needs the *new* rows.

But the lake is open Iceberg on S3, and **Iceberg's own metadata already
describes what changed.** Every commit is a snapshot; an `APPEND` snapshot's new
manifest lists exactly the data files it added. A consumer can therefore read
just the diff — no query engine, no Storage Read API, no Omni.

## Decision

**For incremental / streaming consumption into GCP sinks, read the Iceberg
snapshot diffs directly from S3 and emit them to the sink, tracking a snapshot
watermark. Bypass Omni (and BigQuery) entirely for this path.**

The loop, implemented in
[`scripts/omni_incremental_cdc.py`](../../scripts/omni_incremental_cdc.py):

1. Remember the last snapshot processed (a durable **watermark**).
2. Walk the snapshots added since it (parent chain from `current_snapshot`).
3. For each, read **only the data files that snapshot ADDED** (its own new
   manifests, entries with status `ADDED`) — plain Parquet, read with the object
   store's own credentials.
4. Emit those rows to the sink (Pub/Sub, Bigtable, ...).
5. Advance the watermark.

### Proven 2026-07-21

Against `demo.orders` on `s3://iceberg-poc-omni-jdg`:

- **Sync #1** (no watermark): full load, 4 rows, watermark set.
- **Append** a batch → new snapshot.
- **Sync #2**: emitted **only the 3 new rows** (the diff), not the original 4.
- **Sync #3**: no new snapshots → "up to date."
- **End-to-end sink**: appended another snapshot, published its diff to a
  **Pub/Sub** topic via the script's sink, and **pulled the 2 new rows back**
  from a subscription. No Omni, no Storage Read API in the path.

## Consequences

- The third consumption mode alongside [R001](R001-omni-read-in-place-over-copy.md)
  (Omni query-in-place for aggregates) and [R003](R003-materialize-for-native-consumers.md)
  (materialize for batch native consumers): **incremental streaming**, the
  cheapest path when a sink needs only new rows.
- No full re-scan, no BigQuery in the loop — cost is S3 GETs on the new files
  plus the sink writes.
- **Scope: append-only tables** (the common event/streaming case). Row-level
  deletes / overwrites (Iceberg position/equality delete files) need extra
  handling — read delete files and apply them, or restrict this path to
  append-only feeds. Called out so it isn't mistaken for full CDC with updates.
- The **watermark must be durable** (GCS object, Bigtable cell, DB row) so a
  restart resumes exactly once; the POC uses a local JSON file.
- Reads use S3 credentials directly. In production, give the reader a
  least-privilege role — the same keyless posture as
  [R002](R002-keyless-web-identity-federation.md) applies to whatever runs this
  (Cloud Run job, Dataflow with an S3 connector, a scheduled container).
- This is, in effect, **Iceberg CDC** — the table format doing the change-feed
  work that would otherwise need a separate CDC system.
