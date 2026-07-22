# ADR-R003: Materialize a GCP-side copy for non-Omni native consumers

**Status:** Accepted · **Date:** 2026-07-20

## Context

The intended downstream consumers are **Cloud Run, Compute Engine (Python),
Dataflow, Bigtable, and AlloyDB**. The instinct is to treat the Omni external
table as a normal BigQuery table the whole platform can read. It is not.

**Omni is a query surface, not a shared storage layer for GCP.** The table's
bytes live in AWS; a GCP-native service cannot read that storage directly. Two
facts force the design:

1. **The BigQuery Storage Read API does not support external/Omni tables.**
   Anything that reads BigQuery at high throughput — Dataflow's `BigQueryIO`,
   the Spark-BigQuery connector — cannot stream from an Omni table. **Verified**
   in this POC ([`scripts/omni_storage_read_test.py`](../../scripts/omni_storage_read_test.py)):
   a `create_read_session` on `omni_s3.orders` failed with `InvalidArgument: 400
   ... Read API can be used to read temporary tables only in this region.`
2. **Bigtable and AlloyDB can't read S3 or Omni at all.** They ingest from a
   pipeline over a GCP-resident source.

So consumption splits into two shapes: **query → small result** (aggregates,
slices, lookups — cheap, only the result crosses) versus **materialize → GCP
copy** (CTAS to a native table, a cross-cloud materialized view, or `EXPORT
DATA`, which pays cross-cloud transfer on the moved volume). Materializing the
full dataset repeatedly is, economically, just a copy — at which point
[R001](R001-omni-read-in-place-over-copy.md) says use a scheduled replica
instead.

## Decision

**Serve non-Omni GCP-native consumers from a materialized GCP-side surface, not
directly from the Omni table.**

- **Aggregates / filtered slices** → query Omni in place; hand the small result
  to Cloud Run / Compute / a pipeline.
- **Full dataset needed in GCP, repeatedly** → maintain a **cross-cloud
  materialized view** (or scheduled CTAS/export) into a native BigQuery table or
  GCS, and point Dataflow / Bigtable / AlloyDB at *that*.
- **Latency-sensitive serving** → front it with the materialized native table
  (or Bigtable/AlloyDB) — Omni is analytics-grade, not interactive.

## Consequences

- Downstream architecture is explicit about where the cross-cloud transfer
  happens (at materialization, once) instead of being surprised by it.
- Dataflow/Bigtable/AlloyDB never depend on an unsupported read path.
- The "materialize everything every run" anti-pattern is called out as the
  signal to switch to a scheduled copy, keeping [R001](R001-omni-read-in-place-over-copy.md)'s
  economics honest.
