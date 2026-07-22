# ADR-R001: Read S3 in place with BigQuery Omni over copying into GCS

**Status:** Accepted · **Date:** 2026-07-20

## Context

The forward leg answered "AWS consumer needs GCS-resident Iceberg." This is the
mirror: **a GCP consumer needs an S3-resident Iceberg lake**, and S3 is (or
should remain) the authoritative home.

Two ways to satisfy it:

- **Copy into GCS** — Storage Transfer / a pipeline lands a second copy in GCS,
  then BigQuery reads it natively.
- **Read in place with BigQuery Omni** — Google runs BigQuery compute *inside
  AWS*, in the bucket's region, and returns only results to GCP.

The cost structures are opposites. A copy pays cross-cloud egress on the **full
dataset every sync** and doubles storage. Omni runs the scan next to the data;
only the (usually small) result crosses the boundary — **no per-scan bulk
egress**. Measured on the POC: a full read of the table shuffled **151 bytes**
across the boundary (execution details), 787 ms elapsed.

The catch: Omni is a **query surface, not a shared storage layer** (see
[R003](R003-materialize-for-native-consumers.md)), it is **region-locked** (see
[R005](R005-omni-region-placement.md)), and reach is limited to BigQuery/Spark —
the mirror of the Athena/Redshift ceiling in [ADR-0007](../adr/0007-direct-write-over-replication.md).

## Decision

**When S3 is the authoritative lake and a GCP consumer needs to read or join it,
read in place with BigQuery Omni rather than copying into GCS** — provided the
data is in an Omni region and the consumers can take results via BigQuery.

Copy into GCS only when a non-Omni-aware GCP engine needs the raw bytes local,
GCS must become authoritative, or reads are heavy enough that repeated full
materialization is cheaper as a scheduled replica (the mirror of
[ADR-0006](../adr/0006-accept-per-query-egress.md)'s break-even).

## Consequences

- One copy of the data, in S3; GCS stays free of a duplicate.
- No per-scan bulk egress; cost moves to in-AWS scan (~$6.25/TiB) + per-GB
  transfer on results only.
- Live reads against S3 — no sync-interval staleness.
- Bounded reach and region lock become the design constraints, handled in
  [R003](R003-materialize-for-native-consumers.md) and
  [R005](R005-omni-region-placement.md).
