# ADR-0002: Zero-copy catalog federation over replication

**Status:** Accepted · **Date:** 2026-07-13

## Context

Three ways to make GCS-resident Iceberg data available to Snowflake on AWS:

- **A. Catalog federation (zero-copy):** Snowflake catalog-linked database reads GCS in place via the Iceberg REST catalog.
- **B. Snowflake-native share:** second Snowflake account on GCP + listing with cross-cloud auto-fulfillment (Snowflake-managed replication).
- **C. Physical replication:** GCS→S3 sync + `rewrite_table_path` + `register_table` (Iceberg metadata uses absolute paths, so every sync cycle rewrites metadata).

## Decision

Option A. One authoritative copy in GCS; consumers get live reads of the latest committed snapshot.

## Consequences

- No replication pipelines, no divergence monitoring, no second storage bill; freshness is inherent.
- Cost moves to per-query cross-cloud egress (~$90–155/TB scanned) — accepted and bounded in ADR-0006.
- Analytics-grade latency only (cross-cloud object reads); not for serving paths.
- C remains the sanctioned fallback for hot tables or S3-only consumers (Athena/Redshift); B rejected as it doubles Snowflake footprint and is Snowflake-only.

## Consumer reach (tested 2026-07-16)

Option A is only available to engines whose storage layer is pluggable.
Snowflake, Spark, Trino, and Flink read `gs://` fine. **AWS's managed SQL
services cannot**: Glue happily accepts a Table with
`metadata_location = gs://…` (it does not validate the scheme), but Athena
fails at read time with `GENERIC_INTERNAL_ERROR: Wrong scheme for S3
location: gs://…`. Redshift Spectrum shares that S3-bound read path.

The trap is that registration *succeeds* — the failure only surfaces on query.

Consequence: if Athena or Redshift are on the consumer list, option C is not
a cost optimisation, it is **mandatory**. That is a requirements-driven
trigger for replication, independent of the ADR-0006 break-even economics.
