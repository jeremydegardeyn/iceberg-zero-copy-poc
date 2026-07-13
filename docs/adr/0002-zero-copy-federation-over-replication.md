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
