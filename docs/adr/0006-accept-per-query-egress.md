# ADR-0006: Accept per-query egress, with a measured break-even review

**Status:** Accepted · **Date:** 2026-07-13

## Context

Zero-copy across clouds trades storage/pipeline cost for network egress: GCP→AWS transfer runs roughly $90–155/TB scanned, metered by Snowflake under `DATA_TRANSFER_HISTORY` (`transfer_type = 'DATA_LAKE'`). Iceberg mitigates via partition pruning and column projection — Snowflake fetches pruned Parquet byte ranges, not whole tables.

## Decision

Accept per-query egress as the default cost model. Instrument from day one; review per-table at 30 days against the break-even rule: **if monthly scanned bytes for a table materially exceed its size, replicate that table (ADR-0002 option C or a Snowflake-materialized copy) and keep federating the long tail.**

## Consequences

- Zero standing cost for rarely-queried tables; no pipelines built ahead of demonstrated need.
- Guardrails required: Snowflake resource monitors + a transfer-cost dashboard keyed on `DATA_TRANSFER_HISTORY`; alert on egress runaway from unbounded ad-hoc workloads.
- Producers must partition/sort shared tables around consumer query patterns — partitioning quality is now a cost lever, not just a performance one.
- Region pairing (Snowflake AWS region nearest the GCS region) minimizes latency; egress price is cloud-boundary-driven either way.

## Alternatives considered

- **Replicate everything upfront:** predictable cost but pays for copies nobody queries and reintroduces pipeline ops for the entire estate.
- **Snowflake cross-cloud auto-fulfillment:** managed, but replicates all shared data regardless of query demand and is Snowflake-only.
