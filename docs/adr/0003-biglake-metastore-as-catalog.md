# ADR-0003: BigLake (Lakehouse) metastore as the Iceberg catalog

**Status:** Accepted · **Date:** 2026-07-13

## Context

Zero-copy federation needs an Iceberg REST catalog reachable from AWS. Candidates: Google's managed BigLake metastore (renamed Lakehouse runtime catalog, Apr 2026), self-hosted Apache Polaris/Nessie, or a third-party managed catalog.

## Decision

BigLake metastore's Iceberg REST endpoint (`https://biglake.googleapis.com/iceberg/v1/restcatalog`).

## Consequences

- Serverless, zero ops; GA Snowflake catalog integration (June 2026) with documented keyless auth path.
- BigQuery reads the same tables via catalog federation — producers keep their native engine.
- Credential vending built in — no standing storage credentials for consumers.
- Accepted platform coupling to GCP for the catalog control plane (data files remain open Parquet/Iceberg; migration path to Polaris exists via `register_table`).
- Known limits: per-minute API quota (raisable), BigQuery DDL/DML not supported on catalog-managed tables (writes go through Spark or IRC-capable engines).

## Alternatives considered

- **Self-hosted Polaris/Nessie:** full control, cross-cloud neutral, but adds a service to run, secure, and expose publicly — unjustified ops burden at this stage.
- **Snowflake Open Catalog (managed Polaris):** inverts the coupling (catalog lives with the consumer, data with the producer); weaker BigQuery-side integration.
