# ADR-0001: Apache Iceberg as the cross-cloud table format

**Status:** Accepted · **Date:** 2026-07-13

## Context

Data produced in GCP must be consumable by engines on AWS (initially Snowflake) without vendor-specific export pipelines. The sharing mechanism must outlive any single engine choice on either side.

## Decision

Store shared datasets as Apache Iceberg tables (V2) in GCS. The table format — not any engine — owns the data contract.

## Consequences

- Any Iceberg-capable engine (Snowflake, BigQuery, Spark, Trino, Flink) reads the same files; consumer choice on AWS is reversible.
- Snapshot isolation gives consumers consistent reads while producers commit.
- Constraints inherited from the catalog: Parquet only, Iceberg V2/V3, no engine-native fine-grained ACLs on the shared tables (mitigated in ADR-0005).

## Alternatives considered

- **Delta Lake + UniForm:** viable, but adds a translation layer and the GCP-native tooling (BigQuery, Lakehouse catalog) is Iceberg-first.
- **Plain Parquet on GCS:** no schema evolution, no transactional commits, no catalog discovery — every consumer builds bespoke conventions.
- **Engine-native sharing (BigQuery Analytics Hub, Snowflake shares):** locks the contract to one vendor on one or both sides.
