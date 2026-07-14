# ADR-0005: Single-writer on GCP; read-only consumers

**Status:** Accepted · **Date:** 2026-07-13

## Context

Snowflake catalog-linked databases support writes to externally managed Iceberg tables, so multi-engine writes are technically possible. Iceberg's optimistic concurrency makes cross-engine writes safe at the format level, but ownership, maintenance, and data-quality accountability blur fast.

## Decision

All writes, compaction, snapshot expiry, and orphan-file cleanup happen on the GCP side. The AWS-side grant is read-only: `roles/biglake.viewer` in GCP IAM, read-only role grants on the catalog-linked database in Snowflake — enforced at both ends of the trust boundary.

## Consequences

- Clear ownership: one team accountable for correctness, SLAs, and table maintenance.
- Small-file management stays with the producer — critical because compaction quality directly drives consumer-side scan (egress) cost.
- The shared namespace (`shared_aws`) is the published contract: additive schema evolution flows through transparently; breaking changes require consumer coordination.
- Catalog-managed tables lack row/column-level security — consumer-side policies are layered in Snowflake RBAC on top of the CLD; producer-side, share only curated tables into the namespace.
- If AWS-side writeback is ever needed, that's a new ADR (separate namespace, not shared tables).
