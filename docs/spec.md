# Spec: Zero-Copy Sharing of GCP Data to AWS via Apache Iceberg

**Status:** Draft · **Author:** Jeremy · **Date:** 2026-07-13

## 1. Goal

Expose data produced and stored in GCP to consumers running on AWS — ultimately Snowflake — without maintaining a second physical copy of the data or building replication pipelines. "Zero-copy" here means: one authoritative set of Parquet/Iceberg files in GCS, with AWS-side engines reading that data in place via shared Iceberg metadata.

## 2. Why Iceberg enables this

An Iceberg table is just (a) immutable data/metadata files in object storage and (b) a catalog pointer to the current metadata file. Any engine that can reach the storage and the catalog can query the table — the table format, not the engine, owns the data. So cross-cloud sharing reduces to two problems:

1. **Catalog access** — the AWS-side engine must discover tables and current snapshots.
2. **Storage access** — it must be able to read the underlying GCS objects.

Both are solved by the Iceberg REST Catalog (IRC) protocol, which Snowflake, Spark, Trino, and BigQuery all speak.

## 3. Recommended architecture (Option A): BigLake IRC + Snowflake catalog-linked database

```
GCP                                          AWS
┌──────────────────────────────┐             ┌──────────────────────────┐
│  Producers (BigQuery, Spark) │             │  Snowflake account       │
│            │                 │             │                          │
│            ▼                 │   IRC       │  Catalog integration     │
│  BigLake metastore ──────────┼────────────▶│  Catalog-linked database │
│  (Lakehouse runtime catalog, │  (HTTPS)    │            │             │
│   Iceberg REST endpoint)     │             │            ▼             │
│            │                 │             │  Virtual warehouses      │
│            ▼                 │             │  read Parquet directly   │
│  GCS bucket (Iceberg tables) │◀────────────┼── data reads (egress $)  │
└──────────────────────────────┘             └──────────────────────────┘
```

**Components:**

- **Source of truth:** Iceberg tables in a GCS bucket, registered in **BigLake metastore** (renamed "Lakehouse runtime catalog" in April 2026), which exposes a managed Iceberg REST catalog endpoint. Producers (BigQuery, managed Spark) read/write these same tables natively.
- **Snowflake side:** a **catalog integration** for the BigLake IRC endpoint — this combination is **GA as of June 2, 2026** — wrapped in a **catalog-linked database (CLD)**. The CLD auto-syncs namespaces and tables from the remote catalog: new tables appear in Snowflake without any DDL, and reads always reflect the latest committed snapshot. No data is ingested; Snowflake warehouses scan the GCS Parquet files directly.
- **Storage access:** prefer **credential vending** from the IRC (catalog hands Snowflake short-lived GCS tokens per table). If vending isn't available for your setup, configure a Snowflake **external volume** pointing at the GCS bucket (Snowflake service account granted read on the bucket). Cross-cloud external volumes are supported for externally managed (catalog-integrated) tables — the same-cloud/same-region restriction only applies to *Snowflake-managed* Iceberg tables.

**Write policy:** consumer access is read-only. All writes and table maintenance (compaction, snapshot expiry, orphan cleanup) stay on the GCP side. Snowflake CLDs do support writes to externally managed tables, but disable this for the share to keep a single-writer model.

## 4. Alternatives considered

| | A. IRC federation (recommended) | B. Snowflake-native share | C. Physical replication |
|---|---|---|---|
| How | BigLake IRC → Snowflake CLD | Snowflake account **on GCP** ingests/points at data; share to AWS account via listing + cross-cloud auto-fulfillment | Sync GCS→S3 (Storage Transfer Service), rewrite metadata, register in Glue/S3 Tables |
| Copies | 0 | 1 (auto-fulfillment replicates under the hood) | 1, self-managed |
| Egress | Per query, on scanned bytes | Once per changed data (managed) | Once per changed data (self-managed) |
| Freshness | Live (latest snapshot) | Near-live (replication lag) | Batch lag |
| Ops burden | Minimal | Low (Snowflake-managed) | High |
| Consumer lock-in | None — any IRC-capable engine on AWS (Spark, Trino, Athena) can use the same share | Snowflake-only | None |

**Why not B/C by default:** B isn't zero-copy and requires a second Snowflake deployment; C is the classic pipeline this spec is trying to avoid — Iceberg metadata uses **absolute paths**, so replication requires `rewrite_table_path` + file sync + `register_table` on every sync cycle, plus divergence monitoring. Keep C in reserve as a cost optimization (see §6).

## 5. Detailed design (Option A)

**GCP setup**

1. Land tables as Iceberg in GCS (BigQuery tables for Apache Iceberg, or Spark writing via the BigLake IRC endpoint).
2. Enable the BigLake/Lakehouse Iceberg REST endpoint; create a dedicated namespace (e.g., `shared_aws`) containing only tables intended for sharing.
3. Configure **workload identity federation** so Snowflake authenticates to GCP without long-lived service account keys (supported natively by the GA catalog integration), scoped read-only to the share bucket + catalog namespace.

**Snowflake setup**

```sql
CREATE CATALOG INTEGRATION biglake_irc
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_URI = '<biglake-irc-endpoint>'
  REST_AUTHENTICATION = ( TYPE = OAUTH ... )   -- GCP OAuth token exchange
  ENABLED = TRUE;

CREATE DATABASE shared_gcp_data
  LINKED_CATALOG = ( CATALOG = 'biglake_irc',
                     NAMESPACE_FILTERS = ('shared_aws') )
  EXTERNAL_VOLUME = gcs_share_vol;  -- omit if credential vending is used
```

Then grant read on `shared_gcp_data` to consumer roles. If multiple downstream Snowflake accounts/regions need it, share the entire CLD via a listing with cross-cloud auto-fulfillment (note: fulfillment replicates — those consumers trade zero-copy for locality).

**Governance:** the shared namespace is the contract boundary. Additive schema evolution (new columns) flows through transparently via Iceberg metadata; breaking changes require consumer coordination. Row/column-level policies can be layered in Snowflake on top of the CLD tables.

## 6. Cost model — the real trade-off

Zero-copy across clouds moves cost from storage/pipelines to **network egress**. GCP egress to internet/AWS runs roughly $90–155/TB scanned (region-dependent); Snowflake bills cross-cloud transfer under `DATA_LAKE`.

- Iceberg mitigates this: partition pruning + column projection mean Snowflake fetches only needed Parquet byte ranges, not whole tables. Partition and sort the shared tables around consumer query patterns.
- **Break-even rule of thumb:** if AWS-side queries repeatedly scan the same large partitions such that monthly scanned-bytes ≫ table size, a scheduled replica (Option C, or materialized copies of hot tables inside Snowflake) becomes cheaper than per-query egress. Start with A, instrument `DATA_TRANSFER_HISTORY`, and hybridize per-table if needed.
- Latency: cross-cloud object reads add tens of ms per request — fine for analytics, wrong for latency-sensitive serving.

## 7. Risks and open questions

- **Egress runaway:** an unbounded ad-hoc workload on AWS can generate surprise egress. Mitigate with resource monitors + per-table transfer tracking from day one.
- **Credential vending support:** confirm current BigLake IRC vending behavior with Snowflake catalog integrations in your regions; fall back to external volume + service-account key management if needed.
- **Feature gaps:** externally managed tables in Snowflake lag Snowflake-native tables on some features (e.g., certain replication/cloning paths). Validate the consumer's required features against current docs.
- **Maintenance ownership:** GCP side must run compaction/snapshot expiry; small-file bloat directly inflates AWS-side scan cost.
- **Region pairing:** choose the Snowflake AWS region nearest the GCS region to minimize latency (egress price is cloud-boundary driven either way).

## 8. Implementation plan

1. **Week 1 — POC:** one non-sensitive table in GCS via BigLake IRC; Snowflake catalog integration + CLD; validate reads, auth, and observed egress per query.
2. **Week 2 — Hardening:** credential strategy, namespace filters, read-only enforcement, resource monitors, transfer-cost dashboards.
3. **Week 3+ — Rollout:** migrate shared datasets into the `shared_aws` namespace; document the schema-evolution contract; set break-even review (query volume vs. egress) at 30 days.

## Sources

- [Snowflake: Use a catalog-linked database for Apache Iceberg tables](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database)
- [Snowflake: Configure a catalog integration for Iceberg REST catalogs](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest)
- [Snowflake release note: Google Cloud BigLake Metastore catalog integration GA (June 2, 2026)](https://docs.snowflake.com/en/release-notes/2026/other/2026-06-02-iceberg-google-biglake-metastore-catalog-integration-ga)
- [Snowflake engineering blog: Catalog-Linked Database updates (cross-cloud auto-fulfillment)](https://www.snowflake.com/en/blog/engineering/catalog-linked-database-cld-updates/)
- [Snowflake quickstart: Snowflake and BigQuery via Iceberg](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-and-bigquery-via-iceberg/)
- [Google Cloud: Use the BigLake metastore Iceberg REST catalog](https://docs.cloud.google.com/biglake/docs/blms-rest-catalog)
- [Google Cloud: Lakehouse for Apache Iceberg (formerly BigLake)](https://cloud.google.com/products/lakehouse)
- [Snowflake: Understanding data transfer cost](https://docs.snowflake.com/en/user-guide/cost-understanding-data-transfer)
- [Snowflake: Storage for Apache Iceberg tables](https://docs.snowflake.com/en/user-guide/tables-iceberg-storage)
- [Dremio: Disaster recovery for Iceberg tables (rewrite_table_path / register_table)](https://www.dremio.com/blog/disaster-recovery-for-apache-iceberg-tables-restoring-from-backup-and-getting-back-online/)
