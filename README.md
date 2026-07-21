# iceberg-zero-copy-poc

Zero-copy sharing of GCS-resident Apache Iceberg tables to Snowflake on AWS, via the BigLake (Lakehouse) Iceberg REST catalog and a Snowflake catalog-linked database. One copy of the data in GCS; Snowflake reads it in place.

## Layout

- `docs/spec.md` — architecture spec (options, cost model, risks)
- `docs/zero-copy-decision-tree.md` — which tables fit zero-copy federation vs a copy/direct-write (decision aid)
- `docs/runbook.md` — index; per-path runbooks: `runbook-zero-copy.md` + `runbook-s3-replica.md`
- `docs/as-run.md` — as-run log from the successful 2026-07-14 execution (fastest path to reproduce)
- `docs/architecture/overview.md` — ARB diagrams (context, auth sequence, trust boundaries, decision flow)
- `docs/adr/` — architecture decision records (index in `docs/adr/README.md`)
- `env.example.sh` — copy to `env.sh`, fill in, `source` it (gitignored)
- `scripts/` — GCP-side setup + pipeline execution, ordered
- `sql/` — Snowflake-side setup, ordered
- `dataflow/` — flex templates: streaming (Pub/Sub→Iceberg) + batch (CSV→Iceberg), shared Java-enabled launcher image
- `trigger/` — Cloud Run functions: file-drop launcher + event-driven archive-on-success
- `validation.yaml` + `scripts/validate.py` — config-driven integrity controls (source vs lake vs Snowflake)
- `terraform/` — IaC for everything terraform-able (README documents what isn't and why)

## Execution order

```bash
cp env.example.sh env.sh && $EDITOR env.sh && source env.sh
./scripts/01_gcp_catalog_setup.sh        # API, bucket, IRC catalog, vending IAM
./scripts/02_create_table.sh             # Dataproc Serverless: namespace + test table
```

Then in Snowflake run `sql/01_catalog_integration.sql` (pauses to exchange values with `scripts/03_wif_setup.sh` — see comments), then `sql/02_catalog_linked_db.sql`, then `sql/03_freshness_egress.sql` (rerun `./scripts/02_create_table.sh --append` first to prove freshness).

Teardown: `sql/99_teardown.sql` then `./scripts/99_teardown_gcp.sh`.

## Prerequisites

GCP project with billing; `gcloud`/`gsutil` authed. Snowflake trial: Enterprise edition, cloud AWS, region near your GCS region.
