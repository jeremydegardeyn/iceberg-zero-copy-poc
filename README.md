# iceberg-zero-copy-poc

Zero-copy sharing of GCS-resident Apache Iceberg tables to Snowflake on AWS, via the BigLake (Lakehouse) Iceberg REST catalog and a Snowflake catalog-linked database. One copy of the data in GCS; Snowflake reads it in place.

## Layout

- `docs/spec.md` — architecture spec (options, cost model, risks)
- `docs/runbook.md` — full POC runbook with troubleshooting
- `docs/as-run.md` — as-run log from the successful 2026-07-14 execution (fastest path to reproduce)
- `docs/architecture/overview.md` — ARB diagrams (context, auth sequence, trust boundaries, decision flow)
- `docs/adr/` — architecture decision records (index in `docs/adr/README.md`)
- `env.example.sh` — copy to `env.sh`, fill in, `source` it (gitignored)
- `scripts/` — GCP-side setup, ordered
- `sql/` — Snowflake-side setup, ordered

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
