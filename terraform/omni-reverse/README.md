# terraform/omni-reverse

Infrastructure-as-code for the **reverse leg** — reading an S3-resident Iceberg
table from BigQuery (Omni). This is a **separate root module** from the
forward-leg `terraform/` because it spans two clouds (it adds the AWS provider);
keeping it apart means the forward-leg apply never needs AWS credentials.

See [../../docs/runbook-omni-reverse.md](../../docs/runbook-omni-reverse.md) for
the manual (`gcloud`/`bq`/`aws`) walk-through this module automates, and
[../../docs/adr-omni-reverse/](../../docs/adr-omni-reverse/) for the decisions.

## What it manages

| Resource | Provider | Purpose |
|---|---|---|
| `aws_s3_bucket.omni` | aws | Bucket that holds the Iceberg table (optional; `create_bucket`) |
| `google_bigquery_connection.omni` | google | BigLake-on-AWS connection (mints the Google identity) |
| `aws_iam_role.omni` | aws | Role Omni assumes; web-identity trust + 12h session |
| `aws_iam_role_policy.omni_s3_read` | aws | Least-privilege read on the bucket |
| `google_bigquery_dataset.omni` | google | Dataset in the `aws-<region>` location |
| `google_bigquery_table.orders` | google | External Iceberg table (phase 2, see below) |

## What it does NOT manage (data plane)

- **Writing the Iceberg table** (Parquet + metadata) into S3 — that is a
  data-plane step, not infrastructure. Use
  [`scripts/omni_write_iceberg.py`](../../scripts/omni_write_iceberg.py)
  (PyIceberg) or a Spark job. The module consumes its output (the
  `metadata.json` URI) as `omni_metadata_uri`.

## The circular dependency, and how this breaks it

BigQuery Omni has a chicken-and-egg: the **connection** needs the AWS **role
ARN**, and the role's **trust** needs the connection's **Google identity**.

This module sidesteps it: `iam_role_id` is built from `aws_account_id` +
`omni_role_name` as a **string** (`local.role_arn`), so
`google_bigquery_connection.omni` does not reference the `aws_iam_role`
resource. The role's `assume_role_policy` then reads
`google_bigquery_connection.omni.aws[0].access_role[0].identity`. Create order
resolves cleanly: **connection → role → policy**. No `terraform apply` two-step
for the trust, and no manual identity copy-paste.

## Two-phase apply (the table)

The external table needs the `metadata.json` URI, which only exists after the
Iceberg write. So:

```bash
# Phase 1 — stand up connection, role, policy, dataset, bucket
terraform init
terraform apply                      # omni_metadata_uri unset -> table skipped

# Write the Iceberg data (prints METADATA_LOCATION)
python ../../scripts/omni_write_iceberg.py --bucket <omni_bucket>

# Phase 2 — create the external table
terraform apply -var 'omni_metadata_uri=s3://.../metadata/00001-....metadata.json'
```

(This mirrors the forward-leg module's two-phase pattern for the Snowflake WIF
subject.)

## Notes / gotchas

- **AWS credentials** come from the standard chain (env vars, shared config,
  SSO). The `google` provider uses ADC as usual.
- **`source_format = "ICEBERG"`** on `google_bigquery_table` needs a recent
  provider (`~> 6.0` here). If yours rejects it, create the table with the SQL
  DDL in the runbook (step 4) and `terraform import` it, or drop that one
  resource and keep everything else in Terraform.
- **Region:** `aws_region` must be a BigQuery Omni region. `us-east-2` is
  **not** supported — see [ADR-R005](../../docs/adr-omni-reverse/R005-omni-region-placement.md).
- **State:** contains the connection identity and role ARN — store it in a
  backend you control (GCS/S3), not locally, for anything beyond a POC.
