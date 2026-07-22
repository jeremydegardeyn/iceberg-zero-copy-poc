# ADR-R004: Terraform the control plane; script the data plane

**Status:** Accepted · **Date:** 2026-07-21

## Context

The reverse leg was first stood up with `gcloud`/`bq`/`aws` CLI commands (see
the runbook). Making it reproducible raised the question of *what* to codify.

The pieces split cleanly:

- **Control plane (infrastructure):** the BigQuery Omni connection, the AWS IAM
  role + web-identity trust, the S3 read policy, the dataset, the external
  table, the bucket. All have first-class Terraform resources.
- **Data plane:** writing the Iceberg table itself (Parquet + metadata) into S3.
  That is a job (PyIceberg / Spark), not infrastructure; Terraform should not
  own row data.

Two wrinkles make a naïve "terraform everything" apply fail:

1. **A circular dependency.** The BigQuery connection needs the AWS role ARN;
   the role's trust needs the connection's Google identity. A literal graph of
   `connection → role → connection` won't plan.
2. **The external table needs a `metadata.json` URI** that only exists *after*
   the data-plane write.

## Decision

**Codify the control plane in a dedicated Terraform root module
(`terraform/omni-reverse/`); leave the Iceberg write to a script.**

- Break the cycle by building `iam_role_id` from `aws_account_id` +
  `omni_role_name` as a **string**, so the connection depends on a value, not on
  the `aws_iam_role` resource. The role's trust then reads the connection's
  identity. Order resolves as **connection → role → policy**, with no manual
  trust handshake.
- Handle the metadata URI with a **two-phase apply**: phase 1 stands up
  connection/role/policy/dataset/bucket; the script writes the table; phase 2
  supplies `omni_metadata_uri` and creates the external table (a nullable var
  gates the resource with `count`). This mirrors the forward-leg module's
  Snowflake-WIF-subject two-phase pattern.
- Keep it a **separate root module** from `terraform/` so the forward-leg apply
  never needs AWS credentials.

## Consequences

- The keyless-federation handshake ([R002](R002-keyless-web-identity-federation.md))
  becomes fully automated — no copy-pasting the identity onto the AWS role.
- `terraform validate` passes with no cycle; the module is reproducible.
- The one caveat is `source_format = "ICEBERG"` provider support: on older
  providers, create the table via SQL DDL and `import` it, keeping the rest in
  Terraform.
- Data creation stays where it belongs (a job), so state never carries row data.
