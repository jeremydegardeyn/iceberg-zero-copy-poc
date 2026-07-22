# Terraform for the POC

Codifies everything in this POC that Terraform *can* own. Written after the
fact — the resources already exist — so this is (a) documentation of the
infra as code, and (b) the starting point if this graduates from POC.

## What's here

| File | Resources |
|---|---|
| `apis.tf` | All required `google_project_service` enablements |
| `storage.tf` | Iceberg data bucket + raw/archive/work buckets + worker SA object IAM |
| `pubsub.tf` | Event topic + subscription |
| `artifact_registry.tf` | Docker repo for flex-template images |
| `wif.tf` | Workload identity pool + OIDC provider (Snowflake trust); outputs the `OAUTH_AUDIENCE` |
| `iam.tf` | Worker SA project roles, GCS service-agent Pub/Sub grant, Snowflake WIF principal grants |
| `trigger_function.tf` | Cloud Run function (gen2) + Eventarc GCS trigger for the batch pipeline |

## Reverse leg (BigQuery Omni) — separate module

The reverse direction (reading S3-resident Iceberg from BigQuery Omni) lives in
its own root module, **[`omni-reverse/`](omni-reverse/)**, because it spans two
clouds (adds the AWS provider) and shouldn't force AWS credentials on this
forward-leg apply. It codifies the BigQuery connection, the AWS IAM role +
web-identity trust (breaking the connection↔identity circular dependency), the
S3 read policy, the dataset, and the external Iceberg table. See its README for
the two-phase apply.

## Adopting the already-created resources

Everything exists, so `terraform apply` from scratch would collide. Import
first (Terraform 1.5+ `import` blocks or CLI), e.g.:

```
terraform import google_storage_bucket.raw scs-raw
terraform import google_pubsub_topic.events projects/<project>/topics/iceberg-poc-events
terraform import google_iam_workload_identity_pool.snowflake projects/<project>/locations/global/workloadIdentityPools/snowflake-pool
...
```

`google_project_iam_member` / bucket IAM members import with
`"<project> <role> <member>"` triples. Expect a few no-op diffs (labels,
soft-delete policies) on first plan.

## Two-phase apply (the WIF ↔ Snowflake circular dependency)

The trust setup crosses two control planes that each need the other's output:

1. **Phase 1**: apply with `snowflake_wif_subject = null`. Creates the pool +
   provider; note the `oauth_audience` output.
2. In Snowflake: `CREATE CATALOG INTEGRATION` using that audience, then
   `DESC CATALOG INTEGRATION` → copy `WORKLOAD_IDENTITY_FEDERATION_SUBJECT`.
3. **Phase 2**: apply again with `snowflake_wif_subject` set. Grants
   `biglake.viewer` + `serviceusage.serviceUsageConsumer` to the principal.

Remember: `CREATE OR REPLACE CATALOG INTEGRATION` mints a NEW subject —
update the variable and re-apply, or the grants point at a dead principal.

## Outside Terraform — and why

| Thing | Why it can't (or shouldn't) live in TF |
|---|---|
| **BigLake/Lakehouse Iceberg REST catalog** | No `google_*` provider resource for `biglake iceberg catalogs` (the v1 `google_biglake_catalog` resource is the *old* BigLake Metastore, a different API). Stays in `scripts/01_gcp_catalog_setup.sh` until the provider catches up. |
| **Catalog runtime SA bucket grant** | The `blirc-...@gcp-sa-biglakerestcatalog` SA only materializes on catalog creation (eventually consistent) — no data source to reference it. Scripted in `01`. |
| **Flex template images + spec JSONs** | Build artifacts, not infrastructure: `gcloud dataflow flex-template build` runs Cloud Build. Belongs in CI (`scripts/05_build_templates.sh`). |
| **Dataflow jobs themselves** | Runtime workloads. (`google_dataflow_flex_template_job` exists, but managing a POC streaming job's lifecycle through TF state fights every relaunch — e.g. the 1 h BigLake token limitation.) |
| **Snowflake objects** (catalog integration, catalog-linked DB, warehouse) | A Snowflake TF provider exists, but Iceberg REST catalog integrations and catalog-linked databases aren't first-class resources in it yet; you'd be templating raw SQL through an escape hatch (`snowflake_execute`) for no drift detection. Stays in `sql/`. |
| **Iceberg namespaces/tables** | Data plane, owned by the pipelines (auto-created by the managed Iceberg sink / Spark). Schema belongs to producers, not infra code. |
| **Local workstation config** | `gcloud auth`, `snow` connection, password env var — per-user, never in IaC. |
