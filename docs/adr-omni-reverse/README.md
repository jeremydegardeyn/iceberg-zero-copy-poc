# Architecture Decision Records — Reverse Leg (BigQuery Omni)

A **separate** ADR set from [`../adr/`](../adr/). Those records cover the forward
direction (GCS-resident Iceberg read from AWS consumers). These cover the
**reverse** direction: an **S3-resident Iceberg lake read from GCP via BigQuery
Omni**, with GCP-native consumers (Cloud Run, Compute Engine/Python, Dataflow,
Bigtable, AlloyDB) downstream.

They are numbered `R00x` to keep them visibly distinct from the forward-leg
`000x` records.

Format: lightweight MADR. Status lifecycle: Proposed → Accepted → Superseded.

| # | Decision | Status |
|---|---|---|
| [R001](R001-omni-read-in-place-over-copy.md) | Read S3 in place with BigQuery Omni over copying into GCS | Accepted |
| [R002](R002-keyless-web-identity-federation.md) | Keyless web-identity federation over static AWS keys | Accepted |
| [R003](R003-materialize-for-native-consumers.md) | Materialize a GCP-side copy for non-Omni native consumers | Accepted |
| [R004](R004-terraform-control-plane-script-data-plane.md) | Terraform the control plane; script the data plane | Accepted |
| [R005](R005-omni-region-placement.md) | Place Omni-consumed data in an Omni-supported region | Accepted |

Companion runbook:
[`../runbook-omni-reverse.md`](../runbook-omni-reverse.md). Terraform:
[`../../terraform/omni-reverse/`](../../terraform/omni-reverse/).
