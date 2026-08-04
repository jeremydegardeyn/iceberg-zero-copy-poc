# Provisioning Checklist — BigQuery Omni Reverse Leg

What to enable and what to bind to stand up this POC in a regulated
environment,
split by cloud and by **who** needs it: the person/pipeline provisioning
(broad, one-time) vs. the runtime identity Omni actually uses at query time
(narrow, permanent). Request the runtime grants as the standing state — the
provisioning grants should not persist past setup.

Scoped to exactly what this POC exercised — BigQuery Omni reading S3 Iceberg
via Glue federation. Nothing speculative and nothing for adjacent work is
listed, so every line here is something you actually need.

---

## 1. GCP — APIs to enable

| API | Why |
|---|---|
| `bigquery.googleapis.com` | Core — datasets, tables, jobs |
| `bigqueryconnection.googleapis.com` | Required to create the AWS connection (`bq mk --connection --connection_type=AWS`) |
| `iam.googleapis.com` | Read/inspect the connection's federated identity |
| `sts.googleapis.com` | Underlies the short-lived OIDC token BigQuery presents to AWS STS — no direct calls needed, but the API must be enabled in-project |
| `cloudresourcemanager.googleapis.com` | Needed if provisioning via Terraform/gcloud automation |
| `serviceusage.googleapis.com` | Needed only if the same identity is also enabling APIs (usually a separate platform-team step) |

**Not required for the reverse leg specifically** (only for the forward,
Snowflake-reads-GCS leg, or unrelated to this POC): `dataflow`, `pubsub`,
`cloudbuild`, `artifactregistry`, `eventarc`, `run`, `cloudfunctions`.
`biglake.googleapis.com` also falls in this bucket — **isolation-tested
2026-07-30**: with the API fully disabled, an existing Omni Iceberg query, a
brand-new AWS connection, and a brand-new external Iceberg table (create +
query) all succeeded. BigLake metastore is a forward-leg (Iceberg REST
catalog) dependency, not an Omni one — don't request it for this POC unless
you're also standing up the streaming/CDC pieces or the forward leg.

---

## 2. GCP — IAM, provisioning time (broad, temporary)

Whoever runs the `bq`/Terraform commands to stand this up:

- `roles/bigquery.connectionAdmin` — create the connection, read back its
  identity (`bigquery.connections.create`, `.get`, `.update`)
- `roles/bigquery.dataEditor` (scoped to the target dataset, or
  `roles/bigquery.admin` scoped to the project if creating the dataset too)
- `roles/serviceusage.serviceUsageAdmin` — only if this identity is also
  enabling the APIs above; in a regulated environment this is typically a separate platform
  request, not bundled with the POC owner's access

No GCP-side service account or IAM binding is created *for the connection
itself* — it authenticates outward to AWS via OIDC, not inward to GCP
resources. There is nothing on the GCP side analogous to a service account
key to protect.

## 3. GCP — IAM, runtime (narrow, standing)

Whoever/whatever queries the zero-copy dataset day to day:

- `roles/bigquery.dataViewer` — scoped to the dataset, not project-wide
- `roles/bigquery.jobUser` — to run queries (project-scoped; BigQuery has no
  dataset-scoped job-run role)
- `roles/bigquery.connectionUser` — scoped to the specific connection
  resource; without it, `CREATE EXTERNAL TABLE ... WITH CONNECTION` and
  `EXPORT DATA WITH CONNECTION` both fail even with `dataViewer`

If you also stand up the `EXPORT DATA` write path (§4 below), no *additional*
GCP role is needed beyond `connectionUser` — the write permission lives
entirely on the AWS side.

---

## 4. AWS — IAM

AWS has no "enable this API" step — IAM, S3, Glue, and STS are always on.
What's needed is permissions, split the same way.

### Runtime identity — the role Omni assumes (narrow, standing)

One IAM role, three pieces, all **proven in this POC**:

**Trust policy** — web-identity federation, pinned to the connection's
Google identity (obtained after step 2 below), 12-hour max session:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Federated": "accounts.google.com" },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": { "StringEquals": { "accounts.google.com:sub": "<connection identity>" } }
  }]
}
```
`MaxSessionDuration = 43200` — Omni requests a 12h session; anything lower
fails with "session duration ... smaller than the requested session duration."

**`s3-read`** — GetObject/GetObjectVersion on the objects, ListBucket/
GetBucketLocation on the bucket, scoped to the one bucket:

```json
{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],
 "Resource":"arn:aws:s3:::<bucket>/*"},
{"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],
 "Resource":"arn:aws:s3:::<bucket>"}
```

**`glue-read`** — only if federating an existing Glue catalog (recommended
over per-table external tables). Read-only Glue metadata actions, scoped to
catalog + database + tables:

```json
{"Effect":"Allow",
 "Action":["glue:GetDatabase","glue:GetDatabases","glue:GetTable",
           "glue:GetTables","glue:GetPartitions"],
 "Resource":["arn:aws:glue:<region>:<account>:catalog",
             "arn:aws:glue:<region>:<account>:database/<db>",
             "arn:aws:glue:<region>:<account>:table/<db>/*"]}
```
This is an IAM action grant, not a Glue resource policy or cross-account
share — there is no "sharing" step on the Glue side. `glue:*Write*` and
`glue:*Permissions*` are never granted; Omni cannot alter the catalog.

**`s3-export-write`** — only if you enable the `EXPORT DATA` path, and only while it is actually
running (scoped `PutObject`/`DeleteObject` on an export prefix, not the
whole bucket). Grant immediately before use, revoke after — this POC
scripted it as `omni_aws_role.py grant-write` / `revoke-write` for exactly
that reason.

**No bucket policy is required or was used.** The bucket has zero policy and
blocks public access; every grant flows through the one IAM role. Do not add
a bucket policy pinning a specific VPC endpoint id — proven in this POC to
**block** Omni entirely, since Omni arrives through Google's own endpoint,
not one visible in your account (see §5).

### Provisioning identity (broad, temporary)

Whoever stands up the role:

- `iam:CreateRole`, `iam:PutRolePolicy`, `iam:GetRole`,
  `iam:UpdateAssumeRolePolicy`, `iam:UpdateRole` (the last two are exercised
  twice each — trust gets tightened in step 4, and `UpdateRole` sets the
  12h session duration)
- `s3:CreateBucket` — only if the bucket doesn't already exist; the
  datalake team's existing bucket needs none of this
- `glue:CreateDatabase`/`glue:CreateTable` — only if you're also standing
  up the Glue catalog itself; not needed against an existing one

---

## 5. Regulated / restricted-network environments

Tested against the constraint that usually kills cross-cloud in a regulated
environment: public internet blocked, inter-cloud connectivity blocked. Two hops:

1. **Control plane (GCP) → data plane (AWS).** Google-managed VPN, per
   Google's docs. You provision nothing.
2. **Data plane → S3.** **Proven 2026-07-28**: arrives via a VPC endpoint
   that is **Google's**, not yours — your AWS account shows zero VPC
   endpoints. No PrivateLink, no peering, no firewall change needed.

**If a VPC-SC perimeter wraps the project**, add an egress rule allowing the
connection's identity out to the specific bucket (`externalResources:
's3://<bucket>'`) — this needs `accesscontextmanager.googleapis.com`
enabled and `roles/accesscontextmanager.policyAdmin`, which typically
sits with the security/platform team, not the POC owner. Request it as a
named exception, not a self-service grant.

**If your S3 bucket standard pins a specific VPC endpoint id** in its bucket
policy — proven to **block Omni outright** (§4). This is not fixable with
more IAM; it's a standards-exception conversation for security to make
before sign-off, not an engineering task. See
[`runbook-omni-reverse.md`](runbook-omni-reverse.md#pinning-a-specific-endpoint-blocks-omni--tested-2026-07-29).

---

## 6. Summary table — hand this to the provisioning ticket

| Side | Identity | Grant | Standing? |
|---|---|---|---|
| GCP | POC owner | `bigquery.connectionAdmin`, `bigquery.dataEditor` (dataset-scoped) | No — provisioning only |
| GCP | Query users/apps | `bigquery.dataViewer` (dataset), `bigquery.jobUser` (project), `bigquery.connectionUser` (connection) | Yes |
| AWS | POC owner | `iam:CreateRole`/`PutRolePolicy`/`UpdateAssumeRolePolicy`/`UpdateRole` | No — provisioning only |
| AWS | `bq-omni-s3-reader` role | trust (pinned identity, 12h session) + `s3-read` + `glue-read` | Yes |
| AWS | same role | `s3-export-write` (scoped prefix) | Only while exporting |

Nothing in this list requires a static AWS access key, a GCP service account
key, or a bucket policy — that absence is itself one of the findings worth
carrying into the review.

---

## Appendix — isolation-tested reductions

Every "not required" claim above that could plausibly be doubted was tested
by disabling the API/removing the grant in the live POC project, re-running
the proven operations, then restoring:

| Removed | Result | Date |
|---|---|---|
| `biglake.googleapis.com` | Query existing table ✅, create new connection ✅, create + query new external Iceberg table ✅ — all succeeded disabled | 2026-07-30 |

Restored immediately after each test; the original `omni_s3.orders` query
was re-verified working post-restore.

---

## Terraform

The whole control plane in this checklist is codified as a standalone module:
**[github.com/jeremydegardeyn/bigquery-omni-terraform](https://github.com/jeremydegardeyn/bigquery-omni-terraform)**
— one `terraform apply` creates the AWS role, both policies, the BigQuery
connection, and the dataset, resolving the connection↔identity circular
dependency automatically.
