# Runbook: Reverse Direction — Read S3 Iceberg from GCP (BigQuery Omni)

The mirror image of the zero-copy federation path. Instead of Snowflake (AWS)
reading GCS-resident Iceberg, this reads **S3-resident Iceberg from BigQuery
(GCP)** — with no copy into GCS.

Executed successfully 2026-07-20 against a personal AWS account and the GCP
project used throughout this POC.

## The key architectural difference

In the GCS -> Snowflake direction, the consumer's compute lives in AWS and
**pulls raw bytes across the cloud boundary** — so every scan pays egress.

BigQuery Omni inverts this: Google runs **BigQuery compute inside AWS**, in the
S3 bucket's region, and executes the query next to the data. The raw data
never leaves AWS during the scan; only the (usually small) result set returns
to GCP. The per-scan bulk-egress problem of the other direction largely goes
away.

## Prerequisites

- The S3 data must be an **Iceberg table in a BigQuery Omni-supported AWS
  region**: `us-east-1`, `us-west-2`, `eu-west-1`, `eu-central-1`,
  `ap-northeast-2`, `ap-southeast-2`. (Our other POC data sits in `us-east-2`,
  which Omni does **not** cover — hence a fresh table in `us-east-1`.)
- Omni runs on-demand analysis pricing or Enterprise-edition reservations
  (no reservation required for on-demand). Standard and Enterprise Plus
  editions do not work in Omni regions.
- Local AWS credentials with S3 + IAM rights; `bq` CLI authed to the GCP
  project; `pyiceberg` for the table write (no Spark needed).

## 1. Land an Iceberg table in an Omni region (S3)

`scripts/omni_write_iceberg.py` creates the bucket and writes a small Iceberg
table with PyIceberg — pure Python, no Spark/Dataproc/NAT. It prints the
`metadata.json` location BigQuery will point at.

```bash
python scripts/omni_write_iceberg.py --bucket <s3-bucket>   # region us-east-1
# -> METADATA_LOCATION: s3://<bucket>/warehouse/demo/orders/metadata/00001-....metadata.json
```

## 2. AWS IAM role for BigQuery Omni to assume

Omni authenticates via **web-identity federation** (`accounts.google.com`), not
a static key. Create a role with a read-only S3 policy and a placeholder trust;
tighten the trust once BigQuery hands back its identity (step 3).

```bash
python scripts/omni_aws_role.py create --bucket <s3-bucket>
# -> role ARN
```

## 3. BigQuery Omni connection, then close the trust loop

```bash
bq mk --connection --connection_type='AWS' --location='aws-us-east-1' \
  --iam_role_id='<ROLE_ARN>' omni_s3_conn
# -> "Identity: '<NUMERIC_SUBJECT>'"

python scripts/omni_aws_role.py trust --identity <NUMERIC_SUBJECT>
```

Then raise the role's max session duration to 12 hours — Omni requires it:

```bash
aws iam update-role --role-name bq-omni-s3-reader --max-session-duration 43200
# (or the boto3 one-liner in scripts/omni_aws_role.py)
```

## 4. External Iceberg table + cross-cloud query

```sql
-- dataset must be in the Omni region
-- bq mk --location=aws-us-east-1 --dataset <project>:omni_s3

CREATE OR REPLACE EXTERNAL TABLE omni_s3.orders
  WITH CONNECTION `<project>.aws-us-east-1.omni_s3_conn`
  OPTIONS ( format = 'ICEBERG',
            uris = ['<METADATA_LOCATION from step 1>'] );

SELECT COUNT(*), ROUND(SUM(amount),2) FROM omni_s3.orders;
-- returned 4 / 467.75 in the POC — compute ran in AWS, only the result crossed back
```

## Sharp edges (hit during the build)

| Symptom | Fix |
|---|---|
| `bq mk --properties '{"aws":...}'` -> "Unknown name aws" | Use the `--iam_role_id` flag form, not the `--properties` JSON, in current `bq`. |
| Query: "session duration of your IAM Role is smaller than requested" | Raise the role's `MaxSessionDuration` to `43200` (12h). |
| Data is in `us-east-2` | Not an Omni region — move/write it to `us-east-1` (or another supported region). |
| `pa.table(...)` ValueError: "numpy.dtype size changed" | Local numpy/pandas ABI mismatch — `pip install -U 'pandas>=2.2'` to match numpy 2.x. |

## Cost & latency notes

- **Egress:** no per-scan bulk egress — compute runs in AWS beside the data,
  only results return. You pay Omni on-demand analysis (per TB scanned, in the
  AWS region) plus small cross-cloud transfer on results.
- **Reach:** BigQuery (via Omni) and Dataproc/Spark (with an S3 connector) can
  read S3 from GCP. It is not universal — same "only some engines" caveat as
  the forward direction.

## Teardown

```bash
bq rm -f -t <project>:omni_s3.orders
bq rm -r -f -d <project>:omni_s3
bq rm -f --connection --location=aws-us-east-1 omni_s3_conn
# AWS: delete role bq-omni-s3-reader, empty + delete the S3 bucket
```
