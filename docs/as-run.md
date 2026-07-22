# As-Run Log: 2026-07-14 POC Execution

Exact steps from a successful end-to-end run (a GCP project in `us-central1` → a Snowflake trial on AWS us-east-2, Enterprise edition). Follow top to bottom to reproduce. The generic runbook is `runbook-zero-copy.md`; this file records what actually happened, including the failures and their fixes (now folded back into the scripts/SQL).

## 0. One-time workstation setup (Windows)

```powershell
# Snowflake CLI (needs Python 3.10+)
python -m pip install --user snowflake-cli
# pip --user scripts land here; add to PATH once:
# C:\Users\<you>\AppData\Roaming\Python\Python310\Scripts

# Connection: non-secret parts in config, password via env var (never in files)
snow connection add --connection-name poc --account <ORGNAME-ACCOUNT> --user <username> --no-interactive
snow connection set-default poc
[Environment]::SetEnvironmentVariable("SNOWFLAKE_CONNECTIONS_POC_PASSWORD", "<password>", "User")
# Snowflake CLI auto-reads SNOWFLAKE_CONNECTIONS_<NAME>_PASSWORD. Open a new terminal after setting.

gcloud auth login    # if tokens are stale, every gcloud call fails with "Reauthentication failed"
```

Sanity check (also reveals which cloud/region the trial landed on):

```
snow sql -q "SELECT CURRENT_REGION(), CURRENT_ROLE()"
# → AWS_US_EAST_2, ACCOUNTADMIN
```

Set a default warehouse once so no session ever needs `USE WAREHOUSE`:

```
snow sql -q "ALTER USER <username> SET DEFAULT_WAREHOUSE = COMPUTE_WH"
```

## 1. GCP: bucket + Iceberg REST catalog (~2 min)

```bash
cp env.example.sh env.sh   # set PROJECT_ID; everything else derives
source env.sh
./scripts/01_gcp_catalog_setup.sh
```

As-run fix: the script originally parsed the runtime SA from `credentialInfo.serviceAccount`; the field is actually `biglakeServiceAccount` (fixed in the script). The SA looks like `blirc-<project-number>-xxxx@gcp-sa-biglakerestcatalog.iam.gserviceaccount.com` and **must** get `roles/storage.objectUser` on the bucket or vending fails later.

## 2. GCP: create the test Iceberg table (~4 min)

```bash
./scripts/02_create_table.sh
```

Dataproc Serverless batch (runtime 2.3), creates `shared_aws.orders` with 3 rows. This is the *producer* simulation — see "Why Spark?" below.

## 3. Trust: Snowflake ↔ GCP workload identity federation (~5 min)

```bash
# 3a. Issuer URL from Snowflake:
snow sql -q "SELECT SYSTEM\$GET_WORKLOAD_IDENTITY_ISSUER_URL()"
# → https://identity.snowflake.com/oauth2/.../...

# 3b. Pool + provider on GCP (prints the OAUTH_AUDIENCE for the next step):
./scripts/03_wif_setup.sh pool '<issuer-url>'
```

3c. In Snowflake, run `sql/01_catalog_integration.sql` with `<BUCKET>`, `<PROJECT_ID>`, and the audience filled in. **`ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS` is mandatory** — without it the catalog-linked database refuses to create ("did not have credential vending enabled"). Grab `WORKLOAD_IDENTITY_FEDERATION_SUBJECT` from the `DESC` output.

```bash
# 3d. Grant the subject BOTH roles (script now does both):
./scripts/03_wif_setup.sh grant '<WIF_SUBJECT>'
```

As-run failures here, in order:
1. `--condition=None` required because the project's IAM policy has conditional bindings (fixed in script).
2. Verify failed with *"Caller does not have required permission to use project"* → the `x-goog-user-project` header requires `roles/serviceusage.serviceUsageConsumer` in addition to `roles/biglake.viewer` (script now grants both).
3. Recreating the catalog integration (to add `ACCESS_DELEGATION_MODE`) **minted a new WIF subject** → had to re-grant both roles to the new subject and remove the old grants. Any `CREATE OR REPLACE CATALOG INTEGRATION` invalidates prior grants.

IAM propagation is real: wait ~45 s after grants before verifying.

```
snow sql -q "SELECT SYSTEM\$VERIFY_CATALOG_INTEGRATION('biglake_catalog_int')"
# → {"success": true}
```

## 4. Snowflake: catalog-linked database + zero-copy read (~2 min)

```
snow sql -q "CREATE OR REPLACE DATABASE shared_gcp_data LINKED_CATALOG = ( CATALOG = 'biglake_catalog_int' )"
# discovery is async — wait ~60 s
snow sql -q "SHOW SCHEMAS IN DATABASE shared_gcp_data"          # shared_aws appears
snow sql -q "USE WAREHOUSE COMPUTE_WH; SELECT * FROM shared_gcp_data.shared_aws.orders"
# → 3 rows, read directly from GCS. No copy, no pipeline.
```

## 5. Freshness proof

```bash
./scripts/02_create_table.sh --append     # one new row via Spark on GCP
```

```
snow sql -q "USE WAREHOUSE COMPUTE_WH; SELECT COUNT(*) FROM shared_gcp_data.shared_aws.orders"
# → 4, within ~30 s of the append (CLD REFRESH_INTERVAL_SECONDS = 30)
```

## 6. Egress meter

```
snow sql -q "SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY WHERE transfer_type='DATA_LAKE' ORDER BY start_time DESC"
```

ACCOUNT_USAGE lags up to ~2 h — check this well after the reads.

## Extension run (2026-07-15 PM): streaming + batch flex templates

Both pipelines proven end-to-end into the same catalog, read zero-copy from
Snowflake. Full architecture: `dataflow/` (two flex templates, shared
Java-enabled launcher Dockerfile), `trigger/` (launcher + event-driven
archiver functions), `validation.yaml` + `scripts/validate.py` (integrity
controls), `terraform/` (IaC for everything terraform-able).

```bash
./scripts/04_pubsub_streaming_setup.sh   # topic+sub, AR repo, IAM
./scripts/05_build_templates.sh          # both templates via Cloud Build (~8 min each)
./scripts/07_deploy_batch_trigger.sh     # both functions (launch + archive)
./scripts/06_run_streaming.sh 10         # publish 10 random events, launch stream job
./scripts/08_run_batch.sh 25             # drop random CSV -> trigger -> batch job
./scripts/09_validate.sh                 # three-way integrity report
./scripts/06_run_streaming.sh cancel     # stop the streaming worker when done
```

Results: streaming Pub/Sub→Iceberg with `GoogleAuthManager` (auto-refreshing
auth, no 1 h token cliff — the static-token limitation in Google's guide is
avoidable); batch CSV→Iceberg with file-drop trigger and archive-on-success;
validation PASS with sums reconciling exactly (50 rows: source=lake=Snowflake,
25688.93 == 25688.93).

**Failures hit, in order (all fixed in the code here):**
1. `ZONE_RESOURCE_POOL_EXHAUSTED` us-central1-b — launcher VM ignores
   `--worker-zone`; moved compute to us-east1 + e2 machine family for launcher
   AND workers + Streaming Engine (stockouts are per-family-per-zone; CUDs
   don't reserve capacity, reservations do).
2. `Missing required option: project` — argparse abbreviation matching ate
   Beam's `--project` as a prefix of `--project_id`; `allow_abbrev=False`.
3. `Java must be installed` — managed Iceberg I/O is cross-language; custom
   launcher image (dataflow/Dockerfile) adds a JRE.
4. 400 `X-Iceberg-Access-Delegation` — vended-credentials catalogs require the
   header on ALL table ops (Spark had it; the Dataflow configs initially didn't).
   Streaming jobs mask this as an infinite retry loop while showing Running.
5. Phantom archive — flex-launcher `wait_until_finish()` is a no-op; it
   archived the input mid-preflight. Archive is now event-driven via a Dataflow
   `statusChanged` Eventarc function (batch jobs run ~12 min, past the 540 s
   event-function limit, so polling in the launch function can't work either).
6. Cancelling a streaming job discards Pub/Sub messages already acked into
   Streaming Engine state — use drain, or expect to republish.

**Observed latencies:** batch job wall-clock ~12 min (fixed startup dominates);
streaming publish→Iceberg commit ~90 s; Snowflake CLD table refresh 1–10 min
observed despite nominal `REFRESH_INTERVAL_SECONDS=30` — measure before
promising freshness SLAs. Local gotcha: installing apache-beam downgraded
protobuf and broke the snow CLI (`runtime_version` ImportError) — Beam is no
longer needed locally; `pip install protobuf==5.29.6` restores it.

## S3 replica increment (ADR-0002 option C) — EXECUTED 2026-07-16

Proven end-to-end against a personal AWS free-plan account: same `orders`
table read THREE ways with identical results (4 rows, sum=467.75) —
zero-copy from GCS via Snowflake, the S3 replica via Snowflake, and the S3
replica via **Athena over a Glue catalog** (no Snowflake, no GCP call at read
time). The Athena leg proves the multi-engine argument for replication:
Athena and Redshift cannot read `gs://` at all, so zero-copy federation is
Snowflake-only among common AWS consumers. Three as-run
fixes folded into the scripts: backtick-quote hyphenated catalog names in
Spark `CALL`; staging_location must live INSIDE the table location (vended
credentials are table-prefix-scoped); the rewrite copy plan is a Spark output
directory of part-files.

The sanctioned fallback for hot tables (ADR-0006 break-even) or S3-only
consumers: a point-in-time physical replica in S3, read intra-region by
Snowflake with zero per-query egress. Requires the user's own AWS account.

```bash
# one-time, needs AWS creds (env vars or ~/.aws/credentials) with S3+IAM rights:
python scripts/11_aws_replica_setup.py create --bucket <s3-bucket>   # bucket us-east-2 + role
# Snowflake: sql/04_s3_replica.sql steps 1-2 (external volume; DESC gives ARN+external id)
python scripts/11_aws_replica_setup.py trust --bucket <b> --iam_user_arn <arn> --external_id <id>

# per refresh:
./scripts/10_replicate_to_s3.sh <s3-bucket> shared_aws.orders   # rewrite_table_path + copy plan
# Snowflake: sql/04_s3_replica.sql steps 3-5 (object-store integration, ICEBERG TABLE, compare)
```

Key mechanics: Iceberg metadata stores absolute paths, so the replica needs
`rewrite_table_path` (stages s3://-prefixed metadata + emits a copy plan);
data files copy unchanged. The replica is stale by design between syncs —
refresh = re-run 10 + `ALTER ICEBERG TABLE ... REFRESH '<new metadata path>'`.
Bucket lives in us-east-2 (same region as the Snowflake account), so replica
reads are intra-region: the cross-cloud cost is paid once at sync, not per query.

## Landing-zone pattern (ADR-0007) — EXECUTED 2026-07-17

Full end-to-end: Pub/Sub → Dataflow (windowed boto3 writer) → S3 landing zone
→ AWS Glue Spark job → Iceberg table registered in Glue → Athena. Reconciled
exactly: 10 dataflow-streamed events (sum 100.00) + 15 seeded rows (sum
871.24) = 25 rows in `stream_events`.

**Measured:** publish → bytes landed in S3 = **68 s** (60 s window + flush).
Glue promotion job = **77–81 s** per run. Publish → queryable-as-Iceberg ≈
window + promotion cadence (~2.5–4 min if event-driven/scheduled tightly).

```bash
# GCP side: streaming template with sink=s3_landing (see 06_run_streaming.sh
# params); AWS side:
python scripts/14_glue_job.py setup --bucket <s3-bucket>   # role, script, job def
python scripts/14_glue_job.py run   --bucket <s3-bucket>   # landing -> Iceberg
```

**Why this pattern exists:** Dataflow CANNOT write Iceberg into Glue directly —
Iceberg's Glue client demands a reflectively-constructed credentials provider
(`StaticCredentialsProvider` has no `create()`/`create(Map)`), so static keys
fail at the catalog layer while working fine for plain S3 object writes. The
split puts every catalog operation on AWS-native compute (the Glue job carries
ZERO credentials — pure IAM role) and leaves GCP compute moving bytes only.

**Sharp edges hit (fixed in repo):**
1. Beam's `fileio.WriteToFiles` to s3:// is broken from Dataflow twice over:
   temp files stage to the GCS temp_location and finalize with the GCS
   filesystem (rejects s3://); with temp forced to S3 it dies with
   `AttributeError: 'str' object has no attribute 'get'`. Fix: plain boto3
   `put_object` per window in a DoFn.
2. Worker Python deps are NOT the launcher image's: boto3 in the Dockerfile
   `pip install` reaches only the launcher; workers need
   `FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE` (keep it minimal, no apache-beam).
3. Beam retry loops log at WARNING, not ERROR — a "healthy" monitor grepping
   ERROR misses them. Watch data arrival, not job state.
4. (Repeat offender) cancelling a streaming job discards acked messages — the
   replacement job sat idle on an empty subscription. Drain, or republish.

## Reverse direction (BigQuery Omni) — EXECUTED 2026-07-20

Proven: BigQuery read an S3-resident Iceberg table cross-cloud via Omni — same
4 rows / 467.75 aggregate, with compute running in AWS us-east-1 and only the
result returning to GCP (no bulk egress). Iceberg table written to a fresh
us-east-1 bucket with PyIceberg (no Spark/NAT); BQ Omni AWS connection with a
web-identity IAM role; external Iceberg table over the metadata.json; SELECT
from BigQuery. Full runbook: docs/runbook-omni-reverse.md. As-run fixes: `bq mk`
wants `--iam_role_id` not `--properties` JSON; the role's MaxSessionDuration
must be 12h (43200s); our other data was in us-east-2 which Omni does not cover;
local numpy/pandas ABI mismatch needed `pandas>=2.2`.

Also proven on this leg:
- **Dataflow can't direct-read Omni** — `scripts/omni_storage_read_test.py` ran
  the Storage Read API on `omni_s3.orders`: `InvalidArgument 400 ... Read API can
  be used to read temporary tables only in this region.` (materialization required).
- **Incremental CDC straight from S3** — `scripts/omni_incremental_cdc.py`
  emitted only the new-snapshot rows (the diff) and published a later diff to a
  Pub/Sub topic + pulled it back (topic torn down). Append-only scope.
- **`EXPORT DATA` from Omni to S3** — granted a scoped `s3-export-write` policy,
  ran `EXPORT DATA WITH CONNECTION ... OPTIONS(uri='s3://.../exports/orders/*')`,
  BigQuery Omni wrote a Parquet file (4 rows) to S3; read it back, deleted it,
  revoked write. Export runs in AWS, lands in S3 (not GCS).

## Teardown

`sql/99_teardown.sql` in Snowflake, then `./scripts/99_teardown_gcp.sh`.
Extension: `./scripts/06_run_streaming.sh cancel`, delete the two functions,
topic/subscription, AR repo (`terraform destroy` once imported, or by hand).
Reverse leg: drop the BQ external table/dataset/connection, delete the AWS
role and the us-east-1 bucket.
