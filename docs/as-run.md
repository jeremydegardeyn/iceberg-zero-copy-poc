# As-Run Log: 2026-07-14 POC Execution

Exact steps from a successful end-to-end run (a GCP project in `us-central1` → a Snowflake trial on AWS us-east-2, Enterprise edition). Follow top to bottom to reproduce. The generic runbook is `runbook.md`; this file records what actually happened, including the failures and their fixes (now folded back into the scripts/SQL).

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

## Teardown

`sql/99_teardown.sql` in Snowflake, then `./scripts/99_teardown_gcp.sh`.
