#!/usr/bin/env bash
# Phase 3 (GCP side): workload identity federation trust for Snowflake.
# Step 1 needs ISSUER_URL from Snowflake:  SELECT SYSTEM$GET_WORKLOAD_IDENTITY_ISSUER_URL();
# Step 2 needs WIF_SUBJECT from Snowflake: DESC CATALOG INTEGRATION biglake_catalog_int;
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}" "${PROJECT_NUMBER:?}" "${POOL_ID:?}" "${PROVIDER_ID:?}"

STEP="${1:?usage: 03_wif_setup.sh pool <ISSUER_URL> | grant <WIF_SUBJECT>}"

case "$STEP" in
  pool)
    ISSUER_URL="${2:?pass the issuer URL from Snowflake}"
    gcloud iam workload-identity-pools create "$POOL_ID" \
      --project "$PROJECT_ID" --location global || true
    gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_ID" \
      --project "$PROJECT_ID" --location global \
      --workload-identity-pool "$POOL_ID" \
      --issuer-uri "$ISSUER_URL" \
      --attribute-mapping "google.subject=assertion.sub"
    echo
    echo "OAUTH_AUDIENCE for sql/01_catalog_integration.sql:"
    echo "//iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL_ID/providers/$PROVIDER_ID"
    ;;
  grant)
    WIF_SUBJECT="${2:?pass WORKLOAD_IDENTITY_FEDERATION_SUBJECT from DESC CATALOG INTEGRATION}"
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
      --role roles/biglake.viewer \
      --member "principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL_ID/subject/$WIF_SUBJECT"
    echo "Granted. Now run SELECT SYSTEM\$VERIFY_CATALOG_INTEGRATION('biglake_catalog_int'); in Snowflake."
    ;;
  *) echo "unknown step: $STEP" >&2; exit 1 ;;
esac
