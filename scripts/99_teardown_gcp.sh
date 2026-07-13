#!/usr/bin/env bash
# Teardown GCP resources (run sql/99_teardown.sql in Snowflake first).
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}" "${BUCKET:?}" "${POOL_ID:?}"

gcloud biglake iceberg catalogs delete "$BUCKET" --project "$PROJECT_ID" || true
gsutil -m rm -r "gs://$BUCKET" || true
gcloud iam workload-identity-pools delete "$POOL_ID" --project "$PROJECT_ID" --location global || true
