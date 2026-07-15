#!/usr/bin/env bash
# Phase 1: enable API, create bucket + Iceberg REST catalog (credential vending), grant runtime SA storage access.
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}" "${REGION:?}" "${BUCKET:?}"

gcloud services enable biglake.googleapis.com --project "$PROJECT_ID"

gsutil ls -b "gs://$BUCKET" >/dev/null 2>&1 || gsutil mb -l "$REGION" -p "$PROJECT_ID" "gs://$BUCKET"

gcloud biglake iceberg catalogs create "$BUCKET" \
  --project "$PROJECT_ID" \
  --catalog-type gcs-bucket \
  --credential-mode vended-credentials

# Runtime SA gets NO storage access by default; vending fails without this grant.
# SA creation is eventually consistent — retry until it appears.
echo "Looking up catalog runtime service account..."
for i in $(seq 1 10); do
  RUNTIME_SA=$(gcloud biglake iceberg catalogs describe "$BUCKET" --project "$PROJECT_ID" \
    --format='value(biglakeServiceAccount)' 2>/dev/null || true)
  [ -n "${RUNTIME_SA:-}" ] && break
  sleep 15
done
if [ -z "${RUNTIME_SA:-}" ]; then
  echo "Runtime SA not found in describe output — inspect manually:"
  gcloud biglake iceberg catalogs describe "$BUCKET" --project "$PROJECT_ID"
  exit 1
fi
gsutil iam ch "serviceAccount:${RUNTIME_SA}:roles/storage.objectUser" "gs://$BUCKET"
echo "Done. Catalog: $BUCKET  Runtime SA: $RUNTIME_SA"
