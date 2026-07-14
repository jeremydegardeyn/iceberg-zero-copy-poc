#!/usr/bin/env bash
# Phase 2: create (or --append to) the test Iceberg table via Dataproc Serverless.
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}" "${REGION:?}" "${BUCKET:?}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
gsutil cp "$SCRIPT_DIR/create_table.py" "gs://$BUCKET/jobs/create_table.py"

gcloud dataproc batches submit pyspark "gs://$BUCKET/jobs/create_table.py" \
  --project "$PROJECT_ID" --region "$REGION" --version 2.3 \
  -- --catalog "$BUCKET" --project "$PROJECT_ID" ${1:-}
