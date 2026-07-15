#!/usr/bin/env bash
# Deploy the Cloud Run function that launches the batch flex template whenever
# a CSV lands in the raw bucket. Trigger location must match the bucket region.
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}" "${PROJECT_NUMBER:?}" "${REGION:?}" "${BUCKET:?}"

RAW_BUCKET="${RAW_BUCKET:-scs-raw}"
ARCHIVE_BUCKET="${ARCHIVE_BUCKET:-scs-raw-archive}"
WORK_BUCKET="${WORK_BUCKET:-scs-dataflow}"
TRIGGER_LOCATION="${TRIGGER_LOCATION:-us-east1}"   # = scs-raw bucket region
DF_REGION="${DF_REGION:-us-east1}"                 # where batch jobs run (see 06)
SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

ENV_VARS="PROJECT_ID=$PROJECT_ID,REGION=$DF_REGION,CATALOG=$BUCKET,TEMPLATE_PATH=gs://$WORK_BUCKET/templates/batch.json,TEMP_LOCATION=gs://$WORK_BUCKET/tmp/batch,ARCHIVE_BUCKET=$ARCHIVE_BUCKET"

# Launcher: CSV lands in the drop bucket -> start the batch template.
gcloud functions deploy iceberg-poc-batch-trigger \
  --project "$PROJECT_ID" --gen2 --region "$REGION" \
  --runtime python312 \
  --source "$(dirname "$SCRIPT_DIR")/trigger" \
  --entry-point on_file \
  --service-account "$SA" \
  --trigger-location "$TRIGGER_LOCATION" \
  --trigger-event-filters "type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters "bucket=$RAW_BUCKET" \
  --set-env-vars "$ENV_VARS" \
  --memory 256Mi --max-instances 3

# Archiver: Dataflow job-status event -> archive the input on JOB_STATE_DONE.
# Event-driven (no polling) because batch jobs outlive the 540s function limit.
gcloud functions deploy iceberg-poc-batch-archiver \
  --project "$PROJECT_ID" --gen2 --region "$DF_REGION" \
  --runtime python312 \
  --source "$(dirname "$SCRIPT_DIR")/trigger" \
  --entry-point on_job_status \
  --service-account "$SA" \
  --trigger-location "$DF_REGION" \
  --trigger-event-filters "type=google.cloud.dataflow.job.v1beta3.statusChanged" \
  --set-env-vars "$ENV_VARS" \
  --memory 256Mi --max-instances 3

echo "Deployed: gs://$RAW_BUCKET drop -> launch; Dataflow DONE event -> archive."
