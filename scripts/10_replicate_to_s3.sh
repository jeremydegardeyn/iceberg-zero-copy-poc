#!/usr/bin/env bash
# ADR-0002 option C: replicate one Iceberg table GCS -> S3 (point-in-time).
#   1. Dataproc batch: rewrite_table_path stages s3://-prefixed metadata + copy plan
#   2. Execute the copy plan (GCS reads via gsutil ADC, S3 writes via boto3)
# Re-run to refresh the replica after new commits (each run copies the delta
# plan for the current snapshot; the replica is stale by design between runs).
#
# Usage: ./scripts/10_replicate_to_s3.sh <s3-bucket> [namespace.table]
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}" "${REGION:?}" "${BUCKET:?}"

S3_BUCKET="${1:?usage: 10_replicate_to_s3.sh <s3-bucket> [namespace.table]}"
TABLE="${2:-shared_aws.orders}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

gsutil cp "$SCRIPT_DIR/rewrite_table_path.py" "gs://$BUCKET/jobs/rewrite_table_path.py"

echo "== rewrite_table_path via Dataproc Serverless (~4 min)..."
OUT=$(gcloud dataproc batches submit pyspark "gs://$BUCKET/jobs/rewrite_table_path.py" \
  --project "$PROJECT_ID" --region "$REGION" --version 2.3 \
  -- --catalog "$BUCKET" --project "$PROJECT_ID" --table "$TABLE" --target_bucket "$S3_BUCKET" \
  2>&1 | tee /dev/stderr | grep "REWRITE_RESULT" | tail -1)

[ -n "$OUT" ] || { echo "ERROR: rewrite_table_path emitted no REWRITE_RESULT — check the Dataproc batch logs" >&2; exit 1; }
LATEST_VERSION=$(echo "$OUT" | sed -E 's/.*latest_version=([^ ]+).*/\1/')
FILE_LIST=$(echo "$OUT" | sed -E 's/.*file_list=([^ ]+).*/\1/')
echo "== latest_version: $LATEST_VERSION"
echo "== copy plan:      $FILE_LIST"

echo "== executing copy plan..."
python "$SCRIPT_DIR/s3_copy_filelist.py" --file_list "$FILE_LIST"

echo
echo "Replica ready. In Snowflake (sql/04_s3_replica.sql), METADATA_FILE_PATH is:"
echo "  ${TABLE#*.}/metadata/$LATEST_VERSION   (relative to the external volume base)"
