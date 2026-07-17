#!/usr/bin/env bash
# Run scripts/spark_direct_s3_glue.py on Dataproc Serverless — Spark writes
# Iceberg straight to S3+Glue, no landing zone. Requires a Cloud NAT gateway
# on the job's VPC/region (Dataproc Serverless has no internet egress
# otherwise — AWS is public internet). See docs/adr/0007 for how this was found.
#
# One-time NAT setup (skip if already present):
#   gcloud compute routers create dataproc-nat-router --network default --region us-east1
#   gcloud compute routers nats create dataproc-nat --router dataproc-nat-router \
#     --region us-east1 --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges
#
# Usage: ./scripts/15_run_spark_direct_s3.sh <s3-bucket> [namespace] [table]
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}"

S3_BUCKET="${1:?usage: 15_run_spark_direct_s3.sh <s3-bucket> [namespace] [table]}"
NAMESPACE="${2:-direct_test}"
TABLE="${3:-orders}"
DF_REGION="${DF_REGION:-us-east1}"   # must have the Cloud NAT gateway
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

: "${AWS_ACCESS_KEY_ID:?export AWS creds first}" "${AWS_SECRET_ACCESS_KEY:?}"

gsutil cp "$SCRIPT_DIR/spark_direct_s3_glue.py" "gs://$BUCKET/jobs/spark_direct_s3_glue.py"

gcloud dataproc batches submit pyspark "gs://$BUCKET/jobs/spark_direct_s3_glue.py" \
  --project "$PROJECT_ID" --region "$DF_REGION" --version 2.3 \
  --properties "spark.driver.extraJavaOptions=-Daws.accessKeyId=$AWS_ACCESS_KEY_ID -Daws.secretAccessKey=$AWS_SECRET_ACCESS_KEY,spark.executor.extraJavaOptions=-Daws.accessKeyId=$AWS_ACCESS_KEY_ID -Daws.secretAccessKey=$AWS_SECRET_ACCESS_KEY" \
  -- --catalog "$S3_BUCKET" --namespace "$NAMESPACE" --table "$TABLE"
