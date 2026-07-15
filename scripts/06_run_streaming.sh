#!/usr/bin/env bash
# Run the streaming demo end to end:
#   1. publish N randomly generated test events to the topic (pre-step; the
#      pipeline reads a subscription, so nothing is lost while the job starts)
#   2. launch the streaming flex template
# Usage: ./scripts/06_run_streaming.sh [N]      (default 10 events)
#        ./scripts/06_run_streaming.sh cancel   (stop the job / worker cost)
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}" "${REGION:?}" "${BUCKET:?}"

TOPIC="${TOPIC:-iceberg-poc-events}"
SUBSCRIPTION="${SUBSCRIPTION:-${TOPIC}-sub}"
WORK_BUCKET="${WORK_BUCKET:-scs-dataflow}"
JOB_NAME="iceberg-poc-stream"
# Dataflow compute region; us-east1 co-locates with the work/raw buckets and
# dodged a us-central1 capacity event on 2026-07-15 (launcher VM zone is not
# controllable via --worker-zone).
DF_REGION="${DF_REGION:-us-east1}"

if [ "${1:-}" = "cancel" ]; then
  JOB_ID=$(gcloud dataflow jobs list --project "$PROJECT_ID" --region "$DF_REGION" \
    --status active --filter "name:$JOB_NAME" --format 'value(id)' | head -1)
  [ -n "$JOB_ID" ] || { echo "no active $JOB_NAME job"; exit 0; }
  gcloud dataflow jobs cancel "$JOB_ID" --project "$PROJECT_ID" --region "$DF_REGION"
  exit 0
fi

N="${1:-10}"
echo "Publishing $N random events to $TOPIC..."
python - "$N" "$PROJECT_ID" "$TOPIC" <<'PYEOF'
import json, random, subprocess, sys
from datetime import datetime, timezone

n, project, topic = int(sys.argv[1]), sys.argv[2], sys.argv[3]
sources = ["pos-terminal", "mobile-app", "web-checkout", "kiosk", "call-center"]
for i in range(1, n + 1):
    msg = json.dumps({
        "event_id": random.randint(10_000, 99_999),
        "source": f"{random.choice(sources)}-{random.randint(1, 20)}",
        "amount": round(random.uniform(1, 500), 2),
        "published_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    subprocess.run(["gcloud", "pubsub", "topics", "publish", topic,
                    "--project", project, "--message", msg],
                   check=True, capture_output=True, shell=(sys.platform == "win32"))
    print(f"  published {i}/{n}: {msg}")
PYEOF

echo "Launching streaming flex template..."
# e2 machine family (launcher AND workers) + Streaming Engine: modern plentiful
# VMs and service-side shuffle/state — best defense against zone stockouts.
gcloud dataflow flex-template run "$JOB_NAME" \
  --project "$PROJECT_ID" --region "$DF_REGION" \
  --worker-region "$DF_REGION" \
  --launcher-machine-type e2-standard-2 \
  --worker-machine-type e2-standard-2 \
  --enable-streaming-engine \
  --template-file-gcs-location "gs://$WORK_BUCKET/templates/streaming.json" \
  --temp-location "gs://$WORK_BUCKET/tmp/streaming" \
  --max-workers 1 \
  --parameters "project_id=$PROJECT_ID,catalog=$BUCKET,subscription=projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION,table=shared_aws.events,commit_seconds=30"

echo "Job launched. Events appear in Snowflake ~commit_seconds after workers start."
echo "Cancel with: $0 cancel   (token expires ~1h; relaunch for longer demos)"
