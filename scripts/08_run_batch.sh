#!/usr/bin/env bash
# Run the batch demo: generate a CSV of random events and drop it into the raw
# bucket. The Eventarc trigger launches the batch flex template, which writes
# to Iceberg and archives the file on success.
# Usage: ./scripts/08_run_batch.sh [N]   (default 25 rows)
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}"

RAW_BUCKET="${RAW_BUCKET:-scs-raw}"
N="${1:-25}"
FILE="events-$(date -u +%Y%m%d-%H%M%S).csv"
TMP="$(mktemp -d)/$FILE"

python - "$N" > "$TMP" <<'PYEOF'
import random, sys
from datetime import datetime, timezone

print("event_id,source,amount,published_at")
sources = ["batch-loader", "erp-export", "legacy-feed"]
ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
for _ in range(int(sys.argv[1])):
    print(f"{random.randint(100_000, 999_999)},"
          f"{random.choice(sources)}-{random.randint(1, 9)},"
          f"{round(random.uniform(1, 1000), 2)},{ts}")
PYEOF

echo "Generated $N rows -> $FILE; dropping into gs://$RAW_BUCKET/ ..."
gsutil cp "$TMP" "gs://$RAW_BUCKET/$FILE"
echo "Dropped. Watch the job:  gcloud dataflow jobs list --project $PROJECT_ID --region ${REGION:-us-central1} --status active"
echo "On success the file moves to gs://scs-raw-archive/$FILE"
