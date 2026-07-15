#!/usr/bin/env bash
# Run the integrity-control validation (validation.yaml) across
# source CSVs / Iceberg catalog metadata / Snowflake.
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}" "${BUCKET:?}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# snow CLI reads the connection password from the Windows *user* env; pull it
# into this process if absent (Git Bash doesn't reload user env vars).
if [ -z "${SNOWFLAKE_CONNECTIONS_POC_PASSWORD:-}" ] && command -v powershell.exe >/dev/null; then
  SNOWFLAKE_CONNECTIONS_POC_PASSWORD="$(powershell.exe -NoProfile -Command \
    '[Environment]::GetEnvironmentVariable("SNOWFLAKE_CONNECTIONS_POC_PASSWORD","User")' | tr -d '\r')"
  export SNOWFLAKE_CONNECTIONS_POC_PASSWORD
fi

python "$SCRIPT_DIR/validate.py" \
  --config "$REPO_ROOT/validation.yaml" \
  --project_id "$PROJECT_ID" \
  --catalog "$BUCKET" \
  --snowflake_db shared_gcp_data
