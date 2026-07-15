#!/usr/bin/env bash
# Build both Dataflow flex templates: custom launcher image (with Java for the
# cross-language managed Iceberg transform) via Cloud Build, then template spec.
# Usage: ./scripts/05_build_templates.sh [streaming|batch]   (default: both)
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}" "${REGION:?}"

AR_REPO="${AR_REPO:-dataflow-templates}"
WORK_BUCKET="${WORK_BUCKET:-scs-dataflow}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

build_one() {
  local name="$1"
  local image="$REGION-docker.pkg.dev/$PROJECT_ID/$AR_REPO/${name}:latest"
  gcloud builds submit "$REPO_ROOT/dataflow" \
    --project "$PROJECT_ID" \
    --config "$REPO_ROOT/dataflow/cloudbuild.yaml" \
    --substitutions "_IMAGE=${image},_TEMPLATE=${name}"
  gcloud dataflow flex-template build "gs://$WORK_BUCKET/templates/${name}.json" \
    --project "$PROJECT_ID" \
    --image "$image" \
    --sdk-language PYTHON \
    --metadata-file "$REPO_ROOT/dataflow/${name}/metadata.json"
  echo "Built: gs://$WORK_BUCKET/templates/${name}.json"
}

case "${1:-both}" in
  streaming) build_one streaming ;;
  batch)     build_one batch ;;
  both)      build_one streaming; build_one batch ;;
  *) echo "usage: $0 [streaming|batch]" >&2; exit 1 ;;
esac
