#!/usr/bin/env bash
# Dataflow extension infra: APIs, Pub/Sub topic+subscription, Artifact Registry
# repo for flex-template images, IAM for the Dataflow worker / trigger SA.
set -euo pipefail
: "${PROJECT_ID:?source env.sh first}" "${PROJECT_NUMBER:?}" "${REGION:?}" "${BUCKET:?}"

TOPIC="${TOPIC:-iceberg-poc-events}"
SUBSCRIPTION="${SUBSCRIPTION:-${TOPIC}-sub}"
AR_REPO="${AR_REPO:-dataflow-templates}"
WORK_BUCKET="${WORK_BUCKET:-scs-dataflow}"
RAW_BUCKET="${RAW_BUCKET:-scs-raw}"
ARCHIVE_BUCKET="${ARCHIVE_BUCKET:-scs-raw-archive}"

gcloud services enable dataflow.googleapis.com pubsub.googleapis.com \
  cloudbuild.googleapis.com artifactregistry.googleapis.com \
  eventarc.googleapis.com run.googleapis.com cloudfunctions.googleapis.com \
  --project "$PROJECT_ID"

gcloud pubsub topics describe "$TOPIC" --project "$PROJECT_ID" >/dev/null 2>&1 \
  || gcloud pubsub topics create "$TOPIC" --project "$PROJECT_ID"
gcloud pubsub subscriptions describe "$SUBSCRIPTION" --project "$PROJECT_ID" >/dev/null 2>&1 \
  || gcloud pubsub subscriptions create "$SUBSCRIPTION" --project "$PROJECT_ID" \
       --topic "$TOPIC" --ack-deadline 60

gcloud artifacts repositories describe "$AR_REPO" --project "$PROJECT_ID" \
  --location "$REGION" >/dev/null 2>&1 \
  || gcloud artifacts repositories create "$AR_REPO" --project "$PROJECT_ID" \
       --location "$REGION" --repository-format docker

# One SA for everything at POC scale: the default compute SA runs Dataflow
# workers, the flex-template launcher, Cloud Build, and the trigger function.
SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
for role in roles/dataflow.worker roles/dataflow.admin roles/biglake.editor \
            roles/artifactregistry.writer roles/cloudbuild.builds.builder \
            roles/eventarc.eventReceiver roles/run.invoker; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --role "$role" --condition=None --member "serviceAccount:$SA" >/dev/null
done
# Trigger function launches templates that run AS this SA:
gcloud iam service-accounts add-iam-policy-binding "$SA" --project "$PROJECT_ID" \
  --role roles/iam.serviceAccountUser --member "serviceAccount:$SA" >/dev/null

# Object access: Iceberg data bucket + working/raw/archive buckets.
for b in "$BUCKET" "$WORK_BUCKET" "$RAW_BUCKET" "$ARCHIVE_BUCKET"; do
  gsutil iam ch "serviceAccount:${SA}:roles/storage.objectAdmin" "gs://$b"
done

# Eventarc GCS triggers require the GCS service agent to publish to Pub/Sub.
GCS_SA="service-${PROJECT_NUMBER}@gs-project-accounts.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --role roles/pubsub.publisher --condition=None \
  --member "serviceAccount:$GCS_SA" >/dev/null

echo "Done. Topic: $TOPIC  Subscription: $SUBSCRIPTION  AR repo: $AR_REPO  SA: $SA"
