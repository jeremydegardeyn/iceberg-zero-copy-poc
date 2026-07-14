# Copy to env.sh, fill in, then: source env.sh
export PROJECT_ID="my-gcp-project"
export REGION="us-central1"
export BUCKET="iceberg-poc-${PROJECT_ID}"   # bucket name = catalog name (single-bucket mode)
export POOL_ID="snowflake-pool"
export PROVIDER_ID="snowflake-provider"

export PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
