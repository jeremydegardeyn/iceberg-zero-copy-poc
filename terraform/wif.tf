# Snowflake -> GCP workload identity federation trust.
# Input: issuer URL from SELECT SYSTEM$GET_WORKLOAD_IDENTITY_ISSUER_URL();
resource "google_iam_workload_identity_pool" "snowflake" {
  workload_identity_pool_id = var.wif_pool_id
  depends_on                = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "snowflake" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.snowflake.workload_identity_pool_id
  workload_identity_pool_provider_id = var.wif_provider_id
  oidc {
    issuer_uri = var.snowflake_issuer_url
  }
  attribute_mapping = {
    "google.subject" = "assertion.sub"
  }
}

# OAUTH_AUDIENCE for the Snowflake catalog integration:
output "oauth_audience" {
  value = "//iam.googleapis.com/${google_iam_workload_identity_pool_provider.snowflake.name}"
}
