locals {
  worker_sa = "${var.project_number}-compute@developer.gserviceaccount.com"
  gcs_sa    = "service-${var.project_number}@gs-project-accounts.iam.gserviceaccount.com"

  worker_roles = [
    "roles/dataflow.worker",
    "roles/dataflow.admin",
    "roles/biglake.editor",
    "roles/artifactregistry.writer",
    "roles/cloudbuild.builds.builder",
    "roles/eventarc.eventReceiver",
    "roles/run.invoker",
  ]
}

resource "google_project_iam_member" "worker" {
  for_each = toset(local.worker_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${local.worker_sa}"
}

# The trigger function (runs as worker SA) launches templates that also run as it.
resource "google_service_account_iam_member" "worker_self_use" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${local.worker_sa}"
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${local.worker_sa}"
}

# Eventarc GCS triggers require the GCS service agent to publish to Pub/Sub.
resource "google_project_iam_member" "gcs_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${local.gcs_sa}"
}

# Snowflake WIF principal grants — PHASE 2 (subject exists only after the
# Snowflake catalog integration is created; see README two-phase apply).
locals {
  snowflake_principal = var.snowflake_wif_subject == null ? null : (
    "principal://iam.googleapis.com/${google_iam_workload_identity_pool.snowflake.name}/subject/${var.snowflake_wif_subject}"
  )
  snowflake_roles = [
    "roles/biglake.viewer",
    "roles/serviceusage.serviceUsageConsumer", # required by x-goog-user-project header
  ]
}

resource "google_project_iam_member" "snowflake" {
  for_each = var.snowflake_wif_subject == null ? toset([]) : toset(local.snowflake_roles)
  project  = var.project_id
  role     = each.value
  member   = local.snowflake_principal
}
