locals {
  services = [
    "biglake.googleapis.com",
    "dataproc.googleapis.com",
    "dataflow.googleapis.com",
    "pubsub.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "eventarc.googleapis.com",
    "run.googleapis.com",
    "cloudfunctions.googleapis.com",
    "iam.googleapis.com",
    "sts.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.services)
  service            = each.value
  disable_on_destroy = false
}
