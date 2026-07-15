resource "google_artifact_registry_repository" "templates" {
  repository_id = var.ar_repo
  location      = var.region
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}
