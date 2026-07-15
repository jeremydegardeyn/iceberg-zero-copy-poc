output "worker_service_account" {
  value = local.worker_sa
}

output "topic" {
  value = google_pubsub_topic.events.id
}

output "subscription" {
  value = google_pubsub_subscription.events.id
}

output "template_image_prefix" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.templates.repository_id}"
}
