resource "google_pubsub_topic" "events" {
  name       = var.topic
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "events" {
  name                 = "${var.topic}-sub"
  topic                = google_pubsub_topic.events.id
  ack_deadline_seconds = 60
}
