# Cloud Run function (gen2) launching the batch flex template on CSV drop.
data "archive_file" "trigger_src" {
  type        = "zip"
  source_dir  = "${path.module}/../trigger"
  output_path = "${path.module}/.build/trigger.zip"
}

resource "google_storage_bucket_object" "trigger_src" {
  bucket = google_storage_bucket.work.name
  name   = "functions/trigger-${data.archive_file.trigger_src.output_md5}.zip"
  source = data.archive_file.trigger_src.output_path
}

resource "google_cloudfunctions2_function" "batch_trigger" {
  name     = "iceberg-poc-batch-trigger"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "on_file"
    source {
      storage_source {
        bucket = google_storage_bucket.work.name
        object = google_storage_bucket_object.trigger_src.name
      }
    }
  }

  service_config {
    available_memory      = "256Mi"
    max_instance_count    = 3
    service_account_email = local.worker_sa
    environment_variables = {
      PROJECT_ID     = var.project_id
      REGION         = var.region
      CATALOG        = var.iceberg_bucket
      TEMPLATE_PATH  = "gs://${google_storage_bucket.work.name}/templates/batch.json"
      TEMP_LOCATION  = "gs://${google_storage_bucket.work.name}/tmp/batch"
      ARCHIVE_BUCKET = google_storage_bucket.archive.name
    }
  }

  event_trigger {
    trigger_region        = var.raw_bucket_location # must match the bucket region
    event_type            = "google.cloud.storage.object.v1.finalized"
    service_account_email = local.worker_sa
    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.raw.name
    }
  }

  depends_on = [
    google_project_iam_member.gcs_pubsub,
    google_project_iam_member.worker,
  ]
}
