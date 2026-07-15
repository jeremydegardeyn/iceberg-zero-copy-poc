# Iceberg data bucket — the BigLake catalog itself is NOT terraform-able (see README).
resource "google_storage_bucket" "iceberg" {
  name                        = var.iceberg_bucket
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false
}

resource "google_storage_bucket" "raw" {
  name                        = var.raw_bucket
  location                    = var.raw_bucket_location
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "archive" {
  name                        = var.archive_bucket
  location                    = var.archive_bucket_location
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "work" {
  name                        = var.work_bucket
  location                    = var.work_bucket_location
  uniform_bucket_level_access = true
}

# Worker SA object access on all four buckets.
resource "google_storage_bucket_iam_member" "worker_objects" {
  for_each = {
    iceberg = google_storage_bucket.iceberg.name
    raw     = google_storage_bucket.raw.name
    archive = google_storage_bucket.archive.name
    work    = google_storage_bucket.work.name
  }
  bucket = each.value
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${local.worker_sa}"
}

# NOT HERE: the catalog runtime SA grant (blirc-...@gcp-sa-biglakerestcatalog...)
# — that SA only materializes after `gcloud biglake iceberg catalogs create`,
# which has no terraform resource. See README "Outside Terraform".
