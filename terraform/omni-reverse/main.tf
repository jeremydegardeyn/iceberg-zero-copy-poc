locals {
  # Constructed from vars, NOT from the aws_iam_role resource — so the BigQuery
  # connection below depends only on a string, breaking the otherwise-circular
  # dependency (connection needs the role ARN; role trust needs the connection's
  # Google identity). Create order becomes: connection -> role -> policy.
  role_arn    = "arn:aws:iam::${var.aws_account_id}:role/${var.omni_role_name}"
  bq_location = "aws-${var.aws_region}"
}

# ---------------------------------------------------------------------------
# S3 bucket that holds the Iceberg table.
# The Iceberg data + metadata are written separately (PyIceberg / Spark) — that
# is a data-plane step, not infrastructure. See scripts/omni_write_iceberg.py.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "omni" {
  count  = var.create_bucket ? 1 : 0
  bucket = var.omni_bucket
}

# ---------------------------------------------------------------------------
# BigQuery Omni connection (BigLake on AWS). iam_role_id is the constructed
# string above, so this resource does not reference aws_iam_role.
# ---------------------------------------------------------------------------
resource "google_bigquery_connection" "omni" {
  connection_id = var.connection_id
  location      = local.bq_location
  aws {
    access_role {
      iam_role_id = local.role_arn
    }
  }
}

# ---------------------------------------------------------------------------
# IAM role BigQuery Omni assumes via web-identity federation. The trust pins the
# sub to the connection's Google identity (a computed attribute of the
# connection), and the 12-hour max session duration is an Omni requirement.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "omni" {
  name                 = var.omni_role_name
  max_session_duration = 43200 # 12h — Omni requests a 12-hour session

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = "accounts.google.com" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "accounts.google.com:sub" = google_bigquery_connection.omni.aws[0].access_role[0].identity
        }
      }
    }]
  })
}

# Least privilege: read-only, scoped to the one bucket.
resource "aws_iam_role_policy" "omni_s3_read" {
  name = "s3-read"
  role = aws_iam_role.omni.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "arn:aws:s3:::${var.omni_bucket}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = "arn:aws:s3:::${var.omni_bucket}"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Dataset in the Omni region.
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "omni" {
  dataset_id = var.dataset_id
  location   = local.bq_location
}

# ---------------------------------------------------------------------------
# External Iceberg table over the S3 metadata.json. PHASE 2 — only created once
# omni_metadata_uri is supplied (the PyIceberg write prints it). Depends on the
# read policy so the grant exists before BigQuery validates the table.
# NOTE: source_format = "ICEBERG" requires a recent google provider. If yours
# rejects it, create the table with the SQL DDL in the runbook (step 4) and
# `terraform import`, or omit this resource and keep the rest in Terraform.
# ---------------------------------------------------------------------------
resource "google_bigquery_table" "orders" {
  count               = var.omni_metadata_uri == null ? 0 : 1
  dataset_id          = google_bigquery_dataset.omni.dataset_id
  table_id            = var.table_id
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "ICEBERG"
    connection_id = google_bigquery_connection.omni.id
    source_uris   = [var.omni_metadata_uri]
  }

  depends_on = [aws_iam_role_policy.omni_s3_read]
}
