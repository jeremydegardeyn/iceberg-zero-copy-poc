variable "project_id" {
  type        = string
  description = "GCP project that owns the BigQuery Omni connection, dataset, and external table."
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS region holding the S3 Iceberg data. MUST be a BigQuery Omni-supported region (us-east-1, us-west-2, eu-west-1, eu-central-1, ap-northeast-2, ap-southeast-2). The BigQuery location is derived as aws-<region>."
}

variable "aws_account_id" {
  type        = string
  description = "12-digit AWS account id. Used to construct the role ARN as a string so the BigQuery connection does not depend on the aws_iam_role resource (breaks the trust<->identity cycle)."
}

variable "omni_role_name" {
  type        = string
  default     = "bq-omni-s3-reader"
  description = "Name of the IAM role BigQuery Omni assumes via web-identity federation."
}

variable "omni_bucket" {
  type        = string
  description = "S3 bucket holding the Iceberg table. The Iceberg data itself is written separately (PyIceberg / Spark) — see scripts/omni_write_iceberg.py."
}

variable "create_bucket" {
  type        = bool
  default     = true
  description = "Create the S3 bucket here. Set false to reuse an existing bucket (the read policy still references it by name)."
}

variable "connection_id" {
  type    = string
  default = "omni_s3_conn"
}

variable "dataset_id" {
  type    = string
  default = "omni_s3"
}

variable "table_id" {
  type    = string
  default = "orders"
}

# PHASE 2 — the metadata.json location is an output of the Iceberg write
# (scripts/omni_write_iceberg.py prints it). Leave null on the first apply to
# stand up the connection/role/dataset; set it and re-apply to create the table.
variable "omni_metadata_uri" {
  type        = string
  default     = null
  description = "s3://.../metadata/NNNNN-...metadata.json — set after the Iceberg table is written to create the BigQuery external table."
}
