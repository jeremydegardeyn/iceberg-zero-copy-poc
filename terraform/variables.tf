variable "project_id" {
  type = string
}

variable "project_number" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

# Iceberg data bucket. Also the BigLake catalog name (single-bucket mode).
variable "iceberg_bucket" {
  type = string
}

variable "raw_bucket" {
  type    = string
  default = "scs-raw"
}

variable "raw_bucket_location" {
  type    = string
  default = "US-EAST1"
}

variable "archive_bucket" {
  type    = string
  default = "scs-raw-archive"
}

variable "archive_bucket_location" {
  type    = string
  default = "US"
}

variable "work_bucket" {
  type    = string
  default = "scs-dataflow"
}

variable "work_bucket_location" {
  type    = string
  default = "US-EAST1"
}

variable "topic" {
  type    = string
  default = "iceberg-poc-events"
}

variable "ar_repo" {
  type    = string
  default = "dataflow-templates"
}

# From Snowflake: SELECT SYSTEM$GET_WORKLOAD_IDENTITY_ISSUER_URL();
variable "snowflake_issuer_url" {
  type = string
}

# From Snowflake: DESC CATALOG INTEGRATION ... -> WORKLOAD_IDENTITY_FEDERATION_SUBJECT.
# Only known AFTER the catalog integration exists (two-phase apply); null skips the grants.
variable "snowflake_wif_subject" {
  type    = string
  default = null
}

variable "wif_pool_id" {
  type    = string
  default = "snowflake-pool"
}

variable "wif_provider_id" {
  type    = string
  default = "snowflake-provider"
}
