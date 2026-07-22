output "bigquery_google_identity" {
  description = "The connection's federated identity. Terraform already grants it on the AWS role trust; shown for verification."
  value       = google_bigquery_connection.omni.aws[0].access_role[0].identity
}

output "omni_role_arn" {
  value = local.role_arn
}

output "connection_id" {
  value = google_bigquery_connection.omni.id
}

output "bq_location" {
  value = local.bq_location
}
