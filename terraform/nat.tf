# Cloud NAT: Dataproc Serverless has no internet egress by default (unlike
# Dataflow, which can take a public IP). Google's own APIs (BigLake, GCS) are
# reachable via Private Google Access without this: NAT is only needed for the
# Spark-direct-to-S3+Glue path (ADR-0007), which calls the public AWS endpoints.
# Standing hourly cost while it exists — not required for the rest of the POC.
resource "google_compute_router" "dataproc_nat_router" {
  count   = var.enable_dataproc_nat ? 1 : 0
  name    = "dataproc-nat-router"
  network = "default"
  region  = var.region
}

resource "google_compute_router_nat" "dataproc_nat" {
  count                              = var.enable_dataproc_nat ? 1 : 0
  name                               = "dataproc-nat"
  router                             = google_compute_router.dataproc_nat_router[0].name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}
