"""Rewrite Iceberg metadata paths for GCS -> S3 replication (ADR-0002 option C).

Iceberg metadata stores absolute paths, so a byte-for-byte copy of a table to
S3 would still point at gs://. This job calls the rewrite_table_path procedure,
which stages metadata rewritten to the s3:// prefix and emits a copy plan
(file list) — the actual copying happens outside Spark (scripts/10_replicate_to_s3.sh).

Usage (submitted by 10_replicate_to_s3.sh):
  rewrite_table_path.py --catalog <bucket> --project <project_id> \
      --table shared_aws.orders --target_bucket <s3-bucket>
"""
import argparse

from pyspark.sql import SparkSession

ap = argparse.ArgumentParser(allow_abbrev=False)
ap.add_argument("--catalog", required=True)
ap.add_argument("--project", required=True)
ap.add_argument("--table", default="shared_aws.orders")
ap.add_argument("--target_bucket", required=True, help="S3 bucket name (no s3://)")
args = ap.parse_args()
c = args.catalog

spark = (
    SparkSession.builder.appName("iceberg-poc-rewrite-path")
    .config(f"spark.sql.catalog.{c}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{c}.type", "rest")
    .config(f"spark.sql.catalog.{c}.uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog")
    .config(f"spark.sql.catalog.{c}.warehouse", f"gs://{c}")
    .config(f"spark.sql.catalog.{c}.header.x-goog-user-project", args.project)
    .config(f"spark.sql.catalog.{c}.header.X-Iceberg-Access-Delegation", "vended-credentials")
    .config(f"spark.sql.catalog.{c}.rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager")
    .config(f"spark.sql.catalog.{c}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", c)
    .getOrCreate()
)

# Staging MUST live inside the table's own location: vended credentials are
# downscoped to the table prefix, so writes anywhere else in the bucket 403.
loc = next(
    r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {args.table}").collect()
    if r.col_name == "Location"
)
staging = f"{loc}/_replica_staging"
# Backticks: catalog names with hyphens must be quoted in Spark SQL.
row = spark.sql(f"""
  CALL `{c}`.system.rewrite_table_path(
    table => '{args.table}',
    source_prefix => 'gs://{c}',
    target_prefix => 's3://{args.target_bucket}',
    staging_location => '{staging}'
  )""").collect()[0]

# Parsed by 10_replicate_to_s3.sh — keep the format stable.
print(f"REWRITE_RESULT latest_version={row['latest_version']} file_list={row['file_list_location']}")
