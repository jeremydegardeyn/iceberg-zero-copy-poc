"""Proven working: Dataproc Spark writes Iceberg directly to S3 + Glue.

No landing zone, no promotion job — Spark's Iceberg-AWS integration authenticates
fine with plain static keys, PROVIDED they're supplied as DefaultCredentialsProvider
system properties, not as a StaticCredentialsProvider class reference (which
Iceberg's reflective instantiation rejects — see ADR-0007). Submit with
scripts/15_run_spark_direct_s3.sh, which sets the required extraJavaOptions and
assumes a Cloud NAT gateway exists on the job's VPC (Dataproc Serverless has no
internet egress otherwise; AWS is public internet, unlike Google's own APIs).

Usage: --catalog <s3-bucket> --namespace <ns> --table <name>
"""
import argparse

from pyspark.sql import SparkSession

ap = argparse.ArgumentParser(allow_abbrev=False)
ap.add_argument("--catalog", required=True, help="S3 bucket name (no s3://)")
ap.add_argument("--namespace", default="direct_test")
ap.add_argument("--table", default="orders")
ap.add_argument("--region", default="us-east-2")
args = ap.parse_args()

CAT = "glue_direct"
spark = (
    SparkSession.builder.appName("spark-direct-s3-glue")
    .config(f"spark.sql.catalog.{CAT}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CAT}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CAT}.warehouse", f"s3://{args.catalog}/iceberg")
    .config(f"spark.sql.catalog.{CAT}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{CAT}.client.region", args.region)
    # No client.credentials-provider set: defaults to DefaultCredentialsProvider,
    # which reads the -Daws.accessKeyId/-Daws.secretAccessKey system properties
    # set on this job's launch (see 15_run_spark_direct_s3.sh).
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.{args.namespace}")
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CAT}.{args.namespace}.{args.table} (
    order_id BIGINT, customer STRING, amount DECIMAL(10,2), order_ts TIMESTAMP
  ) USING iceberg
""")
spark.sql(f"""
  INSERT INTO {CAT}.{args.namespace}.{args.table} VALUES
  (1,'acme',100.50,current_timestamp()),
  (2,'globex',250.00,current_timestamp())
""")
spark.sql(f"SELECT * FROM {CAT}.{args.namespace}.{args.table}").show()
print(f"table {CAT}.{args.namespace}.{args.table} written directly to S3+Glue, no landing zone")
