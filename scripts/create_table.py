"""Create/append to the POC Iceberg table via the BigLake Iceberg REST catalog.

Usage (submitted by 02_create_table.sh):
  create_table.py --catalog <bucket> --project <project_id> [--append]
Requires Iceberg 1.10+ runtime (Dataproc Serverless 2.3 >= 2.3.10 for GoogleAuthManager).
"""
import argparse

from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--catalog", required=True)
p.add_argument("--project", required=True)
p.add_argument("--append", action="store_true", help="only insert one new row (freshness test)")
args = p.parse_args()
c = args.catalog

spark = (
    SparkSession.builder.appName("iceberg-poc")
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

if args.append:
    spark.sql("""INSERT INTO shared_aws.orders
        SELECT COALESCE(MAX(order_id),0)+1, 'freshness-test', 42.00, current_timestamp()
        FROM shared_aws.orders""")
else:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS shared_aws")
    spark.sql("""CREATE TABLE IF NOT EXISTS shared_aws.orders (
        order_id BIGINT, customer STRING, amount DECIMAL(10,2), order_ts TIMESTAMP
      ) USING iceberg""")
    spark.sql("""INSERT INTO shared_aws.orders VALUES
        (1,'acme',100.50,current_timestamp()),
        (2,'globex',250.00,current_timestamp()),
        (3,'initech',75.25,current_timestamp())""")

spark.sql("SELECT * FROM shared_aws.orders ORDER BY order_id").show()
