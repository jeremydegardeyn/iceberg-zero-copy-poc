"""AWS Glue (Spark) job: S3 landing zone -> Iceberg table registered in Glue.

This is the "Landing Zone -> Raw (Iceberg)" arrow of the Huntington
future-state architecture, run the way they run it: a Glue Spark job.

NOTE WHAT IS ABSENT: there are no credentials in this file. The job assumes an
IAM role, so GlueCatalog and S3FileIO authenticate natively. Compare
dataflow/batch/main.py --catalog_type glue, which fails precisely because GCP
compute cannot hand static keys to an Iceberg Glue client (ADR-0007). Running
the writer where the storage lives makes the whole problem disappear.

Job parameters (--key value):
  --landing_path  s3://bucket/landing/stream/
  --warehouse     s3://bucket/iceberg/
  --catalog_db    glue database (created if absent)
  --table_name    destination Iceberg table
  --write_mode    append (default) | overwrite
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "landing_path", "warehouse", "catalog_db", "table_name", "write_mode"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

fqn = f"glue_catalog.{args['catalog_db']}.{args['table_name']}"

# Whatever Dataflow dropped. Schema inferred from the JSON — in production this
# is where the Standardized zone's explicit schema contract would apply.
df = spark.read.json(args["landing_path"])
landed = df.count()
print(f"landing zone rows: {landed}")

if landed == 0:
    print("nothing to convert; exiting cleanly")
    job.commit()
    sys.exit(0)

spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{args['catalog_db']}")

exists = spark.catalog.tableExists(fqn)
if exists and args["write_mode"] == "append":
    df.writeTo(fqn).append()
    print(f"appended {landed} rows to {fqn}")
else:
    df.writeTo(fqn).using("iceberg").createOrReplace()
    print(f"created/replaced {fqn} with {landed} rows")

total = spark.table(fqn).count()
print(f"{fqn} now holds {total} rows — registered in Glue, readable by "
      f"Athena / Redshift Spectrum / EMR / Snowflake")

job.commit()
