"""Register the S3 replica in AWS Glue and query it from Athena.

Proves the multi-engine claim: once correct Iceberg metadata is in S3 with a
Glue catalog over it, ANY AWS-native engine reads it — Athena, Redshift
Spectrum, EMR, Spark. Snowflake is one consumer among several, not the point.

This is the capability the zero-copy path CANNOT offer: Athena and Redshift
are structurally S3-bound and cannot read gs:// at all (ADR-0002 names
"S3-only consumers" as a first-class reason to replicate).

Usage:
  python 12_glue_athena_register.py --bucket <s3-bucket> \
      --metadata_path shared_aws/orders/metadata/<version>.metadata.json
"""
import argparse
import time

import boto3

REGION = "us-east-2"
DB = "iceberg_poc_replica"
TABLE = "orders"

# Matches shared_aws.orders; Athena reads the authoritative schema from the
# Iceberg metadata, but Glue wants a storage descriptor.
COLUMNS = [
    {"Name": "order_id", "Type": "bigint"},
    {"Name": "customer", "Type": "string"},
    {"Name": "amount", "Type": "decimal(10,2)"},
    {"Name": "order_ts", "Type": "timestamp"},
]


def register(bucket: str, metadata_path: str) -> None:
    glue = boto3.client("glue", region_name=REGION)
    try:
        glue.create_database(DatabaseInput={"Name": DB})
        print(f"created glue database {DB}")
    except glue.exceptions.AlreadyExistsException:
        print(f"glue database {DB} exists")

    table_input = {
        "Name": TABLE,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "table_type": "ICEBERG",
            "metadata_location": f"s3://{bucket}/{metadata_path}",
        },
        "StorageDescriptor": {
            "Columns": COLUMNS,
            "Location": f"s3://{bucket}/shared_aws/orders",
        },
    }
    try:
        glue.create_table(DatabaseName=DB, TableInput=table_input)
        print(f"registered glue table {DB}.{TABLE}")
    except glue.exceptions.AlreadyExistsException:
        glue.update_table(DatabaseName=DB, TableInput=table_input)
        print(f"updated glue table {DB}.{TABLE}")


def athena_query(bucket: str, sql: str) -> list:
    athena = boto3.client("athena", region_name=REGION)
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": f"s3://{bucket}/athena-results/"},
    )
    qid = q["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if st["State"] != "SUCCEEDED":
        raise RuntimeError(st.get("StateChangeReason", st["State"]))
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows]


def main() -> None:
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--metadata_path", required=True)
    args = ap.parse_args()

    register(args.bucket, args.metadata_path)
    print("\n-- Athena reading the replica (no Snowflake involved):")
    for row in athena_query(args.bucket, f"SELECT * FROM {TABLE} ORDER BY order_id"):
        print("  " + " | ".join(row))


if __name__ == "__main__":
    main()
