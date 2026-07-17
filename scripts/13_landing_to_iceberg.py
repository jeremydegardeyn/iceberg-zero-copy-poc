"""AWS-side half of the landing-zone pattern: raw S3 files -> Iceberg + Glue.

Dataflow's job in this pattern is ONLY to drop bytes into the landing zone —
it never touches a catalog, which is exactly why it works (Beam's S3
filesystem takes static keys; an Iceberg Glue client does not; ADR-0007).

Everything catalog-shaped happens here, on AWS, under an ordinary IAM
identity — no cross-cloud credentials involved:

  1. an external Glue table over the landing prefix (raw JSON), so Athena can
     read what Dataflow dropped
  2. Athena CTAS -> a real Iceberg table registered in Glue

That is the "Landing Zone -> Raw (Iceberg)" arrow in the Huntington
future-state diagram; they run it as a Glue Spark job, we use Athena CTAS
because it needs no cluster.

Usage:
  python 13_landing_to_iceberg.py --bucket <s3-bucket> [--landing_prefix landing/stream]
"""
import argparse
import time

import boto3

REGION = "us-east-2"
DB = "iceberg_poc_replica"
LANDING_TABLE = "landing_stream_raw"
ICEBERG_TABLE = "stream_events"


def athena(sql: str, bucket: str) -> list:
    a = boto3.client("athena", region_name=REGION)
    qid = a.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": f"s3://{bucket}/athena-results/"},
    )["QueryExecutionId"]
    while True:
        st = a.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if st["State"] != "SUCCEEDED":
        raise RuntimeError(st.get("StateChangeReason", st["State"])[:300])
    rows = a.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows]


def main() -> None:
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--landing_prefix", default="landing/stream")
    args = ap.parse_args()

    glue = boto3.client("glue", region_name=REGION)
    try:
        glue.create_database(DatabaseInput={"Name": DB})
    except glue.exceptions.AlreadyExistsException:
        pass

    # 1. Raw view of whatever Dataflow dropped. No partitions -> new files are
    #    picked up on the next query with no partition maintenance.
    print("1. external table over the landing zone...")
    athena(f"DROP TABLE IF EXISTS {LANDING_TABLE}", args.bucket)
    athena(
        f"""CREATE EXTERNAL TABLE {LANDING_TABLE} (
              event_id bigint, source string, amount double, published_at string)
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            LOCATION 's3://{args.bucket}/{args.landing_prefix}/'""",
        args.bucket,
    )
    n = athena(f"SELECT COUNT(*) FROM {LANDING_TABLE}", args.bucket)[1][0]
    print(f"   landing zone holds {n} rows")

    # 2. Promote to a real Iceberg table, registered in Glue by Athena itself.
    print("2. CTAS -> Iceberg...")
    athena(f"DROP TABLE IF EXISTS {ICEBERG_TABLE}", args.bucket)
    athena(
        f"""CREATE TABLE {ICEBERG_TABLE}
            WITH (table_type='ICEBERG',
                  location='s3://{args.bucket}/iceberg/{ICEBERG_TABLE}/',
                  format='PARQUET',
                  is_external=false)
            AS SELECT * FROM {LANDING_TABLE}""",
        args.bucket,
    )

    print("3. reading the Iceberg table back:")
    for row in athena(
        f"SELECT source, COUNT(*) AS n, ROUND(SUM(amount),2) AS total "
        f"FROM {ICEBERG_TABLE} GROUP BY source ORDER BY source",
        args.bucket,
    ):
        print("   " + " | ".join(row))
    print(f"\nGlue now has an Iceberg table {DB}.{ICEBERG_TABLE} — readable by "
          f"Athena, Redshift Spectrum, EMR, and Snowflake.")


if __name__ == "__main__":
    main()
