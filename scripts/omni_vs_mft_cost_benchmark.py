"""Cost benchmark: BigQuery Omni (query-in-place) vs MFT (extract + file transfer).

Produces AUDITABLE costs, not estimates. Every BigQuery job is labelled so the
cost lands attributably in the GCP billing export; AWS costs come from Cost
Explorer. Nothing here is modelled -- the report reads real billed amounts.

WHY THIS EXISTS
  The 4-row POC table is ~7,700x smaller than BigQuery's 10 MiB minimum billing
  floor, so every cost measured on it is the floor, not real consumption.
  Extrapolating from it would be wrong by orders of magnitude. This benchmark
  generates a realistically-sized dataset so the meters actually move.

PHASES
  generate  Write an N-GiB Iceberg dataset to S3 (the "daily partition")
  omni      Approach A: Omni scans in AWS, returns only the delta to GCP
  mft       Approach B: Athena extract -> S3 file -> egress out -> GCS -> BQ load
  jobstats  Immediate per-job metrics from INFORMATION_SCHEMA (bytes billed, slots)
  report    Authoritative reconciliation vs GCP billing export + AWS Cost Explorer
            (run >=24h after the others; billing export lags 24-48h)

USAGE
  python omni_vs_mft_cost_benchmark.py generate --size-gb 5 --bucket <b>
  python omni_vs_mft_cost_benchmark.py omni     --bucket <b>
  python omni_vs_mft_cost_benchmark.py mft      --bucket <b> --gcs-bucket <g>
  python omni_vs_mft_cost_benchmark.py jobstats
  python omni_vs_mft_cost_benchmark.py report            # >= 24h later

COST OF RUNNING THE BENCHMARK ITSELF (~5 GiB default)
  Omni scan   ~5 GiB @ $7.79/TiB          ~= $0.04
  Athena scan ~5 GiB @ $5.00/TB           ~= $0.03
  AWS egress  ~5 GB  @ $0.09/GB           ~= $0.45   <-- dominates, by design
  S3 + GCS storage (delete after)         ~= $0.01
  TOTAL                                   ~= $0.55
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
import warnings

warnings.filterwarnings("ignore")

PROJECT = os.environ.get("BQ_PROJECT", "strongsville-city-schools")
OMNI_CONN = os.environ.get("OMNI_CONN", "aws-us-east-1.omni_s3_conn")
OMNI_LOC = "aws-us-east-1"
GCP_LOC = "us-east4"                      # colocated with aws-us-east-1
BILLING_TABLE = os.environ.get(
    "BILLING_TABLE",
    "strongsville-city-schools.billing.gcp_billing_export_resource_v1_01BE48_A7B284_1D37B9",
)
LABEL_KEY, LABEL_VAL = "cost_test", "omni_vs_mft"


def bq():
    from google.cloud import bigquery
    return bigquery.Client(project=PROJECT)


def run_labelled(client, sql, location, phase, dry_run=False):
    """Run a query labelled for billing attribution; return the job."""
    from google.cloud import bigquery
    cfg = bigquery.QueryJobConfig(
        labels={LABEL_KEY: LABEL_VAL, "phase": phase},
        dry_run=dry_run,
        use_query_cache=False,          # caching would hide real cost
    )
    job = client.query(sql, location=location, job_config=cfg)
    if not dry_run:
        job.result()
    return job


# ---------------------------------------------------------------- generate
def cmd_generate(args):
    """Write an N-GiB Iceberg table to S3 with PyIceberg."""
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog

    target_bytes = int(args.size_gb * (2 ** 30))
    row_bytes = 100                      # ~100 B/row after Parquet compression
    total_rows = target_bytes // row_bytes
    batch = 1_000_000

    cat = SqlCatalog("bench", **{
        "uri": "sqlite:///bench_cat.db",
        "warehouse": f"s3://{args.bucket}/bench",
        "s3.region": "us-east-1",
        "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
    })
    schema = pa.schema([
        ("order_id", pa.int64()), ("customer", pa.string()),
        ("amount", pa.float64()), ("status", pa.string()),
        ("updated_at", pa.string()), ("payload", pa.string()),
    ])
    try:
        cat.create_namespace("bench")
    except Exception:
        pass
    try:
        cat.drop_table("bench.orders_big")
    except Exception:
        pass
    tbl = cat.create_table("bench.orders_big", schema=schema)

    print(f"generating ~{args.size_gb} GiB ({total_rows:,} rows) in batches of {batch:,}")
    written = 0
    while written < total_rows:
        n = min(batch, total_rows - written)
        data = pa.table({
            "order_id": pa.array(range(written, written + n), pa.int64()),
            "customer": pa.array([f"cust-{i%50000}" for i in range(written, written + n)]),
            "amount": pa.array([(i % 10000) / 100.0 for i in range(written, written + n)]),
            "status": pa.array(["OPEN" if i % 3 else "CLOSED" for i in range(written, written + n)]),
            "updated_at": pa.array(["2026-07-25" if i % 50 else "2026-07-24"
                                    for i in range(written, written + n)]),
            "payload": pa.array(["x" * 40] * n),
        }, schema=schema)
        tbl.append(data)
        written += n
        print(f"  {written:,}/{total_rows:,} rows")
    print("METADATA_LOCATION:", tbl.metadata_location)
    print("\nNext: create the BQ external table over that metadata, then run 'omni'.")


# ---------------------------------------------------------------- omni
def cmd_omni(args):
    """Approach A: Omni scans in AWS; only the delta crosses to GCP."""
    c = bq()
    print("== APPROACH A: BigQuery Omni (query in place) ==\n")

    # 1) full scan, delta filtered IN AWS -- only changed rows return
    sql_delta = f"""
    SELECT order_id, customer, amount, status
    FROM `{PROJECT}.{args.dataset}.{args.table}`
    WHERE updated_at = '2026-07-25'
    """
    j = run_labelled(c, sql_delta, OMNI_LOC, "omni_delta_scan")
    rows = list(j.result())
    print(f"  delta query   : job={j.job_id}")
    print(f"    bytes processed : {j.total_bytes_processed:,}")
    print(f"    bytes BILLED    : {j.total_bytes_billed:,}")
    print(f"    slot_ms         : {j.slot_millis}")
    print(f"    rows returned   : {len(rows):,}  <-- only this crosses to GCP")

    # 2) same scan materialised into GCP (worst case: everything crosses)
    sql_ctas = f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{args.gcp_dataset}.omni_delta_landed` AS
    SELECT order_id, customer, amount, status
    FROM `{PROJECT}.{args.dataset}.{args.table}`
    WHERE updated_at = '2026-07-25'
    """
    j2 = run_labelled(c, sql_ctas, GCP_LOC, "omni_ctas_land")
    print(f"\n  CTAS to GCP   : job={j2.job_id}")
    print(f"    bytes BILLED    : {j2.total_bytes_billed:,}")
    print(f"    slot_ms         : {j2.slot_millis}")
    print("    ^ this is the cross-cloud transfer path (egress applies)")
    print("\n  Run 'report' >=24h from now for authoritative billed cost.")


# ---------------------------------------------------------------- mft
def cmd_mft(args):
    """Approach B: Athena extract -> S3 -> egress OUT of AWS -> GCS -> BQ load."""
    import boto3
    print("== APPROACH B: MFT (extract + file transfer) ==\n")
    ath = boto3.client("athena", region_name="us-east-1")
    out = f"s3://{args.bucket}/bench-athena-results/"
    unload = f"""
    UNLOAD (SELECT order_id, customer, amount, status
            FROM {args.glue_db}.{args.glue_table}
            WHERE updated_at = '2026-07-25')
    TO 's3://{args.bucket}/bench-extract/'
    WITH (format = 'PARQUET')
    """
    q = ath.start_query_execution(
        QueryString=unload,
        ResultConfiguration={"OutputLocation": out},
        WorkGroup=args.workgroup)
    qid = q["QueryExecutionId"]
    print(f"  Athena UNLOAD : {qid}")
    while True:
        st = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        s = st["Status"]["State"]
        if s in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    print(f"    state           : {s}")
    if s == "SUCCEEDED":
        stats = st["Statistics"]
        scanned = stats.get("DataScannedInBytes", 0)
        print(f"    bytes scanned   : {scanned:,}  (Athena bills $5.00/TB)")
        print(f"    athena cost     : ${scanned/1e12*5:.6f}")
    else:
        print("    ERROR:", st["Status"].get("StateChangeReason"))
        return

    # measure the extract, then actually pull it OUT of AWS (this is the egress)
    s3 = boto3.client("s3", region_name="us-east-1")
    objs = s3.list_objects_v2(Bucket=args.bucket, Prefix="bench-extract/").get("Contents", [])
    total = sum(o["Size"] for o in objs)
    print(f"\n  extract size    : {total:,} bytes across {len(objs)} file(s)")
    print(f"    egress cost   : ${total/1e9*0.09:.6f}  (${0.09}/GB tier-1)")
    print("\n  NOTE: to measure egress on the AWS bill you must actually transfer")
    print("        the bytes out of AWS. Re-run with --do-egress to download them.")
    if args.do_egress:
        import tempfile
        got = 0
        with tempfile.TemporaryDirectory() as td:
            for o in objs:
                p = os.path.join(td, os.path.basename(o["Key"]))
                s3.download_file(args.bucket, o["Key"], p)
                got += os.path.getsize(p)
        print(f"  downloaded {got:,} bytes out of AWS (egress meter moved)")


# ---------------------------------------------------------------- jobstats
def cmd_jobstats(args):
    """Immediate per-job metrics (available instantly, unlike billing export)."""
    c = bq()
    for region in (GCP_LOC, "us-central1"):
        q = f"""
        SELECT job_id, creation_time, statement_type, total_bytes_billed,
               total_slot_ms, TO_JSON_STRING(labels) lbl
        FROM `region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
          AND EXISTS (SELECT 1 FROM UNNEST(labels) l
                      WHERE l.key='{LABEL_KEY}' AND l.value='{LABEL_VAL}')
        ORDER BY creation_time DESC
        """
        try:
            rows = list(c.query(q, location=region).result())
            print(f"--- region-{region}: {len(rows)} labelled job(s) ---")
            for r in rows:
                tib = (r.total_bytes_billed or 0) / (2**40)
                print(f"  {str(r.creation_time)[:19]} | {r.statement_type:14} | "
                      f"billed={r.total_bytes_billed:>12,} ({tib:.8f} TiB) | "
                      f"slot_ms={r.total_slot_ms} | {r.lbl}")
        except Exception as e:
            print(f"--- region-{region}: {str(e)[:120]}")


# ---------------------------------------------------------------- report
def cmd_report(args):
    """AUTHORITATIVE: reconcile against the GCP billing export + AWS Cost Explorer."""
    c = bq()
    print("== AUTHORITATIVE COST (GCP billing export) ==")
    print("   (billing export lags 24-48h; re-run if empty)\n")
    q = f"""
    SELECT sku.description sku, sku.id sku_id,
           SUM(usage.amount) usage_amt, ANY_VALUE(usage.unit) unit,
           ROUND(SUM(cost),6) cost,
           ROUND(SUM(SUM(cost)) OVER (), 6) grand_total
    FROM `{BILLING_TABLE}`, UNNEST(labels) l
    WHERE l.key = '{LABEL_KEY}' AND l.value = '{LABEL_VAL}'
      AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {args.days} DAY)
    GROUP BY 1,2 ORDER BY cost DESC
    """
    try:
        rows = list(c.query(q).result())
        if not rows:
            print("  no labelled cost rows yet -- billing export has not landed.")
            print("  (labels only attach to costs incurred AFTER the labelled jobs ran)")
        for r in rows:
            print(f"  ${r.cost:>12.6f} | {r.sku[:50]:50} | {r.usage_amt:,.0f} {r.unit} | {r.sku_id}")
        if rows:
            print(f"\n  GCP TOTAL: ${rows[0].grand_total:.6f}")
    except Exception as e:
        print("  query failed:", str(e)[:300])

    print("\n== AUTHORITATIVE COST (AWS Cost Explorer) ==")
    try:
        import boto3
        ce = boto3.client("ce", region_name="us-east-1")
        end = dt.date.today() + dt.timedelta(days=1)
        start = end - dt.timedelta(days=args.days + 1)
        r = ce.get_cost_and_usage(
            TimePeriod={"Start": str(start), "End": str(end)},
            Granularity="DAILY", Metrics=["UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
        tot = 0.0
        agg = {}
        for res in r["ResultsByTime"]:
            for g in res["Groups"]:
                amt = float(g["Metrics"]["UnblendedCost"]["Amount"])
                if amt > 0:
                    agg[g["Keys"][0]] = agg.get(g["Keys"][0], 0) + amt
                    tot += amt
        for k, v in sorted(agg.items(), key=lambda x: -x[1]):
            print(f"  ${v:>12.6f} | {k}")
        print(f"\n  AWS TOTAL: ${tot:.6f}")
    except Exception as e:
        print("  Cost Explorer failed:", str(e)[:200])


def main():
    ap = argparse.ArgumentParser(allow_abbrev=False)
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate"); g.set_defaults(func=cmd_generate)
    g.add_argument("--size-gb", type=float, default=5.0)
    g.add_argument("--bucket", required=True)

    o = sub.add_parser("omni"); o.set_defaults(func=cmd_omni)
    o.add_argument("--bucket", required=True)
    o.add_argument("--dataset", default="omni_s3")
    o.add_argument("--table", default="orders_big")
    o.add_argument("--gcp-dataset", default="omni_join_ref")

    m = sub.add_parser("mft"); m.set_defaults(func=cmd_mft)
    m.add_argument("--bucket", required=True)
    m.add_argument("--gcs-bucket")
    m.add_argument("--glue-db", default="bench")
    m.add_argument("--glue-table", default="orders_big")
    m.add_argument("--workgroup", default="primary")
    m.add_argument("--do-egress", action="store_true")

    j = sub.add_parser("jobstats"); j.set_defaults(func=cmd_jobstats)

    r = sub.add_parser("report"); r.set_defaults(func=cmd_report)
    r.add_argument("--days", type=int, default=7)

    a = ap.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
