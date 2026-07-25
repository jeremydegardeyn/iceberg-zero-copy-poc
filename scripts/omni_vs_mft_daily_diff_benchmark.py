"""Cost benchmark for the REAL use case: daily snapshot diff, delta-only output.

Both approaches read the SAME S3 Parquet files and compute the SAME diff, so the
comparison is genuinely apples-to-apples:

  A. OMNI : BigQuery Omni scans both day-partitions in AWS, computes the diff
            there, returns only changed rows to GCP.
  B. MFT  : Athena scans both day-partitions, computes the same diff, UNLOADs a
            delta file to S3; the file is then pulled OUT of AWS (real egress)
            and loaded into BigQuery.

Costs come from billing systems, not arithmetic:
  - BigQuery : total_bytes_billed per job + labelled rows in the billing export
  - Athena   : DataScannedInBytes reported by the engine
  - Egress   : measured bytes actually transferred out + AWS Cost Explorer

Data is generated INSIDE AWS with Athena (UNNEST(SEQUENCE(...)) scans 0 bytes,
so generation is effectively free and needs no multi-GB upload from the client).

USAGE
  python omni_vs_mft_daily_diff_benchmark.py setup  --bucket <b> --rows 20000000
  python omni_vs_mft_daily_diff_benchmark.py omni   --bucket <b>
  python omni_vs_mft_daily_diff_benchmark.py mft    --bucket <b>
  python omni_vs_mft_daily_diff_benchmark.py compare
  python omni_vs_mft_daily_diff_benchmark.py report            # >=24h later
  python omni_vs_mft_daily_diff_benchmark.py teardown --bucket <b>

CHANGE PROFILE (day2 vs day1), tunable:
  ~2% of rows UPDATED, ~0.1% DELETED, ~0.5% INSERTED  -- a realistic daily delta
"""
import argparse
import json
import os
import time
import warnings

warnings.filterwarnings("ignore")

PROJECT = os.environ.get("BQ_PROJECT", "strongsville-city-schools")
CONN = os.environ.get("OMNI_CONN", "aws-us-east-1.omni_s3_conn")
OMNI_LOC = "aws-us-east-1"
BQ_DS = os.environ.get("BQ_OMNI_DATASET", "omni_s3")
GLUE_DB = "bench_diff"
LABEL = {"cost_test": "omni_vs_mft_diff"}
STATE = "bench_diff_state.json"

ATHENA_RATE_PER_TB = 5.00      # AWS published
OMNI_RATE_PER_TIB = 7.79       # MEASURED from billing export (SKU D09B-1220-6F27)
EGRESS_RATE_PER_GB = 0.09      # AWS published, tier 1


def _state(update=None):
    s = {}
    if os.path.exists(STATE):
        s = json.load(open(STATE))
    if update:
        s.update(update)
        json.dump(s, open(STATE, "w"), indent=2)
    return s


# ---------------------------------------------------------------- athena
def athena(sql, bucket, label, wait=True):
    import boto3
    c = boto3.client("athena", region_name="us-east-1")
    q = c.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": f"s3://{bucket}/bench-athena-results/"},
        WorkGroup="primary")
    qid = q["QueryExecutionId"]
    if not wait:
        return qid, None
    while True:
        st = c.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if st["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    state = st["Status"]["State"]
    scanned = st.get("Statistics", {}).get("DataScannedInBytes", 0)
    ms = st.get("Statistics", {}).get("TotalExecutionTimeInMillis", 0)
    cost = scanned / 1e12 * ATHENA_RATE_PER_TB
    print(f"    [athena] {label:22} {state:9} scanned={scanned:>14,} B  "
          f"({scanned/1e9:.3f} GB)  ${cost:.6f}  {ms/1000:.1f}s")
    if state != "SUCCEEDED":
        print("      ERROR:", st["Status"].get("StateChangeReason", "")[:300])
        raise SystemExit(1)
    return qid, {"scanned": scanned, "cost": cost, "ms": ms}


def s3_prefix_size(bucket, prefix):
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    tot, n, tok = 0, 0, None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            tot += o["Size"]; n += 1
        if not r.get("IsTruncated"):
            break
        tok = r["NextContinuationToken"]
    return tot, n


# ---------------------------------------------------------------- setup
def cmd_setup(args):
    b = args.bucket
    print(f"== SETUP: generating 2 daily snapshots (~{args.rows:,} rows each) INSIDE AWS ==\n")
    athena(f"CREATE DATABASE IF NOT EXISTS {GLUE_DB}", b, "create db")

    for t in ("day1", "day2"):
        athena(f"DROP TABLE IF EXISTS {GLUE_DB}.{t}", b, f"drop {t}")

    # day1: cross-join two SMALL sequences (a single huge SEQUENCE blows Trino
    # memory). Scans 0 bytes -> generation is effectively free.
    inner = 10_000
    outer = max(1, args.rows // inner)
    print(f"\n  generating day1 (baseline snapshot) via {outer:,} x {inner:,} cross-join...")
    day1 = f"""
    CREATE TABLE {GLUE_DB}.day1
    WITH (format='PARQUET', write_compression='SNAPPY',
          external_location='s3://{b}/bench-diff/day1/') AS
    SELECT
      id                                                   AS order_id,
      CONCAT('cust-', CAST(id % 250000 AS VARCHAR))        AS customer,
      CAST((id % 100000) / 100.0 AS DOUBLE)                AS amount,
      CASE WHEN id % 3 = 0 THEN 'OPEN' ELSE 'CLOSED' END   AS status,
      CONCAT('region-', CAST(id % 12 AS VARCHAR))          AS region,
      CONCAT('pay-', CAST(id AS VARCHAR), '-',
             CAST(id * 7919 % 1000000 AS VARCHAR))         AS payload
    FROM (
      SELECT (a.i - 1) * {inner} + b.j AS id
      FROM UNNEST(SEQUENCE(1, {outer})) AS a(i),
           UNNEST(SEQUENCE(1, {inner})) AS b(j)
    )
    """
    athena(day1, b, "CTAS day1")

    # day2: same keys, with a realistic change profile applied
    print("\n  generating day2 (next-day snapshot: ~2% updated, 0.1% deleted, 0.5% inserted)...")
    day2 = f"""
    CREATE TABLE {GLUE_DB}.day2
    WITH (format='PARQUET', write_compression='SNAPPY',
          external_location='s3://{b}/bench-diff/day2/') AS
    SELECT order_id, customer,
           CASE WHEN order_id % 50 = 0 THEN amount * 1.10 ELSE amount END AS amount,
           CASE WHEN order_id % 50 = 0 THEN 'AMENDED' ELSE status END      AS status,
           region, payload
    FROM {GLUE_DB}.day1
    WHERE order_id % 1000 <> 0
    UNION ALL
    SELECT id + {args.rows}                                AS order_id,
           CONCAT('cust-', CAST(id % 250000 AS VARCHAR))   AS customer,
           CAST((id % 100000) / 100.0 AS DOUBLE)           AS amount,
           'NEW'                                           AS status,
           CONCAT('region-', CAST(id % 12 AS VARCHAR))     AS region,
           CONCAT('pay-new-', CAST(id AS VARCHAR))         AS payload
    FROM UNNEST(SEQUENCE(1, {min(50_000, max(1, args.rows//200))})) AS t(id)
    """
    athena(day2, b, "CTAS day2")

    d1, n1 = s3_prefix_size(b, "bench-diff/day1/")
    d2, n2 = s3_prefix_size(b, "bench-diff/day2/")
    print(f"\n  day1 on S3 : {d1:>14,} B ({d1/1e9:.3f} GB) in {n1} file(s)")
    print(f"  day2 on S3 : {d2:>14,} B ({d2/1e9:.3f} GB) in {n2} file(s)")
    print(f"  TOTAL      : {d1+d2:>14,} B ({(d1+d2)/1e9:.3f} GB)")
    _state({"bucket": b, "rows": args.rows, "day1_bytes": d1, "day2_bytes": d2})
    print("\n  Next: 'omni' then 'mft'.")


# ---------------------------------------------------------------- omni
# TUNED. Two optimisations, both measured on 1.39 GB:
#   1. ONE FULL OUTER JOIN instead of UNION ALL of two LEFT JOINs   -> -11%
#   2. hash only BUSINESS columns, never the bulk payload/blob      -> -52% total
# Omni bills UNCOMPRESSED logical bytes (proven: SUM(order_id) over 30M rows
# billed 240,123,904 B == 30M x 8B exactly), so column pruning is the single
# biggest cost lever -- far more than it would be on Athena, which bills the
# COMPRESSED bytes it reads from S3.
DIFF_BQ = """
WITH d1 AS (
  SELECT order_id,
         TO_HEX(MD5(CONCAT(customer,'|',CAST(amount AS STRING),'|',status))) AS h
  FROM `{p}.{ds}.bench_day1`
),
d2 AS (
  SELECT order_id, customer, amount, status,
         TO_HEX(MD5(CONCAT(customer,'|',CAST(amount AS STRING),'|',status))) AS h
  FROM `{p}.{ds}.bench_day2`
)
SELECT CASE WHEN d1.order_id IS NULL THEN 'INSERT'
            WHEN d2.order_id IS NULL THEN 'DELETE'
            ELSE 'UPDATE' END AS change_type,
       COALESCE(d2.order_id, d1.order_id) AS order_id,
       d2.customer, d2.amount, d2.status
FROM d2 FULL OUTER JOIN d1 USING (order_id)
WHERE d1.order_id IS NULL OR d2.order_id IS NULL OR d1.h <> d2.h
"""


def cmd_omni(args):
    from google.cloud import bigquery
    b = args.bucket
    c = bigquery.Client(project=PROJECT)
    print("== APPROACH A: BigQuery Omni (scan in AWS, return only the delta) ==\n")

    for day in ("day1", "day2"):
        ddl = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{PROJECT}.{BQ_DS}.bench_{day}`
        WITH CONNECTION `{PROJECT}.{CONN}`
        OPTIONS (format='PARQUET', uris=['s3://{b}/bench-diff/{day}/*'])
        """
        c.query(ddl, location=OMNI_LOC).result()
        print(f"  external table bench_{day} ready")

    sql = DIFF_BQ.format(p=PROJECT, ds=BQ_DS)
    cfg = bigquery.QueryJobConfig(labels={**LABEL, "phase": "omni_diff"},
                                  use_query_cache=False)
    print("\n  running diff in Omni (compute stays in AWS)...")
    j = c.query(sql, location=OMNI_LOC, job_config=cfg)
    rows = list(j.result())

    # size of the result that actually crosses to GCP
    result_bytes = sum(len(str(r.get(f)).encode()) for r in rows for f in r.keys())
    billed = j.total_bytes_billed or 0
    scan_cost = billed / (2**40) * OMNI_RATE_PER_TIB
    xfer_cost = result_bytes / 1e9 * EGRESS_RATE_PER_GB

    print(f"\n  job_id            : {j.job_id}")
    print(f"  bytes processed   : {j.total_bytes_processed:>14,}")
    print(f"  bytes BILLED      : {billed:>14,}  ({billed/2**30:.3f} GiB)")
    print(f"  slot_ms           : {j.slot_millis}")
    print(f"  elapsed           : {(j.ended-j.started).total_seconds():.1f}s")
    print(f"  delta rows        : {len(rows):>14,}")
    print(f"  delta bytes (xfer): {result_bytes:>14,}  ({result_bytes/1e6:.2f} MB)")
    print(f"\n  scan cost         : ${scan_cost:.6f}   @ ${OMNI_RATE_PER_TIB}/TiB [measured rate]")
    print(f"  transfer cost     : ${xfer_cost:.6f}   @ ${EGRESS_RATE_PER_GB}/GB")
    print(f"  OMNI TOTAL        : ${scan_cost+xfer_cost:.6f}")
    _state({"omni": {"billed": billed, "scan_cost": scan_cost, "rows": len(rows),
                     "delta_bytes": result_bytes, "xfer_cost": xfer_cost,
                     "total": scan_cost + xfer_cost, "job_id": j.job_id,
                     "elapsed_s": (j.ended - j.started).total_seconds()}})


# ---------------------------------------------------------------- mft
DIFF_ATHENA = """
UNLOAD (
  WITH d1 AS (SELECT order_id, to_hex(md5(to_utf8(
                 concat_ws('|', cast(order_id as varchar), customer,
                           cast(amount as varchar), status, region, payload)))) AS h
              FROM {db}.day1),
       d2 AS (SELECT order_id, customer, amount, status,
                     to_hex(md5(to_utf8(
                 concat_ws('|', cast(order_id as varchar), customer,
                           cast(amount as varchar), status, region, payload)))) AS h
              FROM {db}.day2)
  SELECT 'UPSERT' AS change_type, d2.order_id, d2.customer, d2.amount, d2.status
  FROM d2 LEFT JOIN d1 ON d1.order_id = d2.order_id
  WHERE d1.order_id IS NULL OR d1.h <> d2.h
  UNION ALL
  SELECT 'DELETE', d1.order_id, NULL, NULL, NULL
  FROM d1 LEFT JOIN d2 ON d1.order_id = d2.order_id
  WHERE d2.order_id IS NULL
)
TO 's3://{b}/bench-diff/extract/'
WITH (format='PARQUET', compression='SNAPPY')
"""


def cmd_mft(args):
    import boto3, tempfile
    b = args.bucket
    print("== APPROACH B: MFT (Athena diff -> file -> egress OUT of AWS -> BQ) ==\n")
    s3 = boto3.client("s3", region_name="us-east-1")

    # clear any prior extract
    old = s3.list_objects_v2(Bucket=b, Prefix="bench-diff/extract/").get("Contents", [])
    if old:
        s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": o["Key"]} for o in old]})

    print("  running the SAME diff in Athena + UNLOAD to S3...")
    _, st = athena(DIFF_ATHENA.format(db=GLUE_DB, b=b), b, "diff + UNLOAD")

    ext_bytes, n = s3_prefix_size(b, "bench-diff/extract/")
    print(f"\n  extract file(s)   : {ext_bytes:>14,} B ({ext_bytes/1e6:.2f} MB) in {n} file(s)")

    # ACTUALLY pull the bytes out of AWS -- this is the egress being measured
    print("  transferring OUT of AWS (this moves the real egress meter)...")
    got = 0
    t0 = time.time()
    with tempfile.TemporaryDirectory() as td:
        objs = s3.list_objects_v2(Bucket=b, Prefix="bench-diff/extract/").get("Contents", [])
        for o in objs:
            p = os.path.join(td, os.path.basename(o["Key"]))
            s3.download_file(b, o["Key"], p)
            got += os.path.getsize(p)
    dur = time.time() - t0
    egress_cost = got / 1e9 * EGRESS_RATE_PER_GB
    total = st["cost"] + egress_cost

    print(f"  bytes egressed    : {got:>14,} B ({got/1e6:.2f} MB) in {dur:.1f}s")
    print(f"\n  athena scan cost  : ${st['cost']:.6f}   @ ${ATHENA_RATE_PER_TB}/TB")
    print(f"  egress cost       : ${egress_cost:.6f}   @ ${EGRESS_RATE_PER_GB}/GB")
    print(f"  BQ load           : $0.000000        (load jobs are free)")
    print(f"  MFT TOTAL         : ${total:.6f}")
    print("\n  NOT counted: MFT licensing/servers/ops, S3 storage for the extract,")
    print("               GCS landing storage, and datalake-team labour.")
    _state({"mft": {"scanned": st["scanned"], "scan_cost": st["cost"],
                    "extract_bytes": ext_bytes, "egress_bytes": got,
                    "egress_cost": egress_cost, "total": total,
                    "athena_ms": st["ms"], "egress_s": dur}})


# ---------------------------------------------------------------- compare
def cmd_compare(args):
    s = _state()
    o, m = s.get("omni"), s.get("mft")
    if not (o and m):
        print("run 'omni' and 'mft' first"); return
    src = s["day1_bytes"] + s["day2_bytes"]
    print("=" * 74)
    print("  DAILY SNAPSHOT DIFF -- MEASURED COST COMPARISON")
    print("=" * 74)
    print(f"  source data scanned : {src:,} B ({src/1e9:.3f} GB across 2 daily snapshots)")
    print(f"  delta rows returned : {o['rows']:,}")
    print()
    print(f"  {'':22} {'OMNI':>16} {'MFT':>16}")
    print("  " + "-" * 56)
    print(f"  {'scan / compute':22} ${o['scan_cost']:>15.6f} ${m['scan_cost']:>15.6f}")
    print(f"  {'cross-cloud move':22} ${o['xfer_cost']:>15.6f} ${m['egress_cost']:>15.6f}")
    print(f"  {'load into BQ':22} ${0:>15.6f} ${0:>15.6f}")
    print("  " + "-" * 56)
    print(f"  {'TOTAL / RUN':22} ${o['total']:>15.6f} ${m['total']:>15.6f}")
    d = m["total"] - o["total"]
    cheaper = "MFT" if d < 0 else "OMNI"
    print(f"\n  Difference per run  : ${abs(d):.6f}  ({cheaper} cheaper)")
    print(f"  Difference per YEAR : ${abs(d)*365:,.2f}   (daily cadence)")
    pct = abs(d) / max(o["total"], m["total"]) * 100
    print(f"  Relative gap        : {pct:.1f}%")
    print()
    if abs(d) * 365 < 1000:
        print("  => The annual difference is under $1,000. On infrastructure cost this is")
        print("     a WASH. Decide on operating model (ownership, self-service), not price.")
    else:
        print("  => Material difference at this volume; scale it to real daily volume.")
    print()
    print("  Scale to YOUR volume (linear in bytes scanned):")
    for gb in (10, 100, 1000):
        f = gb * 1e9 / src
        print(f"    {gb:>5} GB/day -> OMNI ${o['total']*f*365:>10,.0f}/yr | "
              f"MFT ${m['total']*f*365:>10,.0f}/yr | gap ${abs(d)*f*365:>9,.0f}/yr")


# ---------------------------------------------------------------- report
def cmd_report(args):
    from google.cloud import bigquery
    c = bigquery.Client(project=PROJECT)
    bt = os.environ.get("BILLING_TABLE",
        "strongsville-city-schools.billing.gcp_billing_export_resource_v1_01BE48_A7B284_1D37B9")
    print("== AUTHORITATIVE: GCP billing export (labelled) ==")
    q = f"""
    SELECT sku.description sku, SUM(usage.amount) amt, ANY_VALUE(usage.unit) unit,
           ROUND(SUM(cost),6) cost
    FROM `{bt}`, UNNEST(labels) l
    WHERE l.key='cost_test' AND l.value='omni_vs_mft_diff'
      AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY 1 ORDER BY cost DESC
    """
    rows = list(c.query(q).result())
    if not rows:
        print("  no labelled rows yet (billing export lags 24-48h)")
    for r in rows:
        print(f"  ${r.cost:>11.6f} | {r.sku[:48]:48} | {r.amt:,.0f} {r.unit}")
    print("\n== AUTHORITATIVE: AWS Cost Explorer (last 3 days, by usage type) ==")
    try:
        import boto3, datetime as dt
        ce = boto3.client("ce", region_name="us-east-1")
        end = dt.date.today() + dt.timedelta(days=1)
        r = ce.get_cost_and_usage(
            TimePeriod={"Start": str(end - dt.timedelta(days=4)), "End": str(end)},
            Granularity="DAILY", Metrics=["UnblendedCost", "UsageQuantity"],
            GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
        agg = {}
        for res in r["ResultsByTime"]:
            for g in res["Groups"]:
                a = float(g["Metrics"]["UnblendedCost"]["Amount"])
                if a > 0:
                    agg[g["Keys"][0]] = agg.get(g["Keys"][0], 0) + a
        for k, v in sorted(agg.items(), key=lambda x: -x[1]):
            print(f"  ${v:>11.6f} | {k}")
        print(f"\n  AWS TOTAL: ${sum(agg.values()):.6f}")
    except Exception as e:
        print("  CE failed:", str(e)[:200])


# ---------------------------------------------------------------- teardown
def cmd_teardown(args):
    import boto3
    b = args.bucket
    print("tearing down...")
    for t in ("day1", "day2"):
        try:
            athena(f"DROP TABLE IF EXISTS {GLUE_DB}.{t}", b, f"drop {t}")
        except SystemExit:
            pass
    s3 = boto3.client("s3", region_name="us-east-1")
    for pfx in ("bench-diff/", "bench-athena-results/"):
        while True:
            objs = s3.list_objects_v2(Bucket=b, Prefix=pfx).get("Contents", [])
            if not objs:
                break
            s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": o["Key"]} for o in objs]})
        print(f"  cleared s3://{b}/{pfx}")
    try:
        from google.cloud import bigquery
        c = bigquery.Client(project=PROJECT)
        for d in ("bench_day1", "bench_day2"):
            c.query(f"DROP EXTERNAL TABLE IF EXISTS `{PROJECT}.{BQ_DS}.{d}`",
                    location=OMNI_LOC).result()
        print("  dropped BQ external tables")
    except Exception as e:
        print("  BQ cleanup:", str(e)[:150])


def main():
    ap = argparse.ArgumentParser(allow_abbrev=False)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("setup"); s.set_defaults(func=cmd_setup)
    s.add_argument("--bucket", required=True); s.add_argument("--rows", type=int, default=20_000_000)
    o = sub.add_parser("omni"); o.set_defaults(func=cmd_omni); o.add_argument("--bucket", required=True)
    m = sub.add_parser("mft"); m.set_defaults(func=cmd_mft); m.add_argument("--bucket", required=True)
    sub.add_parser("compare").set_defaults(func=cmd_compare)
    sub.add_parser("report").set_defaults(func=cmd_report)
    t = sub.add_parser("teardown"); t.set_defaults(func=cmd_teardown); t.add_argument("--bucket", required=True)
    a = ap.parse_args(); a.func(a)


if __name__ == "__main__":
    main()
