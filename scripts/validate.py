"""Config-driven integrity controls: source vs Iceberg (lake) vs Snowflake.

Reads validation.yaml; for each feed gathers three sides and compares:
  source    - re-count of archived CSVs in GCS (batch feeds only; streaming
              sources are ephemeral -> producers emit audit counts in prod)
  lake      - Iceberg snapshot summary via the BigLake REST catalog (pure
              HTTP metadata read; no Spark, no file scanning, no cost)
  snowflake - COUNT/SUM/null/dupe aggregates via the snow CLI

POC: run manually via scripts/09_validate.sh after a demo.
Production: the same script, containerized as a Cloud Run job, fired by a
Cloud Scheduler cron (streaming drift) and after each batch archive move;
failures publish to an alert topic instead of a terminal.

Exit code 0 = all hard checks pass; 1 = at least one breach.
"""
import argparse
import datetime
import json
import os
import subprocess
import sys

import requests
import yaml

CATALOG_URI = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
UTC = datetime.timezone.utc


def sh(cmd: list) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, capture_output=True, text=True, shell=(sys.platform == "win32")
    )


def gcloud_token() -> str:
    r = sh(["gcloud", "auth", "print-access-token"])
    if r.returncode != 0:
        sys.exit(f"gcloud token failed: {r.stderr.strip()}")
    return r.stdout.strip()


def lake_side(project: str, bucket: str, namespace: str, table: str, token: str) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "x-goog-user-project": project,
        # Required on table loads when the catalog runs in vended-credentials mode.
        "X-Iceberg-Access-Delegation": "vended-credentials",
    }
    cfg = requests.get(
        f"{CATALOG_URI}/v1/config", params={"warehouse": f"gs://{bucket}"},
        headers=headers, timeout=30,
    )
    cfg.raise_for_status()
    c = cfg.json()
    prefix = c.get("overrides", {}).get("prefix") or c.get("defaults", {}).get("prefix")
    r = requests.get(
        f"{CATALOG_URI}/v1/{prefix}/namespaces/{namespace}/tables/{table}",
        headers=headers, timeout=30,
    )
    if r.status_code in (400, 404):  # BigLake returns 400 for missing tables
        return {"exists": False, "detail": r.json().get("error", {}).get("message", "")[:120]}
    r.raise_for_status()
    meta = r.json()["metadata"]
    snap_id = meta.get("current-snapshot-id")
    snap = next(
        (s for s in meta.get("snapshots", []) if s["snapshot-id"] == snap_id), None
    )
    return {
        "exists": True,
        "total_records": int(snap["summary"].get("total-records", -1)) if snap else 0,
        "snapshot_id": snap_id,
        "snapshot_at": (
            datetime.datetime.fromtimestamp(snap["timestamp-ms"] / 1000, tz=UTC).isoformat()
            if snap else None
        ),
        "snapshot_count": len(meta.get("snapshots", [])),
    }


def snow_sql_rows(query: str) -> list:
    snow = os.environ.get(
        "SNOW_EXE",
        os.path.expanduser("~/AppData/Roaming/Python/Python310/Scripts/snow.exe"),
    )
    r = sh([snow, "sql", "-q", query, "--format", "json"])
    out = r.stdout.strip()
    start = out.find("[")
    if r.returncode != 0 or start < 0:
        raise RuntimeError(f"snow sql failed: {r.stderr.strip()[:400]}")
    return json.loads(out[start:])


def snowflake_side(db: str, namespace: str, table: str, cfg: dict) -> dict:
    fq = f"{db}.{namespace}.{table}"
    nulls_cols = cfg.get("checks", {}).get("nulls", {}).get("columns", [])
    null_exprs = ", ".join(
        f"SUM(IFF({c} IS NULL, 1, 0)) AS nulls_{c}" for c in nulls_cols
    ) or "0 AS nulls_none"
    keys = cfg.get("checks", {}).get("dupes", {}).get("keys", [])
    dupe_expr = (
        f"COUNT(*) - COUNT(DISTINCT {', '.join(keys)}) AS dupes" if keys else "0 AS dupes"
    )
    rows = snow_sql_rows(
        f"SELECT COUNT(*) AS row_count, ROUND(SUM(amount), 2) AS sum_amount, "
        f"MAX(published_at) AS max_published_at, {dupe_expr}, {null_exprs} FROM {fq}"
    )
    return {k.lower(): v for k, v in rows[0].items()}


def source_side(src: dict) -> dict:
    ls = sh(["gsutil", "ls", f"gs://{src['bucket']}/{src['pattern']}"])
    files = [p for p in ls.stdout.split() if p.startswith("gs://")]
    total_rows, total_sum = 0, 0.0
    for path in files:
        content = sh(["gsutil", "cat", path]).stdout
        lines = [l for l in content.strip().splitlines() if l.strip()]
        if not lines:
            continue
        header = [h.strip() for h in lines[0].split(",")]
        idx = header.index(src.get("sum_column", "amount"))
        total_rows += len(lines) - 1
        total_sum += sum(float(l.split(",")[idx]) for l in lines[1:])
    return {"files": len(files), "rows": total_rows, "sum": round(total_sum, 2)}


def validate_feed(feed: dict, project: str, bucket: str, db: str, token: str) -> dict:
    namespace, table = feed["table"].split(".")
    checks_cfg = feed.get("checks", {})
    result = {"table": feed["table"], "checks": [], "sides": {}}

    lake = lake_side(project, bucket, namespace, table, token)
    result["sides"]["lake"] = lake
    if not lake.get("exists"):
        result["checks"].append(
            {"check": "table_exists_in_catalog", "status": "FAIL", "detail": "not in catalog"}
        )
        return result

    sf = snowflake_side(db, namespace, table, feed)
    result["sides"]["snowflake"] = sf

    src = None
    if isinstance(feed.get("source"), dict) and feed["source"].get("type") == "gcs_csv_archive":
        src = source_side(feed["source"])
        result["sides"]["source"] = src

    def check(name, ok, detail, report_only=False):
        status = "PASS" if ok else ("INFO" if report_only else "FAIL")
        result["checks"].append({"check": name, "status": status, "detail": detail})

    check(
        "row_count lake==snowflake",
        lake["total_records"] == sf["row_count"],
        f"lake={lake['total_records']} snowflake={sf['row_count']} "
        f"(mismatch can be catalog-refresh lag; rerun after ~30s)",
    )
    if src is not None:
        check(
            "row_count source==lake",
            src["rows"] == lake["total_records"],
            f"source_csvs={src['rows']} lake={lake['total_records']}",
        )
        tol = checks_cfg.get("sum_source_vs_snowflake", {}).get("tolerance", 0.01)
        diff = abs((src["sum"] or 0) - float(sf["sum_amount"] or 0))
        check(
            "sum(amount) source==snowflake",
            diff <= tol,
            f"source={src['sum']} snowflake={sf['sum_amount']} diff={round(diff, 4)}",
        )
    for col in checks_cfg.get("nulls", {}).get("columns", []):
        n = int(sf.get(f"nulls_{col.lower()}", 0) or 0)
        check(f"nulls({col})", n <= checks_cfg["nulls"].get("max", 0), f"{n} nulls")
    if checks_cfg.get("dupes"):
        d = int(sf.get("dupes", 0) or 0)
        check("dupes(keys)", d <= checks_cfg["dupes"].get("max", 0), f"{d} dupes")
    if checks_cfg.get("freshness_lag"):
        check(
            "freshness", True,
            f"latest snapshot {lake['snapshot_at']}; max published_at {sf['max_published_at']}",
            report_only=True,
        )
    return result


def main() -> None:
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--config", default="validation.yaml")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--snowflake_db", default="shared_gcp_data")
    args = ap.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    token = gcloud_token()
    report = {
        "ran_at": datetime.datetime.now(UTC).isoformat(),
        "feeds": [
            validate_feed(feed, args.project_id, args.catalog, args.snowflake_db, token)
            for feed in cfg["feeds"]
        ],
    }
    failed = [
        c for f in report["feeds"] for c in f["checks"] if c["status"] == "FAIL"
    ]
    report["result"] = "FAIL" if failed else "PASS"
    print(json.dumps(report, indent=2, default=str))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
