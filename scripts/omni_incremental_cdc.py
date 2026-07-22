"""Incremental CDC straight from the S3 Iceberg lake — no BigQuery/Omni in the
loop. This is the alternative to the Dataflow-can't-read-Omni problem
(see docs/adr-omni-reverse/R006-direct-s3-incremental-cdc.md and R003).

Iceberg records every commit as a snapshot; an APPEND snapshot's new manifest
carries the newly ADDED data files. So a consumer can:

  1. remember the last snapshot it processed (a watermark),
  2. on each run, walk the snapshots added since then,
  3. read ONLY the data files those snapshots added (the diff),
  4. emit those rows to a sink (Pub/Sub, Bigtable, ...),
  5. advance the watermark.

That is CDC-style incremental loading without a full re-scan and without the
Storage Read API — the files are plain Parquet in S3, read with the object
store's own credentials.

Scope: append-only tables (the common streaming/event case). Tables with
row-level deletes / overwrites need delete-file handling, called out in the ADR.

AWS creds come from the environment. Prod swaps the SqlCatalog for your real
catalog (Glue / BigLake REST / Nessie) — only the load_table line changes.

Usage:
  python omni_incremental_cdc.py append --rows 3          # add a test snapshot
  python omni_incremental_cdc.py sync  --sink stdout      # emit the diff
  python omni_incremental_cdc.py sync  --sink pubsub --topic projects/P/topics/T
  python omni_incremental_cdc.py sync  --sink bigtable --instance I --table T --key-column order_id
"""
import argparse
import json
import os
import warnings

warnings.filterwarnings("ignore")
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import ManifestContent, ManifestEntryStatus

REGION = os.environ.get("AWS_REGION", "us-east-1")


def load_table(args):
    cat = SqlCatalog("omni", **{
        "uri": args.catalog_uri,
        "warehouse": args.warehouse,
        "s3.region": REGION,
        "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
    })
    return cat.load_table(args.table)


def s3():
    return s3fs.S3FileSystem(
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
        client_kwargs={"region_name": REGION},
    )


# ---------------------------------------------------------------- incremental core
def snapshots_since(table, since_id):
    """Snapshots strictly after since_id, oldest-first (walks the parent chain)."""
    chain, snap = [], table.current_snapshot()
    while snap is not None and snap.snapshot_id != since_id:
        chain.append(snap)
        pid = snap.parent_snapshot_id
        snap = table.snapshot_by_id(pid) if pid is not None else None
    chain.reverse()
    return chain


def added_files(table, snap):
    """Data-file paths this snapshot ADDED (its own new manifests, ADDED entries)."""
    paths = []
    for mf in snap.manifests(table.io):
        if mf.content == ManifestContent.DATA and mf.added_snapshot_id == snap.snapshot_id:
            for e in mf.fetch_manifest_entry(table.io, discard_deleted=True):
                if e.status == ManifestEntryStatus.ADDED:
                    paths.append(e.data_file.file_path)
    return paths


def read_rows(fs, paths):
    tbls = []
    for p in paths:
        tbls.append(pq.read_table(p[len("s3://"):] if p.startswith("s3://") else p, filesystem=fs))
    return pa.concat_tables(tbls) if tbls else None


# ---------------------------------------------------------------- sinks
def sink_stdout(records, args):
    for r in records:
        print(json.dumps(r))
    return len(records)


def sink_pubsub(records, args):
    from google.cloud import pubsub_v1
    pub = pubsub_v1.PublisherClient()
    futures = [pub.publish(args.topic, json.dumps(r).encode("utf-8")) for r in records]
    for f in futures:
        f.result()
    return len(records)


def sink_bigtable(records, args):
    from google.cloud import bigtable
    table = bigtable.Client(admin=False).instance(args.instance).table(args.table_bt)
    rows = []
    for r in records:
        row = table.direct_row(str(r[args.key_column]).encode("utf-8"))
        for k, v in r.items():
            row.set_cell("cf", k.encode(), json.dumps(v).encode())
        rows.append(row)
    table.mutate_rows(rows)
    return len(records)


SINKS = {"stdout": sink_stdout, "pubsub": sink_pubsub, "bigtable": sink_bigtable}


# ---------------------------------------------------------------- watermark state
def read_state(path, table_name):
    if path and os.path.exists(path):
        return json.load(open(path)).get(table_name)
    return None


def write_state(path, table_name, snap_id):
    if not path:
        return
    d = json.load(open(path)) if os.path.exists(path) else {}
    d[table_name] = snap_id
    json.dump(d, open(path, "w"))


# ---------------------------------------------------------------- commands
def cmd_append(args):
    table = load_table(args)
    base = table.scan().to_arrow().num_rows
    rows = [{"order_id": base + i + 1, "customer": f"cdc-{base + i + 1}",
             "amount": round(10.0 * (i + 1), 2), "source_cloud": "aws-s3"}
            for i in range(args.rows)]
    table.append(pa.Table.from_pylist(rows, schema=table.schema().as_arrow()))
    print(f"appended {args.rows} rows -> new snapshot {table.current_snapshot().snapshot_id}")


def cmd_sync(args):
    table = load_table(args)
    since = args.since if args.since is not None else read_state(args.state, args.table)
    new_snaps = snapshots_since(table, since)
    if not new_snaps:
        print(f"up to date — no snapshots since {since}")
        return
    fs, total = s3(), 0
    for snap in new_snaps:
        paths = added_files(table, snap)
        tbl = read_rows(fs, paths)
        recs = tbl.to_pylist() if tbl is not None else []
        for r in recs:
            r["_snapshot_id"] = snap.snapshot_id
        n = SINKS[args.sink](recs, args)
        total += n
        print(f"snapshot {snap.snapshot_id} ({snap.summary.operation.value}): "
              f"{len(paths)} new file(s), emitted {n} row(s) to {args.sink}")
    write_state(args.state, args.table, table.current_snapshot().snapshot_id)
    print(f"watermark advanced to {table.current_snapshot().snapshot_id} — {total} row(s) total")


def main():
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--catalog-uri", default="sqlite:///omni_cat.db")
    ap.add_argument("--warehouse", default="s3://iceberg-poc-omni-jdg/warehouse")
    ap.add_argument("--table", default="demo.orders")
    ap.add_argument("--state", default="omni_cdc_state.json", help="watermark file")
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("append")
    a.add_argument("--rows", type=int, default=3)
    a.set_defaults(func=cmd_append)

    s = sub.add_parser("sync")
    s.add_argument("--since", type=int, default=None, help="override watermark snapshot id")
    s.add_argument("--sink", choices=list(SINKS), default="stdout")
    s.add_argument("--topic", help="pubsub: projects/<p>/topics/<t>")
    s.add_argument("--instance", help="bigtable instance id")
    s.add_argument("--table-bt", help="bigtable table id")
    s.add_argument("--key-column", default="order_id")
    s.set_defaults(func=cmd_sync)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
