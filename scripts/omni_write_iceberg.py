"""Reverse-direction setup: create an S3 bucket in a BigQuery Omni region and
write a small Iceberg table to it with PyIceberg — no Spark, no Dataproc, no
NAT. Prints the metadata.json location for a BigQuery external table to read.

AWS credentials come from the standard chain (env vars / ~/.aws). See
docs/runbook-omni-reverse.md.

Usage: python omni_write_iceberg.py --bucket <s3-bucket> [--region us-east-1]
"""
import argparse
import os

import boto3
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

ap = argparse.ArgumentParser(allow_abbrev=False)
ap.add_argument("--bucket", required=True)
ap.add_argument("--region", default="us-east-1", help="must be an Omni-supported AWS region")
args = ap.parse_args()

# 1. bucket (us-east-1 takes no LocationConstraint; others do)
s3 = boto3.client("s3", region_name=args.region)
kw = {} if args.region == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": args.region}}
try:
    s3.create_bucket(Bucket=args.bucket, **kw)
    print("created s3://" + args.bucket)
except s3.exceptions.BucketAlreadyOwnedByYou:
    print("s3://" + args.bucket + " already exists")

# 2. PyIceberg catalog (sqlite pointer; data + metadata land in S3)
catalog = SqlCatalog("omni", **{
    "uri": "sqlite:///omni_cat.db",
    "warehouse": f"s3://{args.bucket}/warehouse",
    "s3.region": args.region,
    "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
    "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
})
schema = pa.schema([
    ("order_id", pa.int64()), ("customer", pa.string()),
    ("amount", pa.float64()), ("source_cloud", pa.string()),
])
data = pa.table({
    "order_id": [1, 2, 3, 4],
    "customer": ["acme", "globex", "initech", "omni-proof"],
    "amount": [100.50, 250.00, 75.25, 42.00],
    "source_cloud": ["aws-s3"] * 4,
}, schema=schema)

try:
    catalog.create_namespace("demo")
except Exception:
    pass
try:
    catalog.drop_table("demo.orders")
except Exception:
    pass
tbl = catalog.create_table("demo.orders", schema=schema)
tbl.append(data)
print("ROWS:", tbl.scan().to_arrow().num_rows)
print("METADATA_LOCATION:", tbl.metadata_location)
