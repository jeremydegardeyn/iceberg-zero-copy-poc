"""Generate synthetic Iceberg test data in S3 and register it in AWS Glue.

Written for the security-review POC, where the point is to prove the access
pattern without putting real data anywhere near it. Everything this produces is
obviously fabricated: customer references are sequential synthetic ids, not
names; there are no emails, addresses, account numbers or free text. Nothing
here resembles production data, by construction.

Unlike omni_write_iceberg.py (which uses a local sqlite catalog and leaves the
table unregistered), this writes through the **Glue catalog**, so the table
appears in Glue immediately and a BigQuery Omni federated dataset picks it up
with no further work on either side.

    python omni_gen_test_data.py --bucket <s3-bucket> --database omni_poc \
        --table orders --rows 500000

Then, GCP side:

    CREATE EXTERNAL SCHEMA <ds> WITH CONNECTION `<proj>.aws-us-east-1.<conn>`
      OPTIONS (external_source='aws-glue://<arn printed below>',
               location='aws-us-east-1');

Teardown is a single flag:

    python omni_gen_test_data.py --bucket <b> --database omni_poc --destroy
"""
import argparse
import datetime as dt
import random

import boto3
import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

# Deliberately boring, obviously-synthetic value pools. No names, no PII.
REGIONS = ["us-east", "us-west", "eu-west", "ap-south"]
PRODUCTS = ["SKU-ALPHA", "SKU-BRAVO", "SKU-CHARLIE", "SKU-DELTA", "SKU-ECHO"]
STATUSES = ["NEW", "PAID", "SHIPPED", "CLOSED", "REFUNDED"]
CURRENCIES = ["USD", "EUR", "GBP"]

SCHEMA = pa.schema([
    ("order_id", pa.int64()),
    ("order_ts", pa.timestamp("us")),
    ("customer_ref", pa.string()),   # synthetic id, e.g. CUST-000042 — never a name
    ("product_sku", pa.string()),
    ("quantity", pa.int32()),
    ("amount", pa.float64()),
    ("currency", pa.string()),
    ("region", pa.string()),
    ("status", pa.string()),
])

BATCH = 100_000


def batches(rows, seed, customers):
    """Yield pyarrow batches of deterministic synthetic rows."""
    rnd = random.Random(seed)
    base = dt.datetime(2026, 1, 1)
    made = 0
    while made < rows:
        n = min(BATCH, rows - made)
        cols = {
            "order_id": list(range(made, made + n)),
            "order_ts": [base + dt.timedelta(seconds=rnd.randrange(31_536_000)) for _ in range(n)],
            "customer_ref": [f"CUST-{rnd.randrange(customers):06d}" for _ in range(n)],
            "product_sku": [rnd.choice(PRODUCTS) for _ in range(n)],
            "quantity": [rnd.randint(1, 25) for _ in range(n)],
            "amount": [round(rnd.uniform(5, 5000), 2) for _ in range(n)],
            "currency": [rnd.choice(CURRENCIES) for _ in range(n)],
            "region": [rnd.choice(REGIONS) for _ in range(n)],
            "status": [rnd.choice(STATUSES) for _ in range(n)],
        }
        yield pa.Table.from_pydict(cols, schema=SCHEMA)
        made += n


def catalog_for(bucket, region):
    # Be explicit about region on BOTH the Glue client and the S3 filesystem.
    # PyIceberg reads "glue.region"/"s3.region" — a bare region_name is ignored
    # and boto3 silently falls back to AWS_DEFAULT_REGION, which will happily
    # register the table in a non-Omni region and leave you debugging a
    # "database not found" from BigQuery.
    return GlueCatalog("glue", **{
        "warehouse": f"s3://{bucket}/warehouse",
        "glue.region": region,
        "s3.region": region,
    })


def destroy(a):
    """Drop the Glue table + database. S3 objects are left for a lifecycle rule
    or an explicit `aws s3 rm`, so nothing is deleted implicitly."""
    cat = catalog_for(a.bucket, a.region)
    try:
        cat.drop_table(f"{a.database}.{a.table}")
        print(f"dropped Glue table {a.database}.{a.table}")
    except NoSuchTableError:
        print("table not present")
    try:
        cat.drop_namespace(a.database)
        print(f"dropped Glue database {a.database}")
    except Exception as e:
        print("drop database:", type(e).__name__)
    print(f"NOTE: S3 objects under s3://{a.bucket}/warehouse/ are untouched — "
          f"remove them explicitly if you want the bucket empty.")


def main():
    p = argparse.ArgumentParser(allow_abbrev=False)
    p.add_argument("--bucket", required=True, help="S3 bucket in an Omni-supported region")
    p.add_argument("--database", default="omni_poc", help="Glue database name")
    p.add_argument("--table", default="orders")
    p.add_argument("--rows", type=int, default=500_000)
    p.add_argument("--customers", type=int, default=50_000)
    p.add_argument("--seed", type=int, default=7, help="fixed seed -> reproducible data")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--destroy", action="store_true", help="drop the Glue table + database")
    a = p.parse_args()

    if a.destroy:
        destroy(a)
        return

    s3 = boto3.client("s3", region_name=a.region)
    kw = {} if a.region == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": a.region}}
    try:
        s3.create_bucket(Bucket=a.bucket, **kw)
        print(f"created s3://{a.bucket}")
    except (s3.exceptions.BucketAlreadyOwnedByYou, s3.exceptions.BucketAlreadyExists):
        print(f"s3://{a.bucket} already exists")

    cat = catalog_for(a.bucket, a.region)
    try:
        cat.create_namespace(a.database)
        print(f"created Glue database {a.database}")
    except NamespaceAlreadyExistsError:
        print(f"Glue database {a.database} already exists")

    ident = f"{a.database}.{a.table}"
    try:
        cat.drop_table(ident)
        print(f"replaced existing {ident}")
    except NoSuchTableError:
        pass
    tbl = cat.create_table(ident, schema=SCHEMA)

    written = 0
    for b in batches(a.rows, a.seed, a.customers):
        tbl.append(b)
        written += b.num_rows
        print(f"  appended {written:,}/{a.rows:,}")

    acct = boto3.client("sts").get_caller_identity()["Account"]
    print(f"\n{written:,} synthetic rows in Iceberg, registered in Glue.")
    print(f"  Glue ARN : arn:aws:glue:{a.region}:{acct}:database/{a.database}")
    print(f"  S3       : s3://{a.bucket}/warehouse/{a.database}.db/{a.table}/")
    print(f"  Location : {'aws-' + a.region} (BigQuery connection + dataset must match)")


if __name__ == "__main__":
    main()
