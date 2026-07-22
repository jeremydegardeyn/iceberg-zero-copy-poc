"""POC: does the BigQuery Storage Read API work on a BigQuery Omni external
table? This is the exact mechanism Dataflow's BigQueryIO DIRECT_READ (and the
Spark-BigQuery connector) use, so the answer decides whether those tools can
read an Omni table directly or must consume a materialized native copy.

Result observed 2026-07-21 against omni_s3.orders (aws-us-east-1):
    FAILED — InvalidArgument: 400 request failed:
    "Read API can be used to read temporary tables only in this region."
Confirms: Dataflow/Spark cannot direct-read an Omni table; materialize first.
See docs/adr-omni-reverse/R003-materialize-for-native-consumers.md.

Usage: python omni_storage_read_test.py <project> [dataset.table]
"""
import sys
import warnings

warnings.filterwarnings("ignore")
from google.cloud.bigquery_storage_v1 import BigQueryReadClient, types

proj = sys.argv[1]
tref = sys.argv[2] if len(sys.argv) > 2 else "omni_s3.orders"
dataset, tbl = tref.split(".")
table = f"projects/{proj}/datasets/{dataset}/tables/{tbl}"

print("BigQuery Storage Read API (Dataflow DIRECT_READ path) on:", table)
client = BigQueryReadClient()
req = types.CreateReadSessionRequest(
    parent=f"projects/{proj}",
    read_session=types.ReadSession(table=table, data_format=types.DataFormat.ARROW),
    max_stream_count=1,
)
try:
    sess = client.create_read_session(request=req)
    print(f"SUCCESS — read session with {len(sess.streams)} stream(s). Direct read IS supported.")
except Exception as e:
    print("FAILED — Storage Read API rejected the Omni external table.")
    print(" ", type(e).__name__, "-", str(e).splitlines()[0][:400])
    sys.exit(1)
