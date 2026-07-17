"""Batch flex template: CSV in GCS -> Iceberg, in EITHER catalog.

  --catalog_type biglake  (default)  -> Iceberg on GCS, BigLake REST catalog
  --catalog_type glue                -> Iceberg on S3, AWS Glue catalog

The point of the second mode: if the consumers are on AWS, writing straight to
S3+Glue deletes the entire replica path (no rewrite_table_path, no copy plan,
no scheduler, no staleness) for the same egress volume and half the storage.
Same pipeline, same code — the catalog is configuration. See ADR-0007.

Triggered by the scs-raw file-drop function (trigger/main.py) or run manually.
CSV format (header required): event_id,source,amount,published_at

Archiving the input file is NOT done here: in flex-template mode the launcher's
wait_until_finish() is a no-op (it returns before the real job runs — we
learned this the hard way when the launcher archived the input mid-preflight).
The trigger function polls the Dataflow API for genuine completion and then
performs the move.
"""
import argparse
import json
import typing

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

CATALOG_URI = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
AWS_SECRET = "aws-replica-creds"


class Event(typing.NamedTuple):
    event_id: int
    source: str
    amount: float
    published_at: str


beam.coders.registry.register_coder(Event, beam.coders.RowCoder)


def parse_csv_line(line: str) -> Event:
    event_id, source, amount, published_at = [c.strip() for c in line.split(",")]
    return Event(
        event_id=int(event_id),
        source=source,
        amount=float(amount),
        published_at=published_at,
    )


def bearer_token() -> str:
    import google.auth
    from google.auth.transport.requests import Request

    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(Request())
    return creds.token


def aws_creds(project_id: str) -> dict:
    """AWS key from Secret Manager — never a flex-template parameter, which
    would expose it in the Dataflow job description. Production answer is AWS
    workload identity federation (keyless); see ADR-0007."""
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{AWS_SECRET}/versions/latest"
    return json.loads(client.access_secret_version(name=name).payload.data.decode())


def catalog_properties(catalog_type: str, catalog: str, project_id: str, region: str) -> dict:
    if catalog_type == "biglake":
        return {
            "type": "rest",
            "uri": CATALOG_URI,
            "warehouse": f"gs://{catalog}",
            "header.x-goog-user-project": project_id,
            "header.Authorization": f"Bearer {bearer_token()}",
            # Mandatory for catalogs in vended-credentials mode.
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
            "io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
            "rest-metrics-reporting-enabled": "false",
        }
    c = aws_creds(project_id)
    return {
        "type": "glue",
        "warehouse": f"s3://{catalog}/direct",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "client.region": region,
        "s3.access-key-id": c["access_key_id"],
        "s3.secret-access-key": c["secret_access_key"],
        "client.credentials-provider":
            "software.amazon.awssdk.auth.credentials.StaticCredentialsProvider",
        "client.credentials-provider.aws.accessKeyId": c["access_key_id"],
        "client.credentials-provider.aws.secretAccessKey": c["secret_access_key"],
    }


def run() -> None:
    # allow_abbrev=False: otherwise argparse eats Beam's --project as an
    # abbreviation of --project_id and the launcher fails pipeline validation.
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--input", required=True, help="gs://bucket/file.csv")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--catalog", required=True, help="GCS bucket, or S3 bucket for glue")
    ap.add_argument("--catalog_type", choices=["biglake", "glue"], default="biglake")
    ap.add_argument("--aws_region", default="us-east-2")
    ap.add_argument("--table", default="shared_aws.batch_events")
    args, beam_args = ap.parse_known_args()

    write_config = {
        "table": args.table,
        "catalog_name": args.catalog.replace("-", "_"),
        "catalog_properties": catalog_properties(
            args.catalog_type, args.catalog, args.project_id, args.aws_region
        ),
    }

    opts = PipelineOptions(beam_args, save_main_session=True)
    with beam.Pipeline(options=opts) as p:
        (
            p
            | "ReadCSV" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | "Parse" >> beam.Map(parse_csv_line).with_output_types(Event)
            | "WriteIceberg" >> beam.managed.Write(beam.managed.ICEBERG, config=write_config)
        )


if __name__ == "__main__":
    run()
