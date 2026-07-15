"""Batch flex template: CSV in GCS -> Iceberg (BigLake REST catalog).

Triggered by the scs-raw file-drop function (trigger/main.py) or run manually.
CSV format (header required): event_id,source,amount,published_at

Archiving the input file is NOT done here: in flex-template mode the launcher's
wait_until_finish() is a no-op (it returns before the real job runs — we
learned this the hard way when the launcher archived the input mid-preflight).
The trigger function polls the Dataflow API for genuine completion and then
performs the move.
"""
import argparse
import typing

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

CATALOG_URI = "https://biglake.googleapis.com/iceberg/v1/restcatalog"


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


def run() -> None:
    # allow_abbrev=False: otherwise argparse eats Beam's --project as an
    # abbreviation of --project_id and the launcher fails pipeline validation.
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--input", required=True, help="gs://bucket/file.csv")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--catalog", required=True, help="bucket name = catalog name")
    ap.add_argument("--table", default="shared_aws.batch_events")
    args, beam_args = ap.parse_known_args()

    write_config = {
        "table": args.table,
        "catalog_name": args.catalog,
        "catalog_properties": {
            "type": "rest",
            "uri": CATALOG_URI,
            "warehouse": f"gs://{args.catalog}",
            "header.x-goog-user-project": args.project_id,
            "header.Authorization": f"Bearer {bearer_token()}",
            # Mandatory for catalogs in vended-credentials mode.
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
            "io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
            "rest-metrics-reporting-enabled": "false",
        },
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
