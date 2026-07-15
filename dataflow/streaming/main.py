"""Streaming flex template: Pub/Sub subscription -> Iceberg (BigLake REST catalog).

Reads from a SUBSCRIPTION (not the topic) so records published before the job
reaches RUNNING state are retained and processed.

The destination table is auto-created by the managed Iceberg sink from the
Event row schema on first commit; Snowflake's catalog-linked database then
discovers it automatically.

Auth: defaults to Iceberg's GoogleAuthManager (auto-refreshing ADC tokens —
production-capable, ships in the same iceberg-gcp jar as GCSFileIO). If the
Beam-bundled Iceberg is too old to have it, fall back to --auth static, which
mints a bearer token at launch that expires after ~1 hour (Google's documented
managed-I/O limitation; relaunch hourly or use Flink/Spark streaming where you
control the Iceberg version).

Message format (JSON):
  {"event_id": 1, "source": "pos-terminal-7", "amount": 12.5,
   "published_at": "2026-07-15T14:00:00Z"}
"""
import argparse
import json
import typing

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

CATALOG_URI = "https://biglake.googleapis.com/iceberg/v1/restcatalog"


class Event(typing.NamedTuple):
    event_id: int
    source: str
    amount: float
    published_at: str  # ISO-8601; kept as string for freshness math downstream


beam.coders.registry.register_coder(Event, beam.coders.RowCoder)


def parse_event(raw: bytes) -> Event:
    d = json.loads(raw.decode("utf-8"))
    return Event(
        event_id=int(d["event_id"]),
        source=str(d.get("source", "unknown")),
        amount=float(d.get("amount", 0.0)),
        published_at=str(d.get("published_at", "")),
    )


def bearer_token() -> str:
    import google.auth
    from google.auth.transport.requests import Request

    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(Request())
    return creds.token


def iceberg_write_config(
    table: str, catalog: str, project_id: str, commit_seconds: int | None, auth: str
) -> dict:
    catalog_props = {
        "type": "rest",
        "uri": CATALOG_URI,
        "warehouse": f"gs://{catalog}",
        "header.x-goog-user-project": project_id,
        # Mandatory for catalogs in vended-credentials mode (400/BadRequest without).
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
        "io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
        "rest-metrics-reporting-enabled": "false",
    }
    if auth == "google":
        # Auto-refreshing ADC tokens; requires Iceberg 1.10+ on the sink classpath.
        catalog_props["rest.auth.type"] = "org.apache.iceberg.gcp.auth.GoogleAuthManager"
    else:
        catalog_props["header.Authorization"] = f"Bearer {bearer_token()}"
    cfg = {
        "table": table,
        "catalog_name": catalog,
        "catalog_properties": catalog_props,
    }
    if commit_seconds:
        cfg["triggering_frequency_seconds"] = commit_seconds
    return cfg


def run() -> None:
    # allow_abbrev=False: otherwise argparse eats Beam's --project as an
    # abbreviation of --project_id and the launcher fails pipeline validation.
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--catalog", required=True, help="bucket name = catalog name")
    ap.add_argument("--subscription", required=True, help="full subscription path")
    ap.add_argument("--table", default="shared_aws.events")
    ap.add_argument("--commit_seconds", type=int, default=30)
    ap.add_argument("--auth", choices=["google", "static"], default="google")
    args, beam_args = ap.parse_known_args()

    opts = PipelineOptions(beam_args, streaming=True, save_main_session=True)
    with beam.Pipeline(options=opts) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=args.subscription)
            | "Parse" >> beam.Map(parse_event).with_output_types(Event)
            | "WriteIceberg"
            >> beam.managed.Write(
                beam.managed.ICEBERG,
                config=iceberg_write_config(
                    args.table, args.catalog, args.project_id, args.commit_seconds, args.auth
                ),
            )
        )


if __name__ == "__main__":
    run()
