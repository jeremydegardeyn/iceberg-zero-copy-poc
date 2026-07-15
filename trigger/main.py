"""Two Cloud Run functions for the batch pipeline:

on_file       - GCS finalize event on the drop bucket -> launch the batch
                flex template. Launch-only; returns immediately.
on_job_status - Eventarc Dataflow job-status event -> when an iceberg-poc-batch
                job reaches JOB_STATE_DONE, archive its input CSV.

Archiving is event-driven (not polled): batch jobs run ~12 min, which outlives
the 540s event-function request limit, and the flex-template launcher's
wait_until_finish() is a no-op — only a job-status event reliably observes
real completion.
"""
import os
import re
import time

import functions_framework
from google.cloud import storage
from googleapiclient.discovery import build


def archive(bucket_name: str, blob_name: str, archive_bucket: str) -> None:
    client = storage.Client()
    src_bucket = client.bucket(bucket_name)
    src_blob = src_bucket.blob(blob_name)
    src_bucket.copy_blob(src_blob, client.bucket(archive_bucket), blob_name)
    src_blob.delete()
    print(f"archived gs://{bucket_name}/{blob_name} -> gs://{archive_bucket}/{blob_name}")


@functions_framework.cloud_event
def on_file(cloud_event):
    data = cloud_event.data
    bucket, name = data["bucket"], data["name"]
    if not name.lower().endswith(".csv"):
        print(f"ignoring non-csv object: gs://{bucket}/{name}")
        return

    project = os.environ["PROJECT_ID"]
    region = os.environ["REGION"]
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:30]
    job_name = f"iceberg-poc-batch-{slug}-{int(time.time())}"

    body = {
        "launchParameter": {
            "jobName": job_name,
            "containerSpecGcsPath": os.environ["TEMPLATE_PATH"],
            "parameters": {
                "input": f"gs://{bucket}/{name}",
                "project_id": project,
                "catalog": os.environ["CATALOG"],
                "table": os.environ.get("TABLE", "shared_aws.batch_events"),
            },
            "environment": {
                "tempLocation": os.environ["TEMP_LOCATION"],
                "maxWorkers": 1,
                # Capacity hardening: regional placement + plentiful e2 family
                # for both the launcher VM and workers.
                "workerRegion": region,
                "launcherMachineType": "e2-standard-2",
                "machineType": "e2-standard-2",
            },
        }
    }
    dataflow = build("dataflow", "v1b3", cache_discovery=False)
    resp = (
        dataflow.projects()
        .locations()
        .flexTemplates()
        .launch(projectId=project, location=region, body=body)
        .execute()
    )
    job_id = resp.get("job", {}).get("id")
    print(f"launched {job_name}: {job_id}")


@functions_framework.cloud_event
def on_job_status(cloud_event):
    """Archive the input CSV when an iceberg-poc-batch job completes."""
    job = cloud_event.data.get("payload", cloud_event.data)
    name = job.get("name", "")
    state = job.get("currentState", "")
    if not name.startswith("iceberg-poc-batch") or state != "JOB_STATE_DONE":
        return

    project = os.environ["PROJECT_ID"]
    region = os.environ["REGION"]
    dataflow = build("dataflow", "v1b3", cache_discovery=False)
    detail = (
        dataflow.projects()
        .locations()
        .jobs()
        .get(projectId=project, location=region, jobId=job["id"], view="JOB_VIEW_ALL")
        .execute()
    )
    display = detail.get("pipelineDescription", {}).get("displayData", [])
    input_path = next(
        (d.get("strValue") for d in display if d.get("key") == "input"), None
    )
    if not input_path or not input_path.startswith("gs://"):
        print(f"job {job['id']} DONE but no input displayData; nothing archived")
        return
    bucket, _, blob = input_path[5:].partition("/")
    archive(bucket, blob, os.environ.get("ARCHIVE_BUCKET", "scs-raw-archive"))
