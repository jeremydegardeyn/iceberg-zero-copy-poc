"""Create and run the AWS Glue job that turns the S3 landing zone into Iceberg.

  python 14_glue_job.py setup --bucket <s3-bucket>   # IAM role, script upload, job def
  python 14_glue_job.py run   --bucket <s3-bucket>   # start a run and wait

The job itself (aws/glue_landing_to_iceberg.py) carries no credentials: it runs
under an IAM role, so the Iceberg Glue catalog client authenticates natively.
That is the whole argument of ADR-0007 — run the writer where the storage is.
"""
import argparse
import json
import pathlib
import time

import boto3

REGION = "us-east-2"
ROLE = "iceberg-poc-glue-job-role"
JOB = "iceberg-poc-landing-to-iceberg"
CATALOG_DB = "iceberg_poc_replica"
TABLE = "stream_events"


def setup(bucket: str) -> None:
    iam = boto3.client("iam")
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    try:
        role = iam.create_role(
            RoleName=ROLE, AssumeRolePolicyDocument=json.dumps(trust),
            Description="Glue job: S3 landing zone -> Iceberg + Glue catalog",
        )
        print(f"created role {ROLE}")
    except iam.exceptions.EntityAlreadyExistsException:
        role = iam.get_role(RoleName=ROLE)
        print(f"role {ROLE} exists")
    iam.attach_role_policy(
        RoleName=ROLE,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    )
    iam.put_role_policy(
        RoleName=ROLE, PolicyName="bucket-access",
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                           "s3:ListBucket", "s3:GetBucketLocation"],
                "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
            }],
        }),
    )
    print("attached AWSGlueServiceRole + bucket access")

    script = pathlib.Path(__file__).parent.parent / "aws" / "glue_landing_to_iceberg.py"
    key = "glue-scripts/glue_landing_to_iceberg.py"
    boto3.client("s3", region_name=REGION).put_object(
        Bucket=bucket, Key=key, Body=script.read_bytes()
    )
    print(f"uploaded script -> s3://{bucket}/{key}")

    glue = boto3.client("glue", region_name=REGION)
    job_def = {
        "Role": role["Role"]["Arn"],
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": f"s3://{bucket}/{key}",
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            # Glue's native Iceberg support — no jars to manage.
            "--datalake-formats": "iceberg",
            "--conf": (
                "spark.sql.extensions=org.apache.iceberg.spark.extensions."
                "IcebergSparkSessionExtensions "
                "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
                f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{bucket}/iceberg/ "
                "--conf spark.sql.catalog.glue_catalog.catalog-impl="
                "org.apache.iceberg.aws.glue.GlueCatalog "
                "--conf spark.sql.catalog.glue_catalog.io-impl="
                "org.apache.iceberg.aws.s3.S3FileIO"
            ),
            "--landing_path": f"s3://{bucket}/landing/stream/",
            "--warehouse": f"s3://{bucket}/iceberg/",
            "--catalog_db": CATALOG_DB,
            "--table_name": TABLE,
            "--write_mode": "overwrite",
            "--TempDir": f"s3://{bucket}/glue-temp/",
            "--enable-job-insights": "true",
        },
        "GlueVersion": "4.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 20,
    }
    try:
        glue.create_job(Name=JOB, **job_def)
        print(f"created glue job {JOB}")
    except glue.exceptions.AlreadyExistsException:
        glue.update_job(JobName=JOB, JobUpdate=job_def)
        print(f"updated glue job {JOB}")
    print("\nIAM propagation takes ~10s; then: 14_glue_job.py run --bucket <bucket>")


def run(bucket: str) -> None:
    glue = boto3.client("glue", region_name=REGION)
    rid = glue.start_job_run(JobName=JOB)["JobRunId"]
    print(f"started run {rid}")
    while True:
        r = glue.get_job_run(JobName=JOB, RunId=rid)["JobRun"]
        st = r["JobRunState"]
        if st in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            break
        time.sleep(15)
    print(f"run {st} in {r.get('ExecutionTime', '?')}s")
    if st != "SUCCEEDED":
        print("error:", r.get("ErrorMessage", "(see CloudWatch)")[:400])
        raise SystemExit(1)


def main() -> None:
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("cmd", choices=["setup", "run"])
    ap.add_argument("--bucket", required=True)
    args = ap.parse_args()
    (setup if args.cmd == "setup" else run)(args.bucket)


if __name__ == "__main__":
    main()
