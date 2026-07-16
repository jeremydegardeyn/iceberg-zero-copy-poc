"""AWS side of the S3 replica (ADR-0002 option C): bucket + IAM role for the
Snowflake external volume. Two-phase, mirroring the WIF dance:

  phase 1:  python 11_aws_replica_setup.py create --bucket <name>
            -> creates the S3 bucket (us-east-2, same region as Snowflake =
               intra-region reads) + an IAM role with a placeholder trust
               policy + read policy on the bucket. Prints the ROLE ARN for
               CREATE EXTERNAL VOLUME in sql/04_s3_replica.sql.

  (in Snowflake) CREATE EXTERNAL VOLUME, then DESC EXTERNAL VOLUME ->
            copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID.

  phase 2:  python 11_aws_replica_setup.py trust --bucket <name> \
                --iam_user_arn <arn> --external_id <id>
            -> rewrites the role trust policy so Snowflake's IAM user can
               assume it with that external id.

Credentials come from the standard AWS chain (env vars / ~/.aws/credentials),
configured by the user — never stored here.
"""
import argparse
import json
import sys

import boto3

REGION = "us-east-2"  # match the Snowflake account region
ROLE_NAME = "iceberg-poc-snowflake-replica"


def create(bucket: str) -> None:
    s3 = boto3.client("s3", region_name=REGION)
    iam = boto3.client("iam")

    try:
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        print(f"created s3://{bucket} in {REGION}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"s3://{bucket} already exists (yours)")

    account = boto3.client("sts").get_caller_identity()["Account"]
    placeholder_trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{account}:root"},
            "Action": "sts:AssumeRole",
        }],
    }
    read_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow",
             "Action": ["s3:GetObject", "s3:GetObjectVersion"],
             "Resource": f"arn:aws:s3:::{bucket}/*"},
            {"Effect": "Allow",
             "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
             "Resource": f"arn:aws:s3:::{bucket}"},
        ],
    }
    try:
        role = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(placeholder_trust),
            Description="Snowflake external volume read access to the Iceberg replica",
        )
        print(f"created role {ROLE_NAME}")
    except iam.exceptions.EntityAlreadyExistsException:
        role = iam.get_role(RoleName=ROLE_NAME)
        print(f"role {ROLE_NAME} already exists")
    iam.put_role_policy(
        RoleName=ROLE_NAME, PolicyName="replica-bucket-read",
        PolicyDocument=json.dumps(read_policy),
    )
    print()
    print("STORAGE_AWS_ROLE_ARN for sql/04_s3_replica.sql:")
    print(f"  {role['Role']['Arn']}")


def trust(bucket: str, iam_user_arn: str, external_id: str) -> None:
    iam = boto3.client("iam")
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": iam_user_arn},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
        }],
    }
    iam.update_assume_role_policy(
        RoleName=ROLE_NAME, PolicyDocument=json.dumps(trust_policy)
    )
    print(f"trust policy updated: {iam_user_arn} may assume {ROLE_NAME} "
          f"with external id {external_id}")
    print("Now run in Snowflake:  DESC EXTERNAL VOLUME s3_replica_vol;  -- then query")


def main() -> None:
    ap = argparse.ArgumentParser(allow_abbrev=False)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p1 = sub.add_parser("create")
    p1.add_argument("--bucket", required=True)
    p2 = sub.add_parser("trust")
    p2.add_argument("--bucket", required=True)
    p2.add_argument("--iam_user_arn", required=True)
    p2.add_argument("--external_id", required=True)
    args = ap.parse_args()
    if args.cmd == "create":
        create(args.bucket)
    else:
        trust(args.bucket, args.iam_user_arn, args.external_id)


if __name__ == "__main__":
    sys.exit(main())
