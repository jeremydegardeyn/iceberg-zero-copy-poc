"""AWS IAM role for BigQuery Omni to assume (web-identity federation via
accounts.google.com) with read access to the Omni S3 bucket.

  create  -> role with placeholder trust + S3 read policy; prints role ARN
  trust   -> tighten trust to the BQ connection's identity (sub)
"""
import argparse
import json
import boto3

ROLE = "bq-omni-s3-reader"


def create(bucket):
    iam = boto3.client("iam")
    placeholder = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Federated": "accounts.google.com"},
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {"StringEquals": {"accounts.google.com:sub": "__placeholder__"}},
        }],
    }
    try:
        r = iam.create_role(RoleName=ROLE, AssumeRolePolicyDocument=json.dumps(placeholder),
                            Description="BigQuery Omni read access to the Iceberg S3 bucket")
        print("created role", ROLE)
    except iam.exceptions.EntityAlreadyExistsException:
        r = iam.get_role(RoleName=ROLE)
        print("role exists")
    iam.put_role_policy(RoleName=ROLE, PolicyName="s3-read", PolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:GetObjectVersion"],
             "Resource": f"arn:aws:s3:::{bucket}/*"},
            {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
             "Resource": f"arn:aws:s3:::{bucket}"},
        ],
    }))
    print("STORAGE role ARN:", r["Role"]["Arn"])


def trust(identity):
    iam = boto3.client("iam")
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Federated": "accounts.google.com"},
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {"StringEquals": {"accounts.google.com:sub": identity}},
        }],
    }
    iam.update_assume_role_policy(RoleName=ROLE, PolicyDocument=json.dumps(policy))
    print(f"trust updated: accounts.google.com sub = {identity}")


ap = argparse.ArgumentParser()
sub = ap.add_subparsers(dest="cmd", required=True)
c = sub.add_parser("create"); c.add_argument("--bucket", required=True)
t = sub.add_parser("trust")
t.add_argument("--identity", required=True)
args = ap.parse_args()
create(args.bucket) if args.cmd == "create" else trust(args.identity)
