"""AWS IAM role for BigQuery Omni to assume (web-identity federation via
accounts.google.com) with read access to the Omni S3 bucket.

  create        -> role with placeholder trust + S3 read policy; prints role ARN
  trust         -> tighten trust to the BQ connection's identity (sub)
  grant-write   -> add a scoped s3:PutObject policy (for EXPORT DATA to S3)
  revoke-write  -> remove it, restoring least-privilege read-only
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


def grant_write(bucket, prefix):
    """Scoped write for `EXPORT DATA` — PutObject on the export prefix only."""
    iam = boto3.client("iam")
    iam.put_role_policy(RoleName=ROLE, PolicyName="s3-export-write", PolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:PutObject", "s3:DeleteObject"],
            "Resource": f"arn:aws:s3:::{bucket}/{prefix}*",
        }],
    }))
    print(f"granted s3-export-write on s3://{bucket}/{prefix}*")


def revoke_write():
    iam = boto3.client("iam")
    try:
        iam.delete_role_policy(RoleName=ROLE, PolicyName="s3-export-write")
        print("revoked s3-export-write (role back to read-only)")
    except iam.exceptions.NoSuchEntityException:
        print("s3-export-write not present")


ap = argparse.ArgumentParser()
sub = ap.add_subparsers(dest="cmd", required=True)
c = sub.add_parser("create"); c.add_argument("--bucket", required=True)
t = sub.add_parser("trust"); t.add_argument("--identity", required=True)
g = sub.add_parser("grant-write"); g.add_argument("--bucket", required=True); g.add_argument("--prefix", default="exports/")
sub.add_parser("revoke-write")
args = ap.parse_args()
if args.cmd == "create":
    create(args.bucket)
elif args.cmd == "trust":
    trust(args.identity)
elif args.cmd == "grant-write":
    grant_write(args.bucket, args.prefix)
elif args.cmd == "revoke-write":
    revoke_write()
