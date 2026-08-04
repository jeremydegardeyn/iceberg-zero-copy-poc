"""Does a VPC-endpoint-scoped S3 bucket policy let BigQuery Omni through?

Two forms of the same hardening control, with opposite outcomes:

  loose   Deny unless the request arrived via SOME VPC endpoint
          Condition: {"Null": {"aws:SourceVpce": "true"}}
          -> Omni PASSES. Proves Omni's S3 reads do not cross the public internet.

  pinned  Deny unless the request arrived via ONE NAMED endpoint (the common
          enterprise standard, pinning the org's own endpoint id)
          Condition: {"StringNotEquals": {"aws:SourceVpce": "vpce-..."}}
          -> Omni is BLOCKED. Omni arrives via Google's endpoint, not yours, and
             you cannot discover or name it in your own account.

Both runs apply a bucket policy and remove it again in a finally block. The
bucket is left with no policy, which is how the POC keeps it.

  python omni_vpce_policy_test.py --bucket B --table proj.ds.tbl --mode loose
  python omni_vpce_policy_test.py --bucket B --table proj.ds.tbl --mode pinned \
      --vpce vpce-0123456789abcdef0

The probe query must force a real object read. COUNT(*) is answered from Iceberg
metadata, bills 0 bytes, and never touches S3 -- it will pass either policy and
tell you nothing.
"""
import argparse
import json
import subprocess

import boto3


def probe(table, location):
    """Run a scan-forcing Omni query. Returns (ok, message)."""
    q = f"SELECT COUNT(DISTINCT payload) FROM `{table}`"
    r = subprocess.run(
        ["bq", "query", f"--location={location}", "--use_legacy_sql=false",
         "--format=json", q],
        capture_output=True, text=True, shell=True,
    )
    return r.returncode == 0, (r.stdout if r.returncode == 0 else r.stderr).strip()


def policy_for(mode, bucket, vpce):
    cond = ({"Null": {"aws:SourceVpce": "true"}} if mode == "loose"
            else {"StringNotEquals": {"aws:SourceVpce": vpce}})
    return {"Version": "2012-10-17", "Statement": [{
        "Sid": "VpceScopedRead",
        "Effect": "Deny",
        "Principal": "*",
        "Action": ["s3:GetObject", "s3:ListBucket"],
        "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
        "Condition": cond,
    }]}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--table", required=True, help="project.dataset.table on the Omni connection")
    p.add_argument("--mode", choices=["loose", "pinned"], required=True)
    p.add_argument("--vpce", default="vpce-0123456789abcdef0",
                   help="endpoint id to pin; any well-formed id demonstrates the control")
    p.add_argument("--location", default="aws-us-east-1")
    a = p.parse_args()

    s3 = boto3.client("s3")
    try:
        s3.get_bucket_policy(Bucket=a.bucket)
        raise SystemExit(f"{a.bucket} already has a bucket policy -- refusing to overwrite it")
    except s3.exceptions.from_code("NoSuchBucketPolicy"):
        pass

    pol = policy_for(a.mode, a.bucket, a.vpce)
    print(f"applying {a.mode} policy:", json.dumps(pol["Statement"][0]["Condition"]))
    s3.put_bucket_policy(Bucket=a.bucket, Policy=json.dumps(pol))
    try:
        # control: our own credentials arrive over the internet, so they must be denied
        try:
            s3.list_objects_v2(Bucket=a.bucket, MaxKeys=1)
            print("control: our credentials SUCCEEDED -- policy is not in effect, results are void")
        except Exception as e:
            print("control: our credentials DENIED --", type(e).__name__)

        ok, msg = probe(a.table, a.location)
        print("omni:", "SUCCEEDED" if ok else "BLOCKED")
        print(" ", msg[:400])
    finally:
        s3.delete_bucket_policy(Bucket=a.bucket)
        print("policy removed; bucket restored to no-policy")

    ok, _ = probe(a.table, a.location)
    print("baseline after removal:", "works" if ok else "STILL BROKEN -- investigate")


if __name__ == "__main__":
    main()
