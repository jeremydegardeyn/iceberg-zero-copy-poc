"""Execute the copy plan emitted by rewrite_table_path: GCS sources -> S3 targets.

The file list is a CSV of (source_path, target_path) pairs — data files copied
as-is, metadata files from the rewritten staging area. Reads GCS via gsutil
(user ADC), writes S3 via boto3 (standard AWS credential chain: env vars or
~/.aws/credentials — configured by the user, never stored in this repo).

Usage: python s3_copy_filelist.py --file_list gs://.../file-list-... [--dry_run]
"""
import argparse
import csv
import io
import subprocess
import sys

import boto3


def gsutil_cat(path: str) -> bytes:
    r = subprocess.run(["gsutil", "cat", path], capture_output=True,
                       shell=(sys.platform == "win32"))
    if r.returncode != 0:
        raise RuntimeError(f"gsutil cat {path}: {r.stderr.decode()[:300]}")
    return r.stdout


def main() -> None:
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--file_list", required=True, help="gs:// path of the copy plan CSV")
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    rows = list(csv.reader(io.StringIO(gsutil_cat(args.file_list).decode("utf-8"))))
    pairs = [(r[0].strip(), r[1].strip()) for r in rows if len(r) >= 2]
    print(f"{len(pairs)} files in copy plan")

    s3 = boto3.client("s3")
    for i, (src, dst) in enumerate(pairs, 1):
        assert dst.startswith("s3://"), dst
        bucket, _, key = dst[5:].partition("/")
        if args.dry_run:
            print(f"  [{i}/{len(pairs)}] DRY {src} -> {dst}")
            continue
        s3.put_object(Bucket=bucket, Key=key, Body=gsutil_cat(src))
        print(f"  [{i}/{len(pairs)}] {src} -> {dst}")
    print("copy plan complete" + (" (dry run)" if args.dry_run else ""))


if __name__ == "__main__":
    main()
