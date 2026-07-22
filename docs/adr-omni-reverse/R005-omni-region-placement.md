# ADR-R005: Place Omni-consumed data in an Omni-supported region

**Status:** Accepted · **Date:** 2026-07-20

## Context

BigQuery Omni does not run in every AWS region. At build time it covered
`us-east-1`, `us-west-2`, `eu-west-1`, `eu-central-1`, `ap-northeast-2`,
`ap-southeast-2`.

This is not a soft preference — it gates the entire approach. Our other POC data
lived in **`us-east-2`, which Omni does not cover**, so that data was simply
unreachable by Omni; a fresh table had to be written to `us-east-1`. There is no
config flag or workaround: wrong region → no Omni.

The region also determines the BigQuery location string (`aws-<region>`, e.g.
`aws-us-east-1`) for the connection and dataset, and it is where the scan is
billed.

## Decision

**Any S3 data intended for Omni consumption must live in an Omni-supported
region. Verify the region first — before writing the table, creating the
connection, or planning the pipeline.**

If authoritative data already sits in an unsupported region (e.g. `us-east-2`),
either (a) locate the Omni-consumed Iceberg copy in a supported region, or (b)
fall back to copying into GCS ([R001](R001-omni-read-in-place-over-copy.md)) —
do not assume Omni can reach it.

## Consequences

- Region selection becomes the **first** checklist item for the reverse leg, not
  an afterthought discovered at query time.
- The Terraform module surfaces this: `aws_region` must be an Omni region, and
  the BigQuery location is derived from it
  ([R004](R004-terraform-control-plane-script-data-plane.md)).
- For data that cannot move to a supported region, the decision degrades
  gracefully to the copy-into-GCS path rather than failing silently.
- The supported-region list changes over time — re-check current BigQuery Omni
  documentation rather than trusting this snapshot.
