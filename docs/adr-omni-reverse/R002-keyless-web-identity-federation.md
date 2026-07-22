# ADR-R002: Keyless web-identity federation over static AWS keys

**Status:** Accepted · **Date:** 2026-07-20

## Context

BigQuery Omni compute running in AWS must authenticate to S3. Two options:

- **Static AWS access keys** stored in GCP (e.g. in the connection config or a
  secret) — long-lived, must be rotated, and a leak grants standing S3 access.
- **Web-identity federation** — the AWS IAM role trusts a **Google identity**;
  Omni obtains a short-lived OIDC token from `accounts.google.com` and calls
  `sts:AssumeRoleWithWebIdentity` to get temporary credentials.

This is the same keyless posture the forward leg took for Snowflake→GCP
([ADR-0004](../adr/0004-keyless-auth-wif-and-vending.md)), applied in the
opposite direction (GCP→AWS).

Omni supports web-identity natively: creating the connection mints a numeric
OIDC subject, which you pin in the role's trust condition. Two operational
facts fell out of the build: the trust uses `accounts.google.com:sub`, and the
role needs a **12-hour** max session duration (Omni requests a 12h session, and
fails the query if the role caps it lower).

## Decision

**Authenticate Omni to S3 with web-identity federation, never a stored AWS
key.** The IAM role trusts `accounts.google.com` with
`sts:AssumeRoleWithWebIdentity`, conditioned on the connection's identity
(`sub`), scoped to a least-privilege read policy on the one bucket, with a 12h
max session duration.

## Consequences

- No secret to store, rotate, or leak; credentials are short-lived (≤ 12h).
- A clean **connection ↔ identity** dependency that Terraform can express by
  constructing the role ARN as a string (see
  [R004](R004-terraform-control-plane-script-data-plane.md)).
- One extra operational requirement to remember: the 12h `MaxSessionDuration`.
- Least privilege is enforced on the AWS side (read-only, single bucket), so the
  federated principal can do nothing beyond read the Iceberg data.
