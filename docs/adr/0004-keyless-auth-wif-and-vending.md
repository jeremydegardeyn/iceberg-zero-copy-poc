# ADR-0004: Keyless auth — workload identity federation + credential vending

**Status:** Accepted · **Date:** 2026-07-13

## Context

Snowflake (AWS) must authenticate to the GCP catalog and read GCS objects. Options: long-lived service account keys, Snowflake external volumes (Snowflake-managed GCP service agent), or workload identity federation (WIF) with catalog credential vending.

## Decision

WIF for catalog auth (Snowflake OIDC token exchanged at Google STS via `TOKEN_EXCHANGE`), and catalog **credential vending** for storage access (short-lived, table-scoped GCS credentials delegated per request).

## Consequences

- No long-lived secrets anywhere: nothing to rotate, leak, or vault.
- Storage access is scoped per table and expires — blast radius of a compromised consumer session is minimal.
- GCP-side grant is a single read-only role (`roles/biglake.viewer`) on one principal; auditable in IAM.
- Fallback documented (runbook Phase 4): external volume + `storage.objectViewer` if vending fails for a given engine version.
- Operational note: the catalog's runtime service account requires an explicit `storage.objectUser` grant on the bucket — it is not automatic.

## Alternatives considered

- **Service account JSON keys:** rejected — standing credentials, rotation burden, exfiltration risk.
- **External volume only:** works, but grants bucket-wide standing read to the Snowflake service agent rather than per-table vended scope.
