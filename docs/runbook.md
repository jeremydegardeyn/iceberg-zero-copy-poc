# Runbooks

Two consumption paths, one producer-side catalog:

| Path | Runbook | When |
|---|---|---|
| **Zero-copy federation** (default) | [runbook-zero-copy.md](runbook-zero-copy.md) | IRC-capable consumers; freshness inherent; pay per-query egress |
| **S3 replica** (ADR-0002 option C) | [runbook-s3-replica.md](runbook-s3-replica.md) | Hot tables past the ADR-0006 break-even, or S3-only consumers; pay once per sync, reads intra-region |

Not sure which path a given table belongs on? Start with the
[decision tree](zero-copy-decision-tree.md).

The streaming/batch ingestion extension and the integrity-control harness are
documented in [as-run.md](as-run.md). Production-readiness argument:
[production-readiness.md](production-readiness.md).
