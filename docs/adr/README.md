# Architecture Decision Records

Format: lightweight MADR. Status lifecycle: Proposed → Accepted → Superseded.

| # | Decision | Status |
|---|---|---|
| [0001](0001-iceberg-as-sharing-format.md) | Apache Iceberg as the cross-cloud table format | Accepted |
| [0002](0002-zero-copy-federation-over-replication.md) | Zero-copy catalog federation over replication | Accepted |
| [0003](0003-biglake-metastore-as-catalog.md) | BigLake (Lakehouse) metastore as the Iceberg catalog | Accepted |
| [0004](0004-keyless-auth-wif-and-vending.md) | Keyless auth: workload identity federation + credential vending | Accepted |
| [0005](0005-single-writer-read-only-consumers.md) | Single-writer on GCP; read-only consumers | Accepted |
| [0006](0006-accept-per-query-egress.md) | Accept per-query egress with a measured break-even review | Accepted |
| [0007](0007-direct-write-over-replication.md) | Direct-write to the consumer cloud; replicate only as a retrofit | Accepted |

**Reverse leg (BigQuery Omni):** the decisions for reading S3-resident Iceberg
from GCP live in a separate set — [`../adr-omni-reverse/`](../adr-omni-reverse/)
(`R001`–`R005`).
