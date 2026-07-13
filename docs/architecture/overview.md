# Architecture Overview

Diagrams for ARB review. Mermaid sources render natively on GitHub. See `docs/adr/` for the decisions behind each element.

## 1. System context

```mermaid
flowchart LR
    subgraph GCP["GCP (data owner)"]
        BQ["BigQuery / Spark<br/>producers"]
        CAT["Lakehouse runtime catalog<br/>(BigLake metastore)<br/>Iceberg REST endpoint"]
        GCS[("GCS bucket<br/>Iceberg tables<br/>(single source of truth)")]
        BQ -->|read/write| CAT
        BQ -->|data files| GCS
        CAT -.->|tracks metadata pointers| GCS
    end

    subgraph AWS["AWS (consumer)"]
        SF["Snowflake account<br/>catalog-linked database"]
        WH["Virtual warehouses"]
        SF --> WH
    end

    SF -->|"Iceberg REST (HTTPS)<br/>metadata + auth"| CAT
    WH -->|"Parquet byte-range reads<br/>(cross-cloud egress $)"| GCS
```

Data never leaves GCS. Snowflake discovers tables through the REST catalog and scans Parquet in place.

## 2. Authentication & query sequence

```mermaid
sequenceDiagram
    autonumber
    participant SF as Snowflake (AWS)
    participant STS as Google STS
    participant IRC as Lakehouse Iceberg REST catalog
    participant GCS as GCS bucket

    Note over SF,STS: Workload identity federation — no stored keys
    SF->>STS: Exchange Snowflake OIDC token (TOKEN_EXCHANGE)
    STS-->>SF: Short-lived GCP access token
    SF->>IRC: GET namespaces/tables (x-goog-user-project header)
    IRC-->>SF: Table list + current metadata pointers
    SF->>IRC: LoadTable (X-Iceberg-Access-Delegation: vended-credentials)
    IRC-->>SF: metadata.json location + vended, scoped GCS credentials
    SF->>GCS: Read manifests + pruned Parquet byte ranges
    GCS-->>SF: Data (egress metered as DATA_LAKE)
    Note over SF: Query result reflects latest committed snapshot
```

## 3. Deployment / trust boundaries

```mermaid
flowchart TB
    subgraph TB1["Trust boundary: GCP project"]
        WIP["Workload identity pool<br/>+ OIDC provider<br/>(trusts Snowflake issuer)"]
        IAMV["IAM: biglake.viewer<br/>(Snowflake principal, read-only)"]
        RSA["Catalog runtime SA<br/>storage.objectUser on bucket"]
        CAT2["Iceberg REST catalog"]
        GCS2[("GCS: iceberg-poc bucket<br/>namespace shared_aws only")]
    end
    subgraph TB2["Trust boundary: Snowflake (AWS)"]
        CI["Catalog integration<br/>(OAUTH TOKEN_EXCHANGE)"]
        CLD["Catalog-linked DB<br/>(read-only grants to consumers)"]
        RBAC["Snowflake RBAC / policies<br/>layered on shared tables"]
    end
    CI -->|OIDC token| WIP
    WIP --> IAMV
    IAMV --> CAT2
    CAT2 -->|vended scoped creds| CI
    RSA --> GCS2
    CLD --> RBAC
```

Control points: the shared namespace is the contract boundary; `biglake.viewer` caps the blast radius at read-only; credential vending removes standing storage credentials from the AWS side.

## 4. Consumption-pattern decision

```mermaid
flowchart TD
    Q{AWS consumer?} -->|Snowflake / Spark / Trino<br/>IRC-capable| A["Zero-copy federation<br/>(this design)"]
    Q -->|Athena / Redshift /<br/>files must be in S3| C["Scheduled replica:<br/>STS sync + rewrite_table_path<br/>+ register_table in Glue"]
    A --> M{Monthly scanned bytes<br/>≫ table size?}
    M -->|No| A2["Stay zero-copy"]
    M -->|Yes, per hot table| C2["Hybrid: replicate hot tables,<br/>federate the long tail"]
```
