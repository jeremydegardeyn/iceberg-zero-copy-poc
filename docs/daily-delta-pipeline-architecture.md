# Daily Cross-Cloud Delta Pipeline — Architecture Recommendation

A worked solution design that applies this repo's cross-cloud findings (BigQuery
Omni, Iceberg snapshot diffs, the AlloyDB FDW) to a concrete requirement:
detect day-over-day changes in an **AWS** data lake and serve the results from
**GCP**.

## The use case

```
source systems -> ingest to AWS data lake ->
daily delta (changes since prior day) -> persist for next-day compare ->
apply business logic + config rules (GCP-side) -> enrich with other sources ->
store output in GCP -> publish to the enterprise event hub (EEH) ->
consumers (ServiceNow, analytics, downstream apps)
```

Requirements: day-over-day comparison ("CDC", though it is really snapshot-diff
delta detection); **minimize data movement**; persistent storage in **GCP**
(open to BigQuery); **CRUD** support; publish to **EEH**; deliver to
**ServiceNow**; run **daily in ≤ 30 minutes with no single point of failure**.

## Governing principle

*Minimize data movement* and *store in GCP* conflict: the source is in AWS, the
system of record is in GCP, so **some cross-cloud movement is unavoidable**.
"Minimize" therefore resolves to one rule:

> **Move only the daily delta, not the full snapshot — and compute that delta
> where the data already lives (AWS).**

If the diff runs in GCP you must ship the full snapshot across every day, which
is the exact thing to avoid. If the AWS lake is **Iceberg**, "compare to prior
day" is a comparison of two snapshots (time-travel) — you may not need to persist
yesterday's copy at all.

## Reference architecture

```mermaid
flowchart LR
  subgraph AWS[AWS - source cloud]
    SRC[Source systems] --> ING[Ingest to Iceberg lake]
    ING --> LAKE[(Iceberg lake<br/>S3)]
    LAKE --> DIFF[[Daily delta<br/>BigQuery Omni<br/>compute in AWS]]
  end
  DIFF -->|delta only<br/>FDW pull or load| ADB
  subgraph GCP[GCP - system of record]
    ADB[(AlloyDB<br/>pull delta + business logic<br/>config CRUD + serve)]
    BQ[(BigQuery<br/>optional: heavy analytics)]
    ADB -.->|analytical copy| BQ
    ADB --> PUB[[Cloud Run or Dataflow<br/>publish change events]]
    PUB --> EEH([Pub/Sub<br/>EEH])
  end
  EEH --> SNOW[ServiceNow]
  EEH --> ANL[Analytics]
  EEH --> APPS[Downstream apps]
  ORCH[[Cloud Workflows + Scheduler<br/>daily, 30 min or less, no SPOF]] -.orchestrates.-> DIFF
  ORCH -.-> ADB
  ORCH -.-> PUB
```

| Flow step | Tool | Notes |
|---|---|---|
| Ingest to AWS lake | AWS-native (as-is) | Land as **Iceberg** — makes the diff cheap |
| Daily delta / "CDC" | **BigQuery Omni**, in AWS | Compute the day-over-day diff in place; emit only the delta. Iceberg snapshot compare if available |
| Persist for next-day compare | S3 (Iceberg history) | No extra copy if Iceberg; else a dated snapshot in S3 |
| Move delta to GCP | FDW pull (small delta) or CTAS + bulk load (large) | One hop, delta-sized |
| Business logic + config rules | **AlloyDB** SQL / PL-pgSQL | Native CRUD joins to admin/config tables |
| Enrich with other sources | **AlloyDB** joins | Join the delta to reference/master data held in AlloyDB |
| Store output + CRUD | **AlloyDB** (system of record) | Postgres CRUD; HA = no SPOF. BigQuery only if heavy BI (analytical copy) |
| Publish to EEH | **Cloud Run job** (or Dataflow) → Pub/Sub | Read AlloyDB result table, one change-event per row |
| ServiceNow + consumers | Subscribe to the EEH | Decouple ServiceNow behind Pub/Sub |
| Orchestration | **Cloud Workflows + Scheduler** (or Composer) | All components serverless / no SPOF |

## Where BigQuery Omni fits

- **As the diff-pushdown engine: yes.** It runs the day-over-day scan inside AWS
  and returns only the delta — directly serving "minimize movement," in BigQuery
  SQL owned by the GCP team. (See [runbook-omni-reverse.md](runbook-omni-reverse.md).)
- **As the platform/store/serving/CRUD layer: no.** Read-only, region-locked,
  analytics-grade, can't do CRUD, can't feed Pub/Sub or ServiceNow. It is a
  query tool, not a system of record. See
  [adr-omni-reverse/](adr-omni-reverse/) for the proven limits.

## Decision log

### D1 — Delta-compute engine → BigQuery Omni

**Status: DECIDED — BigQuery Omni.** The diff runs in AWS via Omni (SQL
pushdown), so only the delta crosses to GCP, in a BigQuery control plane owned by
the GCP team. Accept Omni's constraints (region-locked, read-only,
analytics-grade, no DML/ML/streaming) — they don't bind the *diff* step, which is
a batch `SELECT`. See [runbook-omni-reverse.md](runbook-omni-reverse.md).

### D2 — GCP-side store → AlloyDB (because they need CRUD)

**Status: DECIDED (pending volume check) — AlloyDB as the system of record.**
CRUD is a hard requirement and BigQuery is not an OLTP/CRUD store, so AlloyDB is
the GCP-side store, transform engine, and serving layer. It pulls the Omni delta
via the `bigquery_fdw` ([verified](adr-omni-reverse/R003-materialize-for-native-consumers.md)),
applies business logic + config-rule joins in SQL, holds the output, and serves
CRUD — one engine instead of a BigQuery+AlloyDB hybrid.

Two guardrails:

- **Delta volume vs FDW throughput.** The FDW's query mode pulls results via
  serial `getQueryResults` pagination — fine for a modest daily delta, a
  bottleneck for a large one. For big deltas, have Omni **materialize the delta to
  a native BigQuery table** and **bulk-load AlloyDB (`COPY`)** instead of pulling
  through the FDW. Confirm the daily delta size to pick the path.
- **Heavy BI.** If "analytics" means real warehouse-scale scans, keep **BigQuery**
  as an optional analytical copy alongside AlloyDB — not on the operational path.

### D3 — Pipeline shape: orchestrated-with-Omni vs single-engine-without-Omni

**Status: DECIDED — orchestrated, because Omni is required (D1).** Omni is a SQL
query step, not a Beam/Spark source, so "use Omni" and "one Dataflow/Spark job"
do not coexist:

- **(a) Orchestrated, with Omni (chosen):** Omni diff → land delta → AlloyDB
  logic/CRUD → publisher → Pub/Sub, sequenced by Cloud Workflows. Multiple
  serverless steps, Omni included.
- **(b) Single Spark/Dataflow job, no Omni:** read the S3 Iceberg delta
  **directly** (S3/Iceberg connector, bypassing Omni) → transform → AlloyDB +
  Pub/Sub. One job, but Omni is out — and Spark/Dataflow can't read Omni anyway
  (the spark-bigquery connector and `BigQueryIO` both use the Storage Read API,
  which Omni doesn't support). Keep (b) in reserve if the "single engine"
  constraint ever outweighs the "use Omni" one.

Note on "Dataflow reading Omni via AlloyDB": technically possible (Dataflow
`JdbcIO` → AlloyDB → FDW → Omni) and the only way to put Omni inside a Dataflow
read — but it chains three systems synchronously and bottlenecks on the FDW.
Prefer **staging**: AlloyDB lands the delta as a native table, then a Cloud Run
job (or Dataflow) reads *that* to publish.

## Non-functional notes

- **≤ 30 min / no SPOF:** the serverless components (Omni/BigQuery, Cloud Run,
  Pub/Sub, Workflows) are multi-zone with no single point of failure — BigQuery
  is effectively active-active across zones within a region (99.99% SLA; a region
  is the failure domain, cross-region needs opt-in managed DR). **AlloyDB is the
  one component you must configure for HA** — enable the regional
  primary + standby (automatic zonal failover); that is active-standby, not
  multi-master, but it satisfies no-SPOF. The remaining SPOF risk hides in the
  **fan-out**: use a Pub/Sub dead-letter topic, idempotent publish keyed by
  record id, and retries on the ServiceNow call.
- **"Lake → analytical → events" is not weird** — it is *compute deltas in batch,
  then emit them as discrete change events*. Standard integration shape.
- **EEH = Pub/Sub?** Probably. If your EEH is managed Kafka/Confluent or Azure
  Event Hubs, the fan-out target changes (Dataflow can write to Kafka), but the
  pattern is identical.

## Open upstream question

They call it CDC but it is **daily full-snapshot delta detection**. If the source
systems can emit real change logs (Datastream / Debezium / native CDC), you skip
the expensive daily full-snapshot compare entirely and stream only changes —
less movement and less cost. Worth asking before building the snapshot-diff.
