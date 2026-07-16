# Is this production-capable, or POC-only?

The claim to evaluate: *"these approaches have flaws — fine for a POC, not for
production."* The honest verdict: the claim conflates two different things.
**POC implementation shortcuts** (real, enumerated below, each with a known
fix) and **the architectural pattern** (production-proven, including at a
large regulated bank running the same design for Zelle payment data). Every
objection below is answered with evidence from this POC's measured runs or
from GA/production references — not vendor slideware.

## The strongest form of each objection, answered

### 1. "Per-query cross-cloud egress is an uncontrolled cost"

It is a *metered, governed* cost. Egress lands in
`SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY` (`transfer_type='DATA_LAKE'`)
from the first query; ADR-0006 mandates a 30-day per-table break-even review
(monthly scanned bytes vs table size), and the escape valve for tables that
fail it is **built and tested in this repo** (the S3 replica path — one sync
cost, then intra-region reads). Iceberg pruning means consumers fetch Parquet
byte ranges, not tables, so producer-side partitioning is a cost control
lever. Production adds resource monitors + an egress dashboard — configuration,
not architecture. An uncontrolled cost is a flaw; a measured cost with a
documented break-even rule and a tested fallback is a *cost model*.

### 2. "You measured 1–10 min Snowflake visibility against a nominal 30 s — that breaks SLAs"

We measured it precisely so nobody promises what the system doesn't do —
that's an argument *for* this POC's discipline, not against the pattern.
ADR-0002 scopes this design to **analytics-grade consumption** and explicitly
excludes serving paths. For BI/reporting/regulatory workloads, minutes-level
freshness beats most replication pipelines (which add pipeline lag *plus*
divergence risk). A use case needing sub-minute reads should never have been
put on cross-cloud federation — that's scope discipline, not a flaw.

### 3. "The streaming writer had a 1-hour auth token cliff — that's not production"

It *did* — in Google's documented sample. This POC eliminated it: the managed
Iceberg sink runs with Iceberg's `GoogleAuthManager` (auto-refreshing ADC
credentials, the same mechanism any production Spark/Flink writer uses), and
it is running proof, not a proposal. Independent of that, the writer choice is
swappable without touching the architecture: self-managed Flink or Spark
Structured Streaming — standard at any bank — write to the same REST catalog
with the same auth. The pattern doesn't depend on Beam's managed connector
maturing.

### 4. "No row/column-level security on catalog-managed shared tables"

True, and designed for rather than discovered: ADR-0005 shares only a curated
namespace (the published contract), and consumer-side governance layers
Snowflake RBAC, dynamic masking, and PII policies on top of the catalog-linked
database. This is **exactly the production posture Huntington's EDW team
presented publicly** for externally-governed Iceberg tables read by Snowflake
(dynamic data masking, RBAC, centralized access control without moving data).
If per-row entitlements inside the shared layer are a hard requirement, that
workload belongs in a consumer-side materialization — which the replica path
provides.

### 5. "You're coupled to Google's catalog"

Control-plane only. The data files are open Parquet + Iceberg metadata in
GCS; the catalog is swappable via Iceberg's `register_table` (to self-hosted
Polaris/Nessie or any IRC implementation) without touching a byte of data —
this POC's replica path literally demonstrates re-registering the same table
under a different catalog on a different cloud. Compare the alternative:
vendor-native sharing locks the *contract* (Snowflake shares are
Snowflake-only forever). Choosing a managed catalog with an exit is the
lower-coupling option on the table.

### 6. "The POC hit six operational failures in one afternoon"

And fixed all six, in the repo, with the production-grade version of each fix
(see as-run.md): event-driven archive instead of polling, regional placement +
machine-family diversity for capacity, JVM-enabled launcher images, mandatory
delegation headers, drain-vs-cancel semantics. None of the six is a property
of the architecture — they are integration engineering, which is what a POC
is *for*. A one-day run that produced a documented failure catalog with fixes
is the strongest possible evidence that the remaining hardening is enumerable,
not open-ended. The genuinely open items are listed at the bottom — there are
three, and none is architectural.

### 7. "The replica path is manual and stale"

Stale **by design** — it exists only for tables where the break-even math says
a copy is cheaper, and staleness tolerance is part of that same decision. The
manual steps in the runbook are one Cloud Scheduler + Cloud Run job away from
scheduled refreshes, and `rewrite_table_path` supports incremental mode so
refreshes copy deltas, not tables. Note what the replica does *not*
reintroduce: bespoke pipelines per feed (it's one generic mechanism, any
Iceberg table), divergence ambiguity (the validation harness reconciles
source/lake/consumer and demonstrably caught real drift on its first run).

### 8. "This is all GA'd-last-quarter tech"

The load-bearing components: Apache Iceberg (years in production at Netflix,
Apple, LinkedIn scale), Snowflake catalog-linked databases (GA), Snowflake
external volumes + object-store catalog integrations (GA, years old), workload
identity federation (GA, the standard keyless pattern), Dataflow/Beam (a
decade old). The newest piece is BigLake's Iceberg REST endpoint with the
GA Snowflake integration (June 2026) — and a large regulated bank presented
the equivalent production architecture (Glue catalog + Snowflake, GCP Pub/Sub
sources, Iceberg lake) at a 2026 industry expo, in production for payment
data. The pattern is not novel; this POC's variant (lake on the producer
cloud) is a topology choice with a measured trade-off, not an experiment.

## What actually remains before production (the honest list)

1. **Freshness SLO validation at scale** — the 1–10 min CLD refresh variance
   was measured on a trial account with two tables; measure on a real account
   under real load before signing any SLA.
2. **Egress governance build-out** — resource monitors, the transfer-cost
   dashboard, and alerting exist as designs (ADR-0006), not yet as deployed
   config.
3. **Ops packaging** — scheduled validation (Cloud Run job), scheduled replica
   refresh, drain-based streaming relaunch automation, and Terraform adoption
   of the live resources (import plan in terraform/README.md).

All three are weeks of engineering with no research risk. That is the
difference between "flawed for production" and "not yet productionized" — and
it's a difference worth being precise about.
