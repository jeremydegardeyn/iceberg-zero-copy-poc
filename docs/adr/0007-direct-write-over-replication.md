# ADR-0007: Direct-write to the consumer cloud; replicate only as a retrofit

**Status:** Accepted · **Date:** 2026-07-16

## Context

ADR-0002 framed the choice as zero-copy federation (A) vs replication (C), and
ADR-0006 made the trigger economic: replicate a table when its monthly scanned
bytes materially exceed its size. Building and running both paths surfaced two
facts that reframe the decision.

**1. Federation has a hard consumer ceiling, not just a cost curve.** Athena
and Redshift cannot read `gs://` at all — Glue accepts a Table with
`metadata_location = gs://…` without validating it, then Athena fails at query
time with `Wrong scheme for S3 location`. Only engines with a pluggable
filesystem (Snowflake, Spark, Trino, Flink) can federate against GCS. So for an
Athena/Redshift consumer, replication is *mandatory*, not economic.

**2. Replication is strictly worse than never having written to the wrong
cloud.** Compare, for a feed whose consumers are on AWS:

| | egress | storage | moving parts | staleness |
|---|---|---|---|---|
| Ingest→GCS, then replicate | 1× data volume | 2× | pipeline + Spark rewrite + copy plan + scheduler + Glue registration | sync interval |
| Ingest→S3 directly | 1× data volume | 1× | pipeline | none |

The same bytes cross the cloud boundary exactly once either way. Replication
buys nothing on transfer cost and pays for a second copy plus an entire second
system — including a Spark dependency (`rewrite_table_path` is a Spark
procedure; Beam cannot call it), so an all-Dataflow shop drags in a second
compute paradigm purely to undo a placement decision.

## Decision

**If a feed's consumers are on AWS, write Iceberg directly to S3 + Glue from
the ingestion pipeline.** The Beam managed Iceberg sink takes the catalog as
configuration, so this is a parameter (`--catalog_type glue`), not a second
pipeline.

### Tested 2026-07-16 — architecture confirmed, credential plumbing unsolved

A Dataflow batch job was run with `--catalog_type glue` against
`s3://…/direct` + a Glue database. Result:

- ✅ Beam's managed Iceberg **does** bundle `iceberg-aws`: `GlueCatalog`
  resolved, the pipeline validated, workers started, rows were written to files.
- ❌ It failed handing static credentials to the Glue client:
  `IllegalArgumentException: Cannot create an instance of
  software.amazon.awssdk.auth.credentials.StaticCredentialsProvider, it does
  not contain a static 'create' or 'create(Map<String, String>)' method`
  (`AwsClientProperties.credentialsProvider`).

Iceberg instantiates `client.credentials-provider` **reflectively**, requiring
a no-arg `create()` or `create(Map)`. `S3FileIO` accepts raw keys
(`s3.access-key-id`/`s3.secret-access-key`), but the *Glue client* has no
equivalent — it only takes a provider class.

Instructively, the AWS SDK providers that *do* fit that signature are the
keyless ones — `WebIdentityTokenFileCredentialsProvider.create()`,
`DefaultCredentialsProvider.create()`. **Iceberg's configuration surface
actively resists the static-key shortcut.** ADR-0004 was right and the
shortcut is what broke.

Remaining fix paths, in preference order:

1. **AWS workload identity federation** (keyless, correct): a GCP service
   account assuming an AWS role via OIDC, surfaced to
   `WebIdentityTokenFileCredentialsProvider`. Open problem: it needs a token
   file and `AWS_ROLE_ARN` in the **Java** SDK-harness container, and Beam does
   not expose harness environment from a Python pipeline.
2. A custom credentials-provider class exposing `create(Map)`, added to the
   expansion classpath — solves it, but ships a jar.
3. Write with a filesystem catalog and register in Glue post-hoc (the pattern
   in `scripts/12_glue_athena_register.py`), splitting write from registration.

So the *decision* below stands on its cost and consumer-reach arguments, but
the "it's just a parameter" claim is **not yet true for Dataflow→Glue**;
budget for path 1 or 2 before relying on it.

**Replication (ADR-0002 option C) is retained strictly as a retrofit**, for:

- data that is *already* in GCS (backfills, existing estates, migrations),
- feeds where GCS must remain authoritative (GCP-native consumers, residency,
  regulatory single-source-of-truth) **and** AWS consumers also need the data.

In that second case the alternative is dual-write (one pipeline, two sinks).
We prefer replication there, because a replica is **byte-identical and provably
derived** from an authoritative snapshot, whereas dual-write produces two
independent tables that *should* agree with nothing guaranteeing it. "Is the
AWS copy the same data?" gets a rigorous answer instead of a hopeful one — worth
the Spark step when someone will be asked to attest to it.

## Consequences

- Zero-copy federation (ADR-0002 option A) remains the default for
  Snowflake/Spark/Trino consumers and for the long tail; nothing here changes it.
- Feed placement becomes a design-time question — *where do this feed's
  consumers live?* — rather than a cost cleanup discovered at the 30-day review.
- Cross-cloud writes need AWS credentials on GCP compute. The keyless answer is
  AWS workload identity federation (a GCP service account assuming an AWS role
  via OIDC), mirroring ADR-0004's Snowflake→GCP trust. **This POC takes a
  documented shortcut**: a static AWS key in Secret Manager, read by the
  launcher and passed as Iceberg catalog properties — never as a flex-template
  parameter, which would expose it in the job description. That shortcut is an
  ADR-0004 violation and is the first thing to fix before production.
- Splitting feeds across clouds by consumer means the lake is no longer in one
  place. Catalog-of-catalogs discovery and per-feed governance become real
  concerns at estate scale.
- At PB scale this decision makes itself: ~$120k of egress per petabyte means
  nobody replicates routinely — you place the data correctly the first time.

## Alternatives considered

- **Replicate everything to AWS anyway** (uniformity): pays double storage and
  runs a second system for feeds that never needed to be on GCP.
- **Dual-write everywhere**: no staleness and no Spark, but two independently
  produced tables with no derivation guarantee, and doubled maintenance
  (compaction, expiry) per feed.
- **Federate everything and accept the Athena/Redshift gap**: only viable if the
  consumer list is permanently Snowflake/Spark/Trino — a bet on the consumer
  roadmap that costs a full replatform if it's wrong.
