# Omni vs MFT — Measured Cost Findings

**Executed 2026-07-25.** Real workload (daily snapshot diff, delta-only output),
real data (1.391 GB across two daily snapshots), real billing systems. Both
approaches read the **same S3 Parquet files** and compute the **same diff**.

Reproduce: [`scripts/omni_vs_mft_daily_diff_benchmark.py`](../scripts/omni_vs_mft_daily_diff_benchmark.py)

## Headline: cost is NOT a wash — MFT is ~2x cheaper on infrastructure

| | Omni (tuned) | MFT (Athena + egress) |
|---|---|---|
| scan / compute | $0.015274 | $0.008159 |
| cross-cloud move | $0.002423 | $0.000515 |
| load into BigQuery | $0 (n/a) | $0 (load jobs are free) |
| **TOTAL per run** | **$0.017697** | **$0.008673** |

**Omni costs 2.04x MFT**, after tuning Omni to its best case. Claiming parity
would not survive scrutiny.

## But the absolute gap is small relative to labour

Scaling linearly in bytes scanned, at daily cadence:

| Daily volume | MFT/yr | Omni/yr | Gap/yr | Gap vs 2 hrs/mo of eng time ($2,400/yr) |
|---|---|---|---|---|
| 10 GB | $23 | $46 | **$24** | 1% |
| 100 GB | $228 | $464 | **$237** | 10% |
| 500 GB | $1,138 | $2,321 | **$1,184** | 49% |
| 1 TB | $2,275 | $4,642 | **$2,367** | 99% |
| 5 TB | $11,375 | $23,211 | **$11,836** | 4.9x |

**Below ~500 GB/day the Omni premium costs less than the labour of owning an
extract job.** Above ~1 TB/day it stops being negligible and needs a real
justification.

## Why Omni costs more — two proven root causes

**1. Omni bills UNCOMPRESSED logical bytes; Athena bills COMPRESSED bytes read.**

Proven directly: `SELECT SUM(order_id)` over 30,000,000 rows billed
**240,123,904 bytes** — exactly 30M x 8 bytes, the uncompressed int64 size.
Snappy Parquet compressed ~2.8x here, so Omni pays ~2.8x the bytes Athena does
for the same scan. **Better compression makes Omni relatively worse.**

**2. Higher unit rate.** $7.79/TiB (measured from the billing export, SKU
`D09B-1220-6F27`) = ~$7.09 per 10^12 bytes, vs Athena's published $5.00/TB —
a 42% premium.

## Omni cost is highly tunable (52% swing)

| Query shape | Bytes billed | Cost |
|---|---|---|
| naive: `UNION ALL` of two LEFT JOINs, hash all columns | 4,477,419,520 | $0.031722 |
| single `FULL OUTER JOIN`, hash all columns | 3,997,171,712 | $0.028320 (-11%) |
| single `FULL OUTER JOIN`, **hash business columns only** | 2,155,872,256 | **$0.015274 (-52%)** |

Column pruning works and is the dominant lever — the bulk `payload` blob was
most of the bill. **Never hash a blob column in an Omni diff.** `COUNT(*)`
billed 0 bytes (metadata only).

## What is NOT in these numbers

Excluded from MFT (all of which favour Omni): MFT licensing/servers/ops,
S3 storage for the extract, GCS landing storage, and **datalake-team labour to
build, own, and modify the extract job for every new requirement**.

Excluded from Omni: nothing material at this scale.

## Honest conclusion

Cost should not decide this. At realistic volumes (<=500 GB/day) the
infrastructure gap is a few hundred dollars a year — smaller than the labour it
displaces — but it is a **gap, not parity**, and it grows linearly. The decision
belongs on operating model:

- **Omni** — datalake team grants a read-only IAM role **once**; the GCP team
  self-serves every subsequent question in SQL.
- **MFT** — datalake team owns a delta-extract job **in perpetuity** and fields
  a ticket for every new field, filter, or table.

Lead with that. Concede the cost point; it is worth ~$237/yr at 100 GB/day.
