-- BigQuery (NOT Snowflake): join an Omni external table with a native BigQuery
-- table via a cross-cloud join.
--
-- KEY: the native dataset must be in the BigQuery region COLOCATED with the Omni
-- region. For aws-us-east-1 that is us-east4 (N. Virginia) -- NOT us-east1.
-- A native dataset in the wrong region fails with:
--   "BigQuery Omni region aws-us-east-1 does not support exporting to us-east1."
--
-- The cross-cloud join runs in the colocated GCP region: BigQuery executes the
-- Omni side in AWS, transfers the (smaller) result to us-east4, and joins there.
-- (A join applies egress AND separately-billed transfer-compute AND
-- filtering-compute -- three meters, not one. See "Estimated cost" in
-- docs/runbook-omni-reverse.md.)
--
-- Proven 2026-07-21 against omni_s3.orders + omni_join_ref.customer_dim.

-- 1. Native dataset + table in the colocated region (us-east4):
--    bq mk --location=us-east4 --dataset <project>:omni_join_ref
CREATE OR REPLACE TABLE omni_join_ref.customer_dim AS
SELECT 'acme'       AS customer, 'enterprise' AS segment, 'NA' AS region UNION ALL
SELECT 'globex',     'mid-market', 'EU' UNION ALL
SELECT 'initech',    'smb',        'NA' UNION ALL
SELECT 'omni-proof', 'internal',   'NA';

-- 2. Cross-cloud join (run the job in us-east4). omni_s3.orders is the
--    aws-us-east-1 Omni external table; customer_dim is native in us-east4.
SELECT o.order_id, o.customer, o.amount, d.segment, d.region
FROM   omni_s3.orders o
JOIN   omni_join_ref.customer_dim d USING (customer)
ORDER BY o.order_id;
