-- ADR-0002 option C: read the S3 replica through an object-store catalog
-- integration (no external catalog needed — Snowflake reads metadata.json
-- directly from the external volume).
-- Interleaves with scripts/11_aws_replica_setup.py — see its docstring.

-- 1. External volume over the replica bucket (fill in <S3_BUCKET> and the
--    role ARN printed by: python scripts/11_aws_replica_setup.py create
CREATE OR REPLACE EXTERNAL VOLUME s3_replica_vol
  STORAGE_LOCATIONS = ((
    NAME = 's3-replica'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://<S3_BUCKET>/'
    STORAGE_AWS_ROLE_ARN = '<ROLE_ARN_FROM_CREATE_STEP>'
  ))
  ALLOW_WRITES = FALSE;

-- 2. Note STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID, then run:
--    python scripts/11_aws_replica_setup.py trust --bucket <S3_BUCKET> \
--        --iam_user_arn <ARN> --external_id <ID>
DESC EXTERNAL VOLUME s3_replica_vol;

-- 3. Object-store catalog integration (points at files, not a catalog service).
CREATE OR REPLACE CATALOG INTEGRATION s3_replica_int
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;

-- 4. Register the replica. METADATA_FILE_PATH comes from the output of
--    scripts/10_replicate_to_s3.sh (e.g. orders/metadata/v3.metadata.json —
--    relative to STORAGE_BASE_URL; adjust to the printed path).
CREATE OR REPLACE ICEBERG TABLE replica_orders
  EXTERNAL_VOLUME = 's3_replica_vol'
  CATALOG = 's3_replica_int'
  METADATA_FILE_PATH = '<FROM_10_REPLICATE_OUTPUT>';

-- 5. Prove it: same rows, different cloud path. The replica is intra-region
--    to this Snowflake account (us-east-2) — reads have NO cross-cloud egress;
--    the cross-cloud cost was paid once at sync time.
SELECT 'zero_copy' AS path, COUNT(*) FROM shared_gcp_data.shared_aws.orders
UNION ALL
SELECT 's3_replica', COUNT(*) FROM replica_orders;

-- Staleness is by design: new GCS commits do NOT appear here until
-- scripts/10_replicate_to_s3.sh runs again (then: ALTER ICEBERG TABLE
-- replica_orders REFRESH '<new metadata path>').
