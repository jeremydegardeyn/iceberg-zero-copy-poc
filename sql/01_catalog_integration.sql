-- Phase 3 (Snowflake side). Run as ACCOUNTADMIN. Interleaves with scripts/03_wif_setup.sh.

-- 3a. Get issuer URL, then run on GCP:  ./scripts/03_wif_setup.sh pool '<ISSUER_URL>'
SELECT SYSTEM$GET_WORKLOAD_IDENTITY_ISSUER_URL();

-- 3c. Fill in <BUCKET>, <PROJECT_ID>, and the OAUTH_AUDIENCE printed by the pool step.
CREATE OR REPLACE CATALOG INTEGRATION biglake_catalog_int
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  REST_CONFIG = (
    CATALOG_URI = 'https://biglake.googleapis.com/iceberg/v1/restcatalog'
    CATALOG_NAME = 'gs://<BUCKET>'
    ADDITIONAL_HEADERS = ( "x-goog-user-project" = '<PROJECT_ID>' )
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS  -- required for CLD without an external volume
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_GRANT_TYPE = TOKEN_EXCHANGE
    OAUTH_TOKEN_URI = 'https://sts.googleapis.com/v1/token'
    OAUTH_AUDIENCE = '<AUDIENCE_FROM_POOL_STEP>'
    OAUTH_ALLOWED_SCOPES = ('https://www.googleapis.com/auth/bigquery')
  )
  ENABLED = TRUE;

-- Note WORKLOAD_IDENTITY_FEDERATION_SUBJECT, then run on GCP:
--   ./scripts/03_wif_setup.sh grant '<WIF_SUBJECT>'
DESC CATALOG INTEGRATION biglake_catalog_int;

-- 3e. Verify (after the grant step).
SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('biglake_catalog_int');
