-- Phase 4: catalog-linked database + first zero-copy read.
CREATE OR REPLACE DATABASE shared_gcp_data
  LINKED_CATALOG = ( CATALOG = 'biglake_catalog_int' );

-- Discovery is async; wait ~1 min if empty.
SHOW SCHEMAS IN DATABASE shared_gcp_data;          -- expect SHARED_AWS
SELECT * FROM shared_gcp_data.shared_aws.orders;   -- 3 rows = zero-copy read works

-- If SELECT fails with a storage-access error: credential vending isn't flowing.
-- Fallback: create an external volume for gs://<BUCKET>, grant its Snowflake
-- service agent roles/storage.objectViewer on the bucket, and recreate this
-- database with EXTERNAL_VOLUME = <vol>. See docs/runbook.md.
