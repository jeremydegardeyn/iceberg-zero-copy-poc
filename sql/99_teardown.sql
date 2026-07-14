-- Run before scripts/99_teardown_gcp.sh
DROP DATABASE IF EXISTS shared_gcp_data;
DROP CATALOG INTEGRATION IF EXISTS biglake_catalog_int;
