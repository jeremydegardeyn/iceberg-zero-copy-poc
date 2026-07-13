-- Phase 5: freshness. First run on GCP:  ./scripts/02_create_table.sh --append
SELECT COUNT(*) FROM shared_gcp_data.shared_aws.orders;  -- expect 4, no pipeline ran

-- Phase 6: egress metering (ACCOUNT_USAGE lags up to ~2h).
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
WHERE transfer_type = 'DATA_LAKE'
ORDER BY start_time DESC;
