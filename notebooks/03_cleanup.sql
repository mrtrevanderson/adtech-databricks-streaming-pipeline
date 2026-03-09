-- =============================================================================
-- Pipeline Cleanup: Drop All Tables
-- =============================================================================
-- Run this notebook to fully reset the pipeline state.
-- Required when:
--   - Changing a dataset type (STREAMING TABLE <-> MATERIALIZED VIEW)
--   - Resetting pipeline for a clean demo run
--   - Troubleshooting schema or checkpoint issues
--
-- After running: delete pipeline in Lakeflow UI and recreate it,
-- or click "Full Refresh" in the pipeline settings.
-- =============================================================================

-- -------------------------
-- GOLD
-- -------------------------
DROP TABLE IF EXISTS ius_unity_prod.sandbox.gold_post_transaction_triggers;
DROP TABLE IF EXISTS ius_unity_prod.sandbox.gold_user_targeting_profile;

-- -------------------------
-- SILVER
-- -------------------------
DROP TABLE IF EXISTS ius_unity_prod.sandbox.silver_enriched_purchases;
DROP TABLE IF EXISTS ius_unity_prod.sandbox.silver_session_summary;
DROP TABLE IF EXISTS ius_unity_prod.sandbox.silver_user_profiles;
DROP TABLE IF EXISTS ius_unity_prod.sandbox.silver_ecommerce_events;

-- -------------------------
-- BRONZE
-- -------------------------
DROP TABLE IF EXISTS ius_unity_prod.sandbox.bronze_user_profiles_raw;
DROP TABLE IF EXISTS ius_unity_prod.sandbox.bronze_ecommerce_events;

-- -------------------------
-- VERIFY: all tables gone
-- -------------------------
SHOW TABLES IN ius_unity_prod.sandbox;
