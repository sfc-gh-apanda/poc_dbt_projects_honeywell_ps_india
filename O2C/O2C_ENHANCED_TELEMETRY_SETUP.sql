-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED - TELEMETRY & MONITORING VIEWS
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Enhanced monitoring views with row count tracking and data validation
-- 
-- Views Created:
--   1. V_ROW_COUNT_TRACKING       - Layer-by-layer row counts
--   2. V_DATA_FLOW_VALIDATION     - Source to mart reconciliation
--   3. V_AUDIT_COLUMN_VALIDATION  - Verify audit columns are populated
--   4. V_BATCH_TRACKING           - Track batches across runs
-- 
-- Prerequisites:
--   - O2C_ENHANCED_AUDIT_SETUP.sql executed
--   - dbt_o2c_enhanced has been run at least once
-- 
-- Idempotent: YES
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE SCHEMA O2C_AUDIT;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 1: ROW COUNT TRACKING
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Track row counts across all layers in real-time

CREATE OR REPLACE VIEW V_ROW_COUNT_TRACKING AS
-- Source Layer
SELECT 
    'SOURCE' AS layer,
    'FACT_SALES_ORDERS' AS table_name,
    'Orders' AS description,
    COUNT(*) AS row_count,
    MAX(CREATED_DATE) AS latest_record,
    CURRENT_TIMESTAMP() AS checked_at
FROM EDW.CORP_TRAN.FACT_SALES_ORDERS

UNION ALL
SELECT 'SOURCE', 'FACT_INVOICES', 'Invoices', COUNT(*), MAX(CREATED_DATE), CURRENT_TIMESTAMP()
FROM EDW.CORP_TRAN.FACT_INVOICES

UNION ALL
SELECT 'SOURCE', 'FACT_PAYMENTS', 'Payments', COUNT(*), MAX(CREATED_DATE), CURRENT_TIMESTAMP()
FROM EDW.CORP_TRAN.FACT_PAYMENTS

UNION ALL
SELECT 'SOURCE', 'DIM_CUSTOMER', 'Customers', COUNT(*), MAX(LOAD_TS), CURRENT_TIMESTAMP()
FROM EDW.CORP_MASTER.DIM_CUSTOMER

UNION ALL
-- Staging Layer (Enhanced)
SELECT 'STAGING', 'STG_ENRICHED_ORDERS', 'Orders+Customer', COUNT(*), NULL, CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_ORDERS

UNION ALL
SELECT 'STAGING', 'STG_ENRICHED_INVOICES', 'Invoices+Terms', COUNT(*), NULL, CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_INVOICES

UNION ALL
SELECT 'STAGING', 'STG_ENRICHED_PAYMENTS', 'Payments+Bank', COUNT(*), NULL, CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_PAYMENTS

UNION ALL
-- Mart Layer (Enhanced)
SELECT 'DIMENSION', 'DIM_O2C_CUSTOMER', 'Customer Dim', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER

UNION ALL
SELECT 'CORE', 'DM_O2C_RECONCILIATION', 'Reconciliation', COUNT(*), MAX(dbt_updated_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL
SELECT 'EVENTS', 'FACT_O2C_EVENTS', 'Event Log', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS

UNION ALL
SELECT 'PARTITIONED', 'FACT_O2C_DAILY', 'Daily Facts', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_DAILY

UNION ALL
SELECT 'AGGREGATE', 'AGG_O2C_BY_CUSTOMER', 'Customer Agg', COUNT(*), MAX(dbt_loaded_at), CURRENT_TIMESTAMP()
FROM EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER;

COMMENT ON VIEW V_ROW_COUNT_TRACKING IS 
    'Real-time row count tracking across all O2C Enhanced layers';

SELECT '✅ VIEW 1 CREATED: V_ROW_COUNT_TRACKING' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 2: DATA FLOW VALIDATION
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Validate data flows correctly through layers with variance detection

CREATE OR REPLACE VIEW V_DATA_FLOW_VALIDATION AS
WITH source_counts AS (
    SELECT 
        'Orders' AS entity,
        COUNT(*) AS source_rows
    FROM EDW.CORP_TRAN.FACT_SALES_ORDERS
    UNION ALL
    SELECT 'Invoices', COUNT(*)
    FROM EDW.CORP_TRAN.FACT_INVOICES
    UNION ALL
    SELECT 'Payments', COUNT(*)
    FROM EDW.CORP_TRAN.FACT_PAYMENTS
),
staging_counts AS (
    SELECT 
        'Orders' AS entity,
        COUNT(*) AS staging_rows
    FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_ORDERS
    UNION ALL
    SELECT 'Invoices', COUNT(*)
    FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_INVOICES
    UNION ALL
    SELECT 'Payments', COUNT(*)
    FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_PAYMENTS
)
SELECT 
    s.entity,
    s.source_rows,
    st.staging_rows,
    st.staging_rows - s.source_rows AS row_variance,
    ROUND((st.staging_rows - s.source_rows) * 100.0 / NULLIF(s.source_rows, 0), 2) AS variance_pct,
    CASE 
        WHEN s.source_rows = st.staging_rows THEN '✅ MATCHED'
        WHEN ABS(st.staging_rows - s.source_rows) / NULLIF(s.source_rows, 0) < 0.01 THEN '⚠️ MINOR VARIANCE'
        ELSE '❌ MISMATCH'
    END AS validation_status,
    CURRENT_TIMESTAMP() AS validated_at
FROM source_counts s
JOIN staging_counts st ON s.entity = st.entity;

COMMENT ON VIEW V_DATA_FLOW_VALIDATION IS 
    'Source to staging row count reconciliation with variance detection';

SELECT '✅ VIEW 2 CREATED: V_DATA_FLOW_VALIDATION' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 3: AUDIT COLUMN VALIDATION
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Verify all audit columns are properly populated

CREATE OR REPLACE VIEW V_AUDIT_COLUMN_VALIDATION AS
SELECT 
    'DIM_O2C_CUSTOMER' AS model_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN dbt_run_id IS NOT NULL THEN 1 ELSE 0 END) AS has_run_id,
    SUM(CASE WHEN dbt_batch_id IS NOT NULL THEN 1 ELSE 0 END) AS has_batch_id,
    SUM(CASE WHEN dbt_loaded_at IS NOT NULL THEN 1 ELSE 0 END) AS has_loaded_at,
    SUM(CASE WHEN dbt_row_hash IS NOT NULL THEN 1 ELSE 0 END) AS has_row_hash,
    COUNT(DISTINCT dbt_run_id) AS distinct_runs,
    COUNT(DISTINCT dbt_batch_id) AS distinct_batches,
    MIN(dbt_loaded_at) AS earliest_load,
    MAX(dbt_loaded_at) AS latest_load,
    CASE 
        WHEN SUM(CASE WHEN dbt_run_id IS NULL THEN 1 ELSE 0 END) = 0 
         AND SUM(CASE WHEN dbt_loaded_at IS NULL THEN 1 ELSE 0 END) = 0
        THEN '✅ VALID'
        ELSE '❌ INVALID'
    END AS audit_status
FROM EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER

UNION ALL

SELECT 
    'DM_O2C_RECONCILIATION',
    COUNT(*),
    SUM(CASE WHEN dbt_run_id IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN dbt_batch_id IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN dbt_created_at IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN dbt_row_hash IS NOT NULL THEN 1 ELSE 0 END),
    COUNT(DISTINCT dbt_run_id),
    COUNT(DISTINCT dbt_batch_id),
    MIN(dbt_created_at),
    MAX(dbt_updated_at),
    CASE 
        WHEN SUM(CASE WHEN dbt_run_id IS NULL THEN 1 ELSE 0 END) = 0 THEN '✅ VALID'
        ELSE '❌ INVALID'
    END
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION

UNION ALL

SELECT 
    'FACT_O2C_EVENTS',
    COUNT(*),
    SUM(CASE WHEN dbt_run_id IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN dbt_batch_id IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN dbt_loaded_at IS NOT NULL THEN 1 ELSE 0 END),
    0,  -- No row hash for events
    COUNT(DISTINCT dbt_run_id),
    COUNT(DISTINCT dbt_batch_id),
    MIN(dbt_loaded_at),
    MAX(dbt_loaded_at),
    CASE 
        WHEN SUM(CASE WHEN dbt_run_id IS NULL THEN 1 ELSE 0 END) = 0 THEN '✅ VALID'
        ELSE '❌ INVALID'
    END
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS;

COMMENT ON VIEW V_AUDIT_COLUMN_VALIDATION IS 
    'Validates that audit columns are properly populated in all models';

SELECT '✅ VIEW 3 CREATED: V_AUDIT_COLUMN_VALIDATION' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 4: BATCH TRACKING
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Track batch IDs across models and runs

CREATE OR REPLACE VIEW V_BATCH_TRACKING AS
SELECT 
    r.run_id,
    r.run_started_at,
    r.run_status,
    r.environment,
    m.model_name,
    m.batch_id,
    m.status AS model_status,
    m.rows_affected,
    m.execution_seconds,
    m.materialization
FROM EDW.O2C_AUDIT.DBT_RUN_LOG r
JOIN EDW.O2C_AUDIT.DBT_MODEL_LOG m ON r.run_id = m.run_id
WHERE r.run_started_at >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY r.run_started_at DESC, m.model_name;

COMMENT ON VIEW V_BATCH_TRACKING IS 
    'Track batch IDs across models and runs for the last 7 days';

SELECT '✅ VIEW 4 CREATED: V_BATCH_TRACKING' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 5: LOAD PATTERN ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════
-- Purpose: Analyze different load patterns and their performance

CREATE OR REPLACE VIEW V_LOAD_PATTERN_ANALYSIS AS
SELECT
    m.materialization AS load_pattern,
    COUNT(DISTINCT m.model_name) AS model_count,
    COUNT(*) AS total_executions,
    ROUND(AVG(m.execution_seconds), 2) AS avg_execution_sec,
    ROUND(MAX(m.execution_seconds), 2) AS max_execution_sec,
    SUM(m.rows_affected) AS total_rows_processed,
    ROUND(AVG(m.rows_affected), 0) AS avg_rows_per_run,
    SUM(CASE WHEN m.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN m.status = 'FAIL' THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(SUM(CASE WHEN m.status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS success_rate_pct
FROM EDW.O2C_AUDIT.DBT_MODEL_LOG m
WHERE m.started_at >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY m.materialization
ORDER BY total_executions DESC;

COMMENT ON VIEW V_LOAD_PATTERN_ANALYSIS IS 
    'Analyze performance of different load patterns (table, incremental, view)';

SELECT '✅ VIEW 5 CREATED: V_LOAD_PATTERN_ANALYSIS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '✅ O2C ENHANCED TELEMETRY SETUP COMPLETE!' AS final_status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- Show created views
SHOW VIEWS LIKE 'V_%' IN SCHEMA EDW.O2C_AUDIT;


