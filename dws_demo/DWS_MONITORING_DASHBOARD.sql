-- ═══════════════════════════════════════════════════════════════════════════════
-- DWS CLIENT REPORTING - COMPREHENSIVE MONITORING DASHBOARD
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- Purpose: Complete observability for dbt_dws_client_reporting project
-- Uses: SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY (Snowflake Native dbt compatible)
--
-- Views Created (22 total):
--
--   EXECUTION TRACKING (4):
--     1.  DWS_MODEL_EXECUTIONS           - Model execution from Query History
--     2.  DWS_TEST_EXECUTIONS            - Test execution from Query History
--     3.  DWS_DAILY_EXECUTION_SUMMARY    - Daily execution summary
--     4.  DWS_SLOWEST_MODELS             - Performance bottlenecks
--
--   FRESHNESS (2):
--     5.  DWS_SOURCE_FRESHNESS           - Source table freshness
--     6.  DWS_MODEL_FRESHNESS            - dbt model freshness
--
--   ALERTS (4):
--     7.  DWS_ALERT_PERFORMANCE          - Performance degradation alerts
--     8.  DWS_ALERT_MODEL_FAILURES       - Model failure alerts
--     9.  DWS_ALERT_STALE_SOURCES        - Stale source alerts
--    10.  DWS_ALERT_SUMMARY              - Alert dashboard summary
--
--   TEST INSIGHTS (3):
--    11.  DWS_TEST_SUMMARY_BY_TYPE       - Test summary by type
--    12.  DWS_TEST_PASS_RATE_TREND       - Test pass rate trend
--    13.  DWS_RECURRING_TEST_FAILURES    - Recurring test failures
--
--   COST MONITORING (3):
--    14.  DWS_COST_DAILY                 - Daily credit consumption
--    15.  DWS_COST_BY_MODEL              - Cost attribution by model
--    16.  DWS_ALERT_COST                 - Cost anomaly alerts
--
--   QUERY PERFORMANCE (3):
--    17.  DWS_LONG_RUNNING_QUERIES       - Queries >1 minute
--    18.  DWS_QUEUE_TIME_ANALYSIS        - Queue time by hour
--    19.  DWS_MODEL_PERFORMANCE_TREND    - Execution time trends
--
--   DATA QUALITY (3):
--    20.  DWS_ROW_COUNT_TRACKING         - Row counts across all layers
--    21.  DWS_DATA_RECONCILIATION        - Source to mart reconciliation
--    22.  DWS_OPERATIONAL_SUMMARY        - Comprehensive operational summary
--
-- Prerequisites:
--   - DWS_AUDIT_SETUP.sql executed
--   - dbt_dws_client_reporting project has run at least once
--
-- Idempotent: YES - Safe to run multiple times
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE DWSEDW;

CREATE SCHEMA IF NOT EXISTS DWSEDW.DWS_MONITORING
    COMMENT = 'DWS Client Reporting platform observability - Query History based monitoring';

USE SCHEMA DWS_MONITORING;


-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 1: EXECUTION TRACKING
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── VIEW 1: MODEL EXECUTIONS ────────────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_MODEL_EXECUTIONS AS
SELECT
    query_id,
    start_time AS run_started_at,
    end_time,
    user_name,
    role_name,
    warehouse_name,
    database_name,
    schema_name,
    execution_status AS status,
    total_elapsed_time / 1000.0 AS total_node_runtime,
    rows_produced AS rows_affected,
    bytes_scanned,
    compilation_time / 1000.0 AS compilation_seconds,
    COALESCE(
        REGEXP_SUBSTR(query_text, 'TABLE\\s+([\\w.]+)', 1, 1, 'ie', 1),
        REGEXP_SUBSTR(query_text, 'VIEW\\s+([\\w.]+)', 1, 1, 'ie', 1),
        schema_name || '.unknown_model'
    ) AS node_id,
    SPLIT_PART(
        COALESCE(
            REGEXP_SUBSTR(query_text, 'TABLE\\s+([\\w.]+)', 1, 1, 'ie', 1),
            REGEXP_SUBSTR(query_text, 'VIEW\\s+([\\w.]+)', 1, 1, 'ie', 1)
        ), '.', -1
    ) AS model_name,
    query_type,
    query_tag
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'DWSEDW'
  AND schema_name LIKE 'DWS_CLIENT_REPORTING%'
  AND start_time >= DATEADD(day, -90, CURRENT_DATE())
  AND execution_status IN ('SUCCESS', 'FAIL')
  AND (
      query_text ILIKE '%create%or%replace%table%'
      OR query_text ILIKE '%create%or%replace%view%'
      OR query_text ILIKE '%insert%into%'
      OR query_text ILIKE '%merge%into%'
      OR query_text ILIKE '%delete%from%'
  );

COMMENT ON VIEW DWS_MODEL_EXECUTIONS IS
    'DWS dbt model executions tracked from Query History';


-- ─── VIEW 2: TEST EXECUTIONS ─────────────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_TEST_EXECUTIONS AS
SELECT
    query_id,
    start_time AS run_started_at,
    end_time,
    user_name,
    execution_status AS status,
    total_elapsed_time / 1000.0 AS total_node_runtime,
    rows_produced,
    bytes_scanned,
    CASE
        WHEN query_text ILIKE '%not%null%' THEN 'not_null'
        WHEN query_text ILIKE '%unique%' THEN 'unique'
        WHEN query_text ILIKE '%relationships%' THEN 'relationships'
        WHEN query_text ILIKE '%accepted_values%' THEN 'accepted_values'
        WHEN query_text ILIKE '%dbt_utils%' THEN 'dbt_utils'
        WHEN query_text ILIKE '%reconciliation%' THEN 'reconciliation'
        ELSE 'generic'
    END AS test_type,
    CASE
        WHEN rows_produced = 0 AND execution_status = 'SUCCESS' THEN 'PASS'
        WHEN rows_produced > 0 AND execution_status = 'SUCCESS' THEN 'FAIL'
        WHEN execution_status = 'FAIL' THEN 'ERROR'
        ELSE 'UNKNOWN'
    END AS test_result,
    query_tag,
    LEFT(query_text, 500) AS query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'DWSEDW'
  AND start_time >= DATEADD(day, -90, CURRENT_DATE())
  AND query_type = 'SELECT'
  AND (query_text ILIKE '%dws_client_reporting%' OR query_tag LIKE '%dbt_dws%')
  AND schema_name LIKE 'DWS_CLIENT_REPORTING%'
  AND (
      query_text ILIKE '%dbt_test%'
      OR (query_text ILIKE '%count(*)%' AND query_text ILIKE '%where%not%')
      OR query_text ILIKE '%dbt_utils%'
      OR query_text ILIKE '%reconciliation%'
  );

COMMENT ON VIEW DWS_TEST_EXECUTIONS IS
    'DWS dbt test executions inferred from Query History';


-- ─── VIEW 3: DAILY EXECUTION SUMMARY ────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_DAILY_EXECUTION_SUMMARY AS
SELECT
    DATE(run_started_at) AS execution_date,
    COUNT(DISTINCT model_name) AS models_run,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_models,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed_models,
    ROUND(SUM(total_node_runtime) / 60, 2) AS total_minutes,
    ROUND(AVG(total_node_runtime), 2) AS avg_execution_seconds,
    MAX(total_node_runtime) AS max_execution_seconds,
    SUM(rows_affected) AS total_rows_affected,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0
          / NULLIF(COUNT(*), 0), 1) AS success_rate_pct
FROM DWS_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY execution_date
ORDER BY execution_date DESC;

COMMENT ON VIEW DWS_DAILY_EXECUTION_SUMMARY IS
    'Daily summary of DWS dbt model executions';


-- ─── VIEW 4: SLOWEST MODELS ─────────────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_SLOWEST_MODELS AS
SELECT
    model_name,
    schema_name,
    COUNT(*) AS run_count,
    ROUND(AVG(total_node_runtime), 2) AS avg_seconds,
    ROUND(MAX(total_node_runtime), 2) AS max_seconds,
    ROUND(MIN(total_node_runtime), 2) AS min_seconds,
    ROUND(SUM(total_node_runtime), 2) AS total_seconds,
    ROUND((SUM(total_node_runtime) / 3600) * 3.0, 2) AS estimated_cost_usd,
    CASE
        WHEN AVG(total_node_runtime) > 300 THEN 'CRITICAL'
        WHEN AVG(total_node_runtime) > 60 THEN 'SLOW'
        WHEN AVG(total_node_runtime) > 10 THEN 'MODERATE'
        ELSE 'FAST'
    END AS performance_tier
FROM DWS_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
  AND status = 'SUCCESS'
GROUP BY model_name, schema_name
ORDER BY avg_seconds DESC
LIMIT 20;

COMMENT ON VIEW DWS_SLOWEST_MODELS IS
    'Top 20 slowest DWS models by average execution time';


-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 2: FRESHNESS
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── VIEW 5: SOURCE FRESHNESS ────────────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_SOURCE_FRESHNESS AS
SELECT 'FACT_PORTFOLIO_HOLDINGS' AS source_table, 'Transactional' AS source_type,
    'DWS_TRAN' AS schema_name, COUNT(*) AS row_count,
    MAX(updated_ts) AS last_load_timestamp,
    DATEDIFF('hour', MAX(updated_ts), CURRENT_TIMESTAMP()) AS hours_since_load,
    CASE
        WHEN DATEDIFF('hour', MAX(updated_ts), CURRENT_TIMESTAMP()) <= 24 THEN 'Fresh'
        WHEN DATEDIFF('hour', MAX(updated_ts), CURRENT_TIMESTAMP()) <= 48 THEN 'Warning'
        ELSE 'Stale'
    END AS freshness_status,
    CURRENT_TIMESTAMP() AS checked_at
FROM DWSEDW.DWS_TRAN.FACT_PORTFOLIO_HOLDINGS

UNION ALL
SELECT 'FACT_TRANSACTIONS', 'Transactional', 'DWS_TRAN', COUNT(*),
    MAX(updated_ts), DATEDIFF('hour', MAX(updated_ts), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(updated_ts), CURRENT_TIMESTAMP()) <= 24 THEN 'Fresh'
         WHEN DATEDIFF('hour', MAX(updated_ts), CURRENT_TIMESTAMP()) <= 48 THEN 'Warning'
         ELSE 'Stale' END, CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_TRAN.FACT_TRANSACTIONS

UNION ALL
SELECT 'FACT_NAV_PRICES', 'Transactional', 'DWS_TRAN', COUNT(*),
    MAX(created_ts), DATEDIFF('hour', MAX(created_ts), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(created_ts), CURRENT_TIMESTAMP()) <= 24 THEN 'Fresh'
         WHEN DATEDIFF('hour', MAX(created_ts), CURRENT_TIMESTAMP()) <= 48 THEN 'Warning'
         ELSE 'Stale' END, CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_TRAN.FACT_NAV_PRICES

UNION ALL
SELECT 'DIM_CLIENT', 'Master', 'DWS_MASTER', COUNT(*),
    MAX(update_ts), DATEDIFF('hour', MAX(update_ts), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(update_ts), CURRENT_TIMESTAMP()) <= 48 THEN 'Fresh'
         WHEN DATEDIFF('hour', MAX(update_ts), CURRENT_TIMESTAMP()) <= 72 THEN 'Warning'
         ELSE 'Stale' END, CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_MASTER.DIM_CLIENT

UNION ALL
SELECT 'DIM_ACCOUNT', 'Master', 'DWS_MASTER', COUNT(*),
    MAX(update_ts), DATEDIFF('hour', MAX(update_ts), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(update_ts), CURRENT_TIMESTAMP()) <= 48 THEN 'Fresh'
         WHEN DATEDIFF('hour', MAX(update_ts), CURRENT_TIMESTAMP()) <= 72 THEN 'Warning'
         ELSE 'Stale' END, CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_MASTER.DIM_ACCOUNT

UNION ALL
SELECT 'DIM_FUND', 'Master', 'DWS_MASTER', COUNT(*),
    MAX(update_ts), DATEDIFF('hour', MAX(update_ts), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(update_ts), CURRENT_TIMESTAMP()) <= 48 THEN 'Fresh'
         WHEN DATEDIFF('hour', MAX(update_ts), CURRENT_TIMESTAMP()) <= 72 THEN 'Warning'
         ELSE 'Stale' END, CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_MASTER.DIM_FUND

UNION ALL
SELECT 'DIM_BENCHMARK', 'Reference', 'DWS_REF', COUNT(*),
    MAX(load_ts), DATEDIFF('hour', MAX(load_ts), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(load_ts), CURRENT_TIMESTAMP()) <= 72 THEN 'Fresh'
         ELSE 'Warning' END, CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_REF.DIM_BENCHMARK

UNION ALL
SELECT 'FACT_FX_RATES', 'Reference', 'DWS_REF', COUNT(*),
    MAX(created_ts), DATEDIFF('hour', MAX(created_ts), CURRENT_TIMESTAMP()),
    CASE WHEN DATEDIFF('hour', MAX(created_ts), CURRENT_TIMESTAMP()) <= 24 THEN 'Fresh'
         WHEN DATEDIFF('hour', MAX(created_ts), CURRENT_TIMESTAMP()) <= 48 THEN 'Warning'
         ELSE 'Stale' END, CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_REF.FACT_FX_RATES;

COMMENT ON VIEW DWS_SOURCE_FRESHNESS IS
    'DWS source table freshness status for all 8 source tables';


-- ─── VIEW 6: MODEL FRESHNESS ────────────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_MODEL_FRESHNESS AS
SELECT
    model_name,
    schema_name,
    layer,
    row_count,
    last_refresh,
    DATEDIFF('minute', last_refresh, CURRENT_TIMESTAMP()) AS minutes_since_refresh,
    CASE
        WHEN DATEDIFF('hour', last_refresh, CURRENT_TIMESTAMP()) <= 24 THEN 'Fresh'
        WHEN DATEDIFF('hour', last_refresh, CURRENT_TIMESTAMP()) <= 48 THEN 'Warning'
        ELSE 'Stale'
    END AS freshness_status
FROM (
    SELECT 'DIM_CLIENT' AS model_name, 'DWS_CLIENT_REPORTING_DIMENSIONS' AS schema_name,
        'DIMENSION' AS layer, COUNT(*) AS row_count,
        MAX(dbt_loaded_at) AS last_refresh
    FROM DWSEDW.DWS_CLIENT_REPORTING_DIMENSIONS.DIM_CLIENT

    UNION ALL
    SELECT 'DIM_FUND', 'DWS_CLIENT_REPORTING_DIMENSIONS', 'DIMENSION', COUNT(*),
        MAX(dbt_loaded_at)
    FROM DWSEDW.DWS_CLIENT_REPORTING_DIMENSIONS.DIM_FUND

    UNION ALL
    SELECT 'DIM_ACCOUNT', 'DWS_CLIENT_REPORTING_DIMENSIONS', 'DIMENSION', COUNT(*),
        MAX(dbt_loaded_at)
    FROM DWSEDW.DWS_CLIENT_REPORTING_DIMENSIONS.DIM_ACCOUNT

    UNION ALL
    SELECT 'DM_AUM_SUMMARY', 'DWS_CLIENT_REPORTING_CORE', 'CORE', COUNT(*),
        MAX(dbt_updated_at)
    FROM DWSEDW.DWS_CLIENT_REPORTING_CORE.DM_AUM_SUMMARY

    UNION ALL
    SELECT 'DM_PORTFOLIO_HOLDINGS_ASOF', 'DWS_CLIENT_REPORTING_CORE', 'CORE', COUNT(*),
        MAX(dbt_loaded_at)
    FROM DWSEDW.DWS_CLIENT_REPORTING_CORE.DM_PORTFOLIO_HOLDINGS_ASOF

    UNION ALL
    SELECT 'DM_CLIENT_PERFORMANCE', 'DWS_CLIENT_REPORTING_CORE', 'CORE', COUNT(*),
        MAX(dbt_loaded_at)
    FROM DWSEDW.DWS_CLIENT_REPORTING_CORE.DM_CLIENT_PERFORMANCE

    UNION ALL
    SELECT 'DM_CASHFLOW_SUMMARY', 'DWS_CLIENT_REPORTING_CORE', 'CORE', COUNT(*),
        MAX(dbt_loaded_at)
    FROM DWSEDW.DWS_CLIENT_REPORTING_CORE.DM_CASHFLOW_SUMMARY

    UNION ALL
    SELECT 'FACT_CLIENT_EVENTS', 'DWS_CLIENT_REPORTING_EVENTS', 'EVENTS', COUNT(*),
        MAX(dbt_loaded_at)
    FROM DWSEDW.DWS_CLIENT_REPORTING_EVENTS.FACT_CLIENT_EVENTS

    UNION ALL
    SELECT 'AGG_AUM_TIME_SERIES', 'DWS_CLIENT_REPORTING_AGGREGATES', 'AGGREGATE', COUNT(*),
        MAX(dbt_loaded_at)
    FROM DWSEDW.DWS_CLIENT_REPORTING_AGGREGATES.AGG_AUM_TIME_SERIES

    UNION ALL
    SELECT 'AGG_CLIENT_OVERVIEW', 'DWS_CLIENT_REPORTING_AGGREGATES', 'AGGREGATE', COUNT(*),
        MAX(dbt_loaded_at)
    FROM DWSEDW.DWS_CLIENT_REPORTING_AGGREGATES.AGG_CLIENT_OVERVIEW
);

COMMENT ON VIEW DWS_MODEL_FRESHNESS IS
    'DWS dbt model freshness status across all layers';


-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 3: ALERTS
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── VIEW 7: PERFORMANCE ALERTS ─────────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_ALERT_PERFORMANCE AS
WITH model_stats AS (
    SELECT
        model_name,
        AVG(total_node_runtime) AS avg_runtime,
        STDDEV(total_node_runtime) AS stddev_runtime
    FROM DWS_MODEL_EXECUTIONS
    WHERE status = 'SUCCESS'
      AND run_started_at >= DATEADD(day, -14, CURRENT_DATE())
    GROUP BY model_name
),
recent_runs AS (
    SELECT
        model_name,
        total_node_runtime,
        run_started_at
    FROM DWS_MODEL_EXECUTIONS
    WHERE status = 'SUCCESS'
      AND run_started_at >= DATEADD(day, -1, CURRENT_DATE())
)
SELECT
    r.model_name,
    ROUND(r.total_node_runtime, 2) AS latest_runtime_seconds,
    ROUND(s.avg_runtime, 2) AS avg_runtime_seconds,
    ROUND((r.total_node_runtime - s.avg_runtime) / NULLIF(s.stddev_runtime, 0), 2) AS z_score,
    ROUND((r.total_node_runtime - s.avg_runtime) / NULLIF(s.avg_runtime, 0) * 100, 1) AS pct_above_avg,
    CASE
        WHEN (r.total_node_runtime - s.avg_runtime) / NULLIF(s.stddev_runtime, 0) > 3 THEN 'CRITICAL'
        WHEN (r.total_node_runtime - s.avg_runtime) / NULLIF(s.stddev_runtime, 0) > 2 THEN 'HIGH'
        WHEN (r.total_node_runtime - s.avg_runtime) / NULLIF(s.avg_runtime, 0) > 0.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    r.run_started_at,
    CURRENT_TIMESTAMP() AS detected_at
FROM recent_runs r
JOIN model_stats s ON r.model_name = s.model_name
WHERE r.total_node_runtime > s.avg_runtime * 1.5
ORDER BY z_score DESC;

COMMENT ON VIEW DWS_ALERT_PERFORMANCE IS
    'Performance degradation alerts (>50% above average or >2 std dev)';


-- ─── VIEW 8: MODEL FAILURE ALERTS ───────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_ALERT_MODEL_FAILURES AS
SELECT
    model_name,
    schema_name,
    status,
    run_started_at,
    total_node_runtime AS runtime_seconds,
    query_tag,
    'Model ' || model_name || ' failed at ' ||
        TO_VARCHAR(run_started_at, 'YYYY-MM-DD HH24:MI') AS alert_description,
    CURRENT_TIMESTAMP() AS detected_at
FROM DWS_MODEL_EXECUTIONS
WHERE status = 'FAIL'
  AND run_started_at >= DATEADD(day, -3, CURRENT_DATE())
ORDER BY run_started_at DESC;

COMMENT ON VIEW DWS_ALERT_MODEL_FAILURES IS
    'DWS model failure alerts from last 3 days';


-- ─── VIEW 9: STALE SOURCE ALERTS ────────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_ALERT_STALE_SOURCES AS
SELECT
    source_table,
    source_type,
    row_count,
    last_load_timestamp,
    hours_since_load,
    freshness_status,
    CASE
        WHEN hours_since_load > 72 THEN 'CRITICAL'
        WHEN hours_since_load > 48 THEN 'HIGH'
        WHEN hours_since_load > 24 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    source_table || ' last loaded ' || hours_since_load || ' hours ago' AS alert_description,
    checked_at AS detected_at
FROM DWS_SOURCE_FRESHNESS
WHERE freshness_status IN ('Warning', 'Stale')
ORDER BY hours_since_load DESC;

COMMENT ON VIEW DWS_ALERT_STALE_SOURCES IS
    'Stale source table alerts (>24h for transactional, >48h for master)';


-- ─── VIEW 10: ALERT SUMMARY DASHBOARD ───────────────────────────────────────

CREATE OR REPLACE VIEW DWS_ALERT_SUMMARY AS
WITH all_alerts AS (
    SELECT 'PERFORMANCE' AS alert_type, severity, alert_description, detected_at
    FROM (SELECT model_name,
        ROUND(latest_runtime_seconds, 2) AS latest_runtime_seconds,
        severity,
        'Model ' || model_name || ' ran ' || ROUND(pct_above_avg, 0) || '% above average' AS alert_description,
        detected_at
    FROM DWS_ALERT_PERFORMANCE)

    UNION ALL
    SELECT 'MODEL_FAILURE', 'CRITICAL', alert_description, detected_at
    FROM DWS_ALERT_MODEL_FAILURES

    UNION ALL
    SELECT 'STALE_SOURCE', severity, alert_description, detected_at
    FROM DWS_ALERT_STALE_SOURCES
)
SELECT
    -- Overall health score
    CASE
        WHEN SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) > 0 THEN 'RED'
        WHEN SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) > 0 THEN 'AMBER'
        WHEN COUNT(*) > 0 THEN 'YELLOW'
        ELSE 'GREEN'
    END AS platform_health,
    COUNT(*) AS total_alerts,
    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_alerts,
    SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) AS high_alerts,
    SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_alerts,
    SUM(CASE WHEN alert_type = 'PERFORMANCE' THEN 1 ELSE 0 END) AS performance_alerts,
    SUM(CASE WHEN alert_type = 'MODEL_FAILURE' THEN 1 ELSE 0 END) AS failure_alerts,
    SUM(CASE WHEN alert_type = 'STALE_SOURCE' THEN 1 ELSE 0 END) AS stale_source_alerts,
    CURRENT_TIMESTAMP() AS checked_at
FROM all_alerts;

COMMENT ON VIEW DWS_ALERT_SUMMARY IS
    'DWS platform health dashboard with overall status and alert counts';


-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 4: TEST INSIGHTS
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── VIEW 11: TEST SUMMARY BY TYPE ──────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_TEST_SUMMARY_BY_TYPE AS
SELECT
    test_type,
    COUNT(*) AS total_executions,
    SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN test_result = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN test_result = 'ERROR' THEN 1 ELSE 0 END) AS errors,
    ROUND(SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) * 100.0
          / NULLIF(COUNT(*), 0), 1) AS pass_rate_pct,
    ROUND(AVG(total_node_runtime), 2) AS avg_test_seconds,
    MAX(run_started_at) AS last_run
FROM DWS_TEST_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY test_type
ORDER BY failed DESC, pass_rate_pct ASC;

COMMENT ON VIEW DWS_TEST_SUMMARY_BY_TYPE IS
    'DWS test summary by test type with pass rates';


-- ─── VIEW 12: TEST PASS RATE TREND ──────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_TEST_PASS_RATE_TREND AS
SELECT
    DATE(run_started_at) AS test_date,
    COUNT(*) AS total_tests,
    SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN test_result = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN test_result = 'ERROR' THEN 1 ELSE 0 END) AS errors,
    ROUND(SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) * 100.0
          / NULLIF(COUNT(*), 0), 1) AS pass_rate_pct,
    CASE
        WHEN SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) * 100.0
             / NULLIF(COUNT(*), 0) >= 99 THEN 'EXCELLENT'
        WHEN SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) * 100.0
             / NULLIF(COUNT(*), 0) >= 95 THEN 'GOOD'
        WHEN SUM(CASE WHEN test_result = 'PASS' THEN 1 ELSE 0 END) * 100.0
             / NULLIF(COUNT(*), 0) >= 90 THEN 'WARNING'
        ELSE 'CRITICAL'
    END AS quality_tier
FROM DWS_TEST_EXECUTIONS
WHERE run_started_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY test_date
ORDER BY test_date DESC;

COMMENT ON VIEW DWS_TEST_PASS_RATE_TREND IS
    'DWS test pass rate trend over 30 days';


-- ─── VIEW 13: RECURRING TEST FAILURES ───────────────────────────────────────

CREATE OR REPLACE VIEW DWS_RECURRING_TEST_FAILURES AS
SELECT
    test_type,
    query_preview,
    COUNT(*) AS failure_count,
    MIN(run_started_at) AS first_failure,
    MAX(run_started_at) AS last_failure,
    DATEDIFF('day', MIN(run_started_at), MAX(run_started_at)) AS failure_span_days,
    CASE
        WHEN COUNT(*) >= 5 THEN 'CRITICAL - Recurring failure'
        WHEN COUNT(*) >= 3 THEN 'HIGH - Multiple failures'
        ELSE 'MEDIUM - Recent failure'
    END AS severity
FROM DWS_TEST_EXECUTIONS
WHERE test_result = 'FAIL'
  AND run_started_at >= DATEADD(day, -14, CURRENT_DATE())
GROUP BY test_type, query_preview
HAVING COUNT(*) >= 2
ORDER BY failure_count DESC;

COMMENT ON VIEW DWS_RECURRING_TEST_FAILURES IS
    'Recurring DWS test failures needing attention';


-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 5: COST MONITORING
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── VIEW 14: DAILY COST ────────────────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_COST_DAILY AS
WITH daily_credits AS (
    SELECT
        DATE(start_time) AS usage_date,
        warehouse_name,
        SUM(total_elapsed_time) / 1000 / 3600 AS compute_hours,
        SUM(
            CASE warehouse_size
                WHEN 'X-Small' THEN total_elapsed_time / 1000 / 3600 * 1
                WHEN 'Small' THEN total_elapsed_time / 1000 / 3600 * 2
                WHEN 'Medium' THEN total_elapsed_time / 1000 / 3600 * 4
                WHEN 'Large' THEN total_elapsed_time / 1000 / 3600 * 8
                WHEN 'X-Large' THEN total_elapsed_time / 1000 / 3600 * 16
                ELSE total_elapsed_time / 1000 / 3600 * 1
            END
        ) AS estimated_credits,
        COUNT(DISTINCT query_id) AS query_count,
        SUM(bytes_scanned) / 1024 / 1024 / 1024 AS gb_scanned
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE database_name = 'DWSEDW'
      AND schema_name LIKE 'DWS_%'
      AND start_time >= DATEADD('day', -30, CURRENT_DATE())
      AND execution_status = 'SUCCESS'
    GROUP BY usage_date, warehouse_name
)
SELECT
    usage_date,
    warehouse_name,
    ROUND(compute_hours, 4) AS compute_hours,
    ROUND(estimated_credits, 4) AS estimated_credits,
    ROUND(estimated_credits * 3.0, 2) AS estimated_cost_usd,
    query_count,
    ROUND(gb_scanned, 2) AS gb_scanned,
    ROUND(AVG(estimated_credits) OVER (
        PARTITION BY warehouse_name
        ORDER BY usage_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 4) AS credits_7day_avg,
    ROUND(
        (estimated_credits - AVG(estimated_credits) OVER (
            PARTITION BY warehouse_name
            ORDER BY usage_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )) / NULLIF(AVG(estimated_credits) OVER (
            PARTITION BY warehouse_name
            ORDER BY usage_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 0) * 100, 1
    ) AS variance_from_avg_pct
FROM daily_credits
ORDER BY usage_date DESC, warehouse_name;

COMMENT ON VIEW DWS_COST_DAILY IS
    'Daily credit consumption with 7-day moving average and variance';


-- ─── VIEW 15: COST BY MODEL ─────────────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_COST_BY_MODEL AS
SELECT
    model_name,
    schema_name,
    COUNT(*) AS executions,
    ROUND(SUM(total_node_runtime), 2) AS total_seconds,
    ROUND(AVG(total_node_runtime), 2) AS avg_seconds,
    SUM(rows_affected) AS total_rows,
    ROUND(SUM(total_node_runtime) / 3600 * 3.0, 2) AS estimated_cost_usd,
    ROUND((SUM(total_node_runtime) / 3600 * 3.0) / NULLIF(COUNT(*), 0), 4) AS cost_per_execution,
    ROUND((SUM(total_node_runtime) / 3600 * 3.0) / NULLIF(SUM(rows_affected), 0) * 1000, 4) AS cost_per_1k_rows,
    RANK() OVER (ORDER BY SUM(total_node_runtime) DESC) AS cost_rank,
    CASE
        WHEN SUM(total_node_runtime) / 3600 * 3.0 > 10 THEN 'HIGH COST'
        WHEN SUM(total_node_runtime) / 3600 * 3.0 > 5 THEN 'MODERATE COST'
        WHEN SUM(total_node_runtime) / 3600 * 3.0 > 1 THEN 'LOW COST'
        ELSE 'MINIMAL'
    END AS cost_tier,
    MAX(run_started_at) AS last_run
FROM DWS_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD('day', -7, CURRENT_DATE())
  AND status = 'SUCCESS'
GROUP BY model_name, schema_name
ORDER BY estimated_cost_usd DESC;

COMMENT ON VIEW DWS_COST_BY_MODEL IS
    'Cost attribution by DWS model with efficiency metrics';


-- ─── VIEW 16: COST ANOMALY ALERTS ───────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_ALERT_COST AS
SELECT
    usage_date,
    warehouse_name,
    estimated_credits,
    credits_7day_avg,
    variance_from_avg_pct,
    estimated_cost_usd,
    CASE
        WHEN variance_from_avg_pct > 100 THEN 'CRITICAL'
        WHEN variance_from_avg_pct > 50 THEN 'HIGH'
        WHEN variance_from_avg_pct > 25 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    'Cost is ' || ABS(ROUND(COALESCE(variance_from_avg_pct, 0), 0)) || '% ' ||
        CASE WHEN COALESCE(variance_from_avg_pct, 0) > 0 THEN 'above' ELSE 'below' END ||
        ' 7-day average' AS alert_description,
    CURRENT_TIMESTAMP() AS detected_at
FROM DWS_COST_DAILY
WHERE usage_date >= DATEADD('day', -3, CURRENT_DATE())
  AND ABS(COALESCE(variance_from_avg_pct, 0)) > 25
ORDER BY variance_from_avg_pct DESC;

COMMENT ON VIEW DWS_ALERT_COST IS
    'Cost anomaly alerts (>25% variance from 7-day average)';


-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 6: QUERY PERFORMANCE
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── VIEW 17: LONG RUNNING QUERIES ──────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_LONG_RUNNING_QUERIES AS
SELECT
    query_id,
    start_time,
    end_time,
    ROUND(total_elapsed_time / 1000, 2) AS elapsed_seconds,
    ROUND(execution_time / 1000, 2) AS execution_seconds,
    ROUND(compilation_time / 1000, 2) AS compilation_seconds,
    ROUND(queued_overload_time / 1000, 2) AS queue_seconds,
    warehouse_name,
    warehouse_size,
    user_name,
    query_type,
    ROUND(bytes_scanned / 1024 / 1024, 2) AS mb_scanned,
    rows_produced,
    LEFT(query_text, 500) AS query_preview,
    CASE
        WHEN total_elapsed_time / 1000 > 600 THEN 'CRITICAL (>10 min)'
        WHEN total_elapsed_time / 1000 > 300 THEN 'HIGH (5-10 min)'
        WHEN total_elapsed_time / 1000 > 120 THEN 'MEDIUM (2-5 min)'
        ELSE 'LOW (1-2 min)'
    END AS severity,
    CASE
        WHEN queued_overload_time / 1000 > execution_time / 1000 * 0.2 THEN 'HIGH QUEUE TIME'
        WHEN compilation_time / 1000 > execution_time / 1000 * 0.3 THEN 'HIGH COMPILATION'
        ELSE 'EXECUTION DOMINATED'
    END AS bottleneck_analysis
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'DWSEDW'
  AND schema_name LIKE 'DWS_%'
  AND total_elapsed_time / 1000 > 60
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND execution_status = 'SUCCESS'
ORDER BY total_elapsed_time DESC
LIMIT 100;

COMMENT ON VIEW DWS_LONG_RUNNING_QUERIES IS
    'Long-running DWS queries (>1 minute) with bottleneck analysis';


-- ─── VIEW 18: QUEUE TIME ANALYSIS ───────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_QUEUE_TIME_ANALYSIS AS
WITH queue_stats AS (
    SELECT
        DATE_TRUNC('hour', start_time) AS hour_bucket,
        warehouse_name,
        warehouse_size,
        COUNT(*) AS query_count,
        AVG(queued_overload_time) / 1000 AS avg_queue_seconds,
        MAX(queued_overload_time) / 1000 AS max_queue_seconds,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY queued_overload_time) / 1000 AS p95_queue_seconds,
        AVG(total_elapsed_time) / 1000 AS avg_total_seconds
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE database_name = 'DWSEDW'
      AND schema_name LIKE 'DWS_%'
      AND start_time >= DATEADD('day', -7, CURRENT_DATE())
      AND execution_status = 'SUCCESS'
    GROUP BY hour_bucket, warehouse_name, warehouse_size
)
SELECT
    hour_bucket,
    warehouse_name,
    warehouse_size,
    query_count,
    ROUND(avg_queue_seconds, 2) AS avg_queue_seconds,
    ROUND(max_queue_seconds, 2) AS max_queue_seconds,
    ROUND(p95_queue_seconds, 2) AS p95_queue_seconds,
    ROUND(avg_total_seconds, 2) AS avg_total_seconds,
    ROUND(avg_queue_seconds * 100.0 / NULLIF(avg_total_seconds, 0), 1) AS queue_impact_pct,
    CASE
        WHEN avg_queue_seconds > 30 THEN 'CRITICAL - Scale Up'
        WHEN avg_queue_seconds > 10 THEN 'HIGH - Consider Scaling'
        WHEN avg_queue_seconds > 5 THEN 'WARNING - Monitor'
        WHEN avg_queue_seconds > 0 THEN 'MODERATE'
        ELSE 'HEALTHY - No Queue'
    END AS queue_status
FROM queue_stats
ORDER BY hour_bucket DESC, avg_queue_seconds DESC;

COMMENT ON VIEW DWS_QUEUE_TIME_ANALYSIS IS
    'Query queue time analysis by hour with scaling recommendations';


-- ─── VIEW 19: MODEL PERFORMANCE TREND ───────────────────────────────────────

CREATE OR REPLACE VIEW DWS_MODEL_PERFORMANCE_TREND AS
WITH daily_perf AS (
    SELECT
        DATE(run_started_at) AS run_date,
        model_name,
        schema_name,
        ROUND(AVG(total_node_runtime), 2) AS avg_seconds,
        ROUND(MAX(total_node_runtime), 2) AS max_seconds,
        ROUND(MIN(total_node_runtime), 2) AS min_seconds,
        COUNT(*) AS run_count,
        SUM(rows_affected) AS total_rows
    FROM DWS_MODEL_EXECUTIONS
    WHERE status = 'SUCCESS'
      AND run_started_at >= DATEADD('day', -14, CURRENT_DATE())
    GROUP BY run_date, model_name, schema_name
)
SELECT
    run_date,
    model_name,
    schema_name,
    avg_seconds,
    max_seconds,
    min_seconds,
    run_count,
    total_rows,
    ROUND(AVG(avg_seconds) OVER (
        PARTITION BY model_name
        ORDER BY run_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS avg_7day_ma,
    CASE
        WHEN avg_seconds > 1.5 * AVG(avg_seconds) OVER (
            PARTITION BY model_name
            ORDER BY run_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) THEN 'DEGRADED (>50% slower)'
        WHEN avg_seconds > 1.2 * AVG(avg_seconds) OVER (
            PARTITION BY model_name
            ORDER BY run_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) THEN 'SLOWING (>20% slower)'
        WHEN avg_seconds < 0.8 * AVG(avg_seconds) OVER (
            PARTITION BY model_name
            ORDER BY run_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) THEN 'IMPROVED (>20% faster)'
        ELSE 'STABLE'
    END AS performance_trend
FROM daily_perf
ORDER BY run_date DESC, model_name;

COMMENT ON VIEW DWS_MODEL_PERFORMANCE_TREND IS
    'Model execution time trend with 7-day moving average';


-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 7: DATA QUALITY
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── VIEW 20: ROW COUNT TRACKING ────────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_ROW_COUNT_TRACKING AS
SELECT 'SOURCE' AS layer, 'FACT_PORTFOLIO_HOLDINGS' AS table_name,
    COUNT(*) AS row_count, CURRENT_TIMESTAMP() AS checked_at
FROM DWSEDW.DWS_TRAN.FACT_PORTFOLIO_HOLDINGS
UNION ALL
SELECT 'SOURCE', 'FACT_TRANSACTIONS', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_TRAN.FACT_TRANSACTIONS
UNION ALL
SELECT 'SOURCE', 'FACT_NAV_PRICES', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_TRAN.FACT_NAV_PRICES
UNION ALL
SELECT 'SOURCE', 'DIM_CLIENT', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_MASTER.DIM_CLIENT
UNION ALL
SELECT 'SOURCE', 'DIM_ACCOUNT', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_MASTER.DIM_ACCOUNT
UNION ALL
SELECT 'SOURCE', 'DIM_FUND', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_MASTER.DIM_FUND
UNION ALL
SELECT 'DIMENSION', 'DIM_CLIENT (dbt)', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_CLIENT_REPORTING_DIMENSIONS.DIM_CLIENT
UNION ALL
SELECT 'DIMENSION', 'DIM_FUND (dbt)', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_CLIENT_REPORTING_DIMENSIONS.DIM_FUND
UNION ALL
SELECT 'DIMENSION', 'DIM_ACCOUNT (dbt)', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_CLIENT_REPORTING_DIMENSIONS.DIM_ACCOUNT
UNION ALL
SELECT 'CORE', 'DM_AUM_SUMMARY', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_CLIENT_REPORTING_CORE.DM_AUM_SUMMARY
UNION ALL
SELECT 'CORE', 'DM_PORTFOLIO_HOLDINGS_ASOF', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_CLIENT_REPORTING_CORE.DM_PORTFOLIO_HOLDINGS_ASOF
UNION ALL
SELECT 'CORE', 'DM_CLIENT_PERFORMANCE', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_CLIENT_REPORTING_CORE.DM_CLIENT_PERFORMANCE
UNION ALL
SELECT 'CORE', 'DM_CASHFLOW_SUMMARY', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_CLIENT_REPORTING_CORE.DM_CASHFLOW_SUMMARY
UNION ALL
SELECT 'EVENTS', 'FACT_CLIENT_EVENTS', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_CLIENT_REPORTING_EVENTS.FACT_CLIENT_EVENTS
UNION ALL
SELECT 'AGGREGATE', 'AGG_AUM_TIME_SERIES', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_CLIENT_REPORTING_AGGREGATES.AGG_AUM_TIME_SERIES
UNION ALL
SELECT 'AGGREGATE', 'AGG_CLIENT_OVERVIEW', COUNT(*), CURRENT_TIMESTAMP()
FROM DWSEDW.DWS_CLIENT_REPORTING_AGGREGATES.AGG_CLIENT_OVERVIEW
ORDER BY
    CASE layer
        WHEN 'SOURCE' THEN 1
        WHEN 'DIMENSION' THEN 2
        WHEN 'CORE' THEN 3
        WHEN 'EVENTS' THEN 4
        WHEN 'AGGREGATE' THEN 5
    END,
    table_name;

COMMENT ON VIEW DWS_ROW_COUNT_TRACKING IS
    'Row counts across all DWS layers (source through aggregates)';


-- ─── VIEW 21: DATA RECONCILIATION ───────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_DATA_RECONCILIATION AS
WITH source_counts AS (
    SELECT 'Holdings' AS data_domain,
        COUNT(DISTINCT account_id || '|' || fund_id) AS source_distinct_keys,
        COUNT(*) AS source_rows
    FROM DWSEDW.DWS_TRAN.FACT_PORTFOLIO_HOLDINGS
    UNION ALL
    SELECT 'Transactions',
        COUNT(DISTINCT transaction_id),
        COUNT(*)
    FROM DWSEDW.DWS_TRAN.FACT_TRANSACTIONS
    UNION ALL
    SELECT 'Clients',
        COUNT(DISTINCT client_id),
        COUNT(*)
    FROM DWSEDW.DWS_MASTER.DIM_CLIENT
),
mart_counts AS (
    SELECT 'Holdings' AS data_domain,
        COUNT(DISTINCT account_id || '|' || fund_id) AS mart_distinct_keys,
        COUNT(*) AS mart_rows
    FROM DWSEDW.DWS_CLIENT_REPORTING_CORE.DM_AUM_SUMMARY
    UNION ALL
    SELECT 'Transactions',
        COUNT(DISTINCT entity_id),
        COUNT(*)
    FROM DWSEDW.DWS_CLIENT_REPORTING_EVENTS.FACT_CLIENT_EVENTS
    UNION ALL
    SELECT 'Clients',
        COUNT(DISTINCT client_id),
        COUNT(*)
    FROM DWSEDW.DWS_CLIENT_REPORTING_DIMENSIONS.DIM_CLIENT
    WHERE is_current = TRUE
)
SELECT
    s.data_domain,
    s.source_distinct_keys,
    s.source_rows,
    m.mart_distinct_keys,
    m.mart_rows,
    s.source_distinct_keys - COALESCE(m.mart_distinct_keys, 0) AS key_difference,
    CASE
        WHEN s.source_distinct_keys = COALESCE(m.mart_distinct_keys, 0) THEN 'RECONCILED'
        WHEN ABS(s.source_distinct_keys - COALESCE(m.mart_distinct_keys, 0))
             <= s.source_distinct_keys * 0.01 THEN 'WITHIN TOLERANCE (1%)'
        ELSE 'MISMATCH - INVESTIGATE'
    END AS reconciliation_status,
    CURRENT_TIMESTAMP() AS checked_at
FROM source_counts s
LEFT JOIN mart_counts m ON s.data_domain = m.data_domain;

COMMENT ON VIEW DWS_DATA_RECONCILIATION IS
    'Source-to-mart data reconciliation by domain';


-- ─── VIEW 22: OPERATIONAL SUMMARY ───────────────────────────────────────────

CREATE OR REPLACE VIEW DWS_OPERATIONAL_SUMMARY AS
SELECT
    -- Platform health
    (SELECT platform_health FROM DWS_ALERT_SUMMARY) AS platform_health,
    (SELECT total_alerts FROM DWS_ALERT_SUMMARY) AS active_alerts,

    -- Latest run stats
    (SELECT MAX(execution_date) FROM DWS_DAILY_EXECUTION_SUMMARY) AS last_run_date,
    (SELECT success_rate_pct FROM DWS_DAILY_EXECUTION_SUMMARY
     WHERE execution_date = (SELECT MAX(execution_date) FROM DWS_DAILY_EXECUTION_SUMMARY)) AS last_run_success_rate,
    (SELECT total_minutes FROM DWS_DAILY_EXECUTION_SUMMARY
     WHERE execution_date = (SELECT MAX(execution_date) FROM DWS_DAILY_EXECUTION_SUMMARY)) AS last_run_minutes,
    (SELECT total_rows_affected FROM DWS_DAILY_EXECUTION_SUMMARY
     WHERE execution_date = (SELECT MAX(execution_date) FROM DWS_DAILY_EXECUTION_SUMMARY)) AS last_run_rows,

    -- Source freshness
    (SELECT COUNT(*) FROM DWS_SOURCE_FRESHNESS WHERE freshness_status = 'Stale') AS stale_sources,
    (SELECT COUNT(*) FROM DWS_SOURCE_FRESHNESS WHERE freshness_status = 'Fresh') AS fresh_sources,

    -- Model freshness
    (SELECT COUNT(*) FROM DWS_MODEL_FRESHNESS WHERE freshness_status = 'Stale') AS stale_models,
    (SELECT COUNT(*) FROM DWS_MODEL_FRESHNESS WHERE freshness_status = 'Fresh') AS fresh_models,

    -- Cost (last 7 days)
    (SELECT ROUND(SUM(estimated_cost_usd), 2) FROM DWS_COST_DAILY
     WHERE usage_date >= DATEADD('day', -7, CURRENT_DATE())) AS cost_last_7_days_usd,

    CURRENT_TIMESTAMP() AS generated_at;

COMMENT ON VIEW DWS_OPERATIONAL_SUMMARY IS
    'Single-pane operational summary for DWS Client Reporting platform';


-- ═══════════════════════════════════════════════════════════════════════════════
-- GRANT PERMISSIONS
-- ═══════════════════════════════════════════════════════════════════════════════

GRANT USAGE ON SCHEMA DWSEDW.DWS_MONITORING TO ROLE DWS_DEVELOPER;
GRANT SELECT ON ALL VIEWS IN SCHEMA DWSEDW.DWS_MONITORING TO ROLE DWS_DEVELOPER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DWSEDW.DWS_MONITORING TO ROLE DWS_DEVELOPER;

GRANT USAGE ON SCHEMA DWSEDW.DWS_MONITORING TO ROLE DWS_TESTER;
GRANT SELECT ON ALL VIEWS IN SCHEMA DWSEDW.DWS_MONITORING TO ROLE DWS_TESTER;

GRANT USAGE ON SCHEMA DWSEDW.DWS_MONITORING TO ROLE DWS_PROD;
GRANT SELECT ON ALL VIEWS IN SCHEMA DWSEDW.DWS_MONITORING TO ROLE DWS_PROD;


-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT 'DWS MONITORING DASHBOARD SETUP COMPLETE' AS status;

SELECT
    TABLE_NAME AS view_name,
    COMMENT AS description
FROM DWSEDW.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'DWS_MONITORING'
ORDER BY TABLE_NAME;
