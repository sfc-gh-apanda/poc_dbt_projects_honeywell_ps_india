-- ============================================================================
-- COMPREHENSIVE DBT ALERTS & MONITORING SYSTEM
-- ============================================================================
-- Purpose: Complete alert and notification system for production dbt monitoring
-- Dependencies: dbt_artifacts package, SNOWFLAKE.ACCOUNT_USAGE access
-- Idempotent: Yes - uses CREATE OR REPLACE for all objects
-- ============================================================================

-- PREREQUISITES:
-- 1. Run: dbt deps (to install packages)
-- 2. Run: dbt run (to create artifact tables)
-- 3. Run: setup_observability_dashboard.sql (to create base monitoring views)
-- 4. Grant ACCOUNT_USAGE access: GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;
-- ============================================================================

USE ROLE ACCOUNTADMIN; -- Or appropriate role
USE DATABASE EDW;
USE SCHEMA DBT_MONITORING;

-- ============================================================================
-- SECTION 1: DATA QUALITY ALERTS
-- ============================================================================

-- Alert 1.1: Critical Test Failures (High Severity)
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_CRITICAL_TEST_FAILURES AS
SELECT 
    generated_at,
    node_id,
    SPLIT_PART(node_id, '.', -1) as test_name,
    status,
    message,
    execution_time,
    -- Severity classification based on test type
    CASE 
        WHEN LOWER(node_id) LIKE '%unique%' THEN 'CRITICAL'
        WHEN LOWER(node_id) LIKE '%not_null%' THEN 'CRITICAL'
        WHEN LOWER(node_id) LIKE '%relationships%' THEN 'HIGH'
        WHEN LOWER(node_id) LIKE '%accepted_values%' THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    -- Add context
    CASE 
        WHEN LOWER(node_id) LIKE '%unique%' THEN 'Duplicate records detected - data integrity issue'
        WHEN LOWER(node_id) LIKE '%not_null%' THEN 'Null values in required field - data completeness issue'
        WHEN LOWER(node_id) LIKE '%relationships%' THEN 'Referential integrity violation - orphaned records'
        ELSE 'Data quality check failed'
    END as alert_description
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE status IN ('fail', 'error')
  AND generated_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY 
    CASE 
        WHEN LOWER(node_id) LIKE '%unique%' THEN 1
        WHEN LOWER(node_id) LIKE '%not_null%' THEN 2
        WHEN LOWER(node_id) LIKE '%relationships%' THEN 3
        ELSE 4
    END,
    generated_at DESC;

COMMENT ON VIEW ALERT_CRITICAL_TEST_FAILURES IS 
    'Critical test failures in the last hour - requires immediate attention';

-- Alert 1.2: Test Pass Rate Degradation
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_TEST_PASS_RATE_DROP AS
WITH daily_pass_rates AS (
    SELECT 
        DATE_TRUNC('day', generated_at) as test_date,
        COUNT(CASE WHEN status = 'pass' THEN 1 END)::FLOAT / NULLIF(COUNT(*), 0) * 100 as pass_rate,
        COUNT(*) as total_tests,
        COUNT(CASE WHEN status = 'pass' THEN 1 END) as passed_tests,
        COUNT(CASE WHEN status IN ('fail', 'error') THEN 1 END) as failed_tests
    FROM DBT_ARTIFACTS.TEST_EXECUTIONS
    WHERE generated_at >= DATEADD(day, -14, CURRENT_DATE())
    GROUP BY 1
),
baseline AS (
    SELECT 
        AVG(pass_rate) as baseline_pass_rate,
        STDDEV(pass_rate) as stddev_pass_rate
    FROM daily_pass_rates
    WHERE test_date BETWEEN DATEADD(day, -14, CURRENT_DATE()) 
                        AND DATEADD(day, -7, CURRENT_DATE())
),
today_rate AS (
    SELECT 
        pass_rate as today_pass_rate,
        total_tests,
        passed_tests,
        failed_tests
    FROM daily_pass_rates
    WHERE test_date = CURRENT_DATE()
)
SELECT 
    CURRENT_DATE() as alert_date,
    ROUND(t.today_pass_rate, 2) as today_pass_rate,
    ROUND(b.baseline_pass_rate, 2) as baseline_pass_rate,
    ROUND(t.today_pass_rate - b.baseline_pass_rate, 2) as pass_rate_change,
    t.total_tests,
    t.passed_tests,
    t.failed_tests,
    CASE 
        WHEN t.today_pass_rate < b.baseline_pass_rate - 15 THEN 'CRITICAL'
        WHEN t.today_pass_rate < b.baseline_pass_rate - 10 THEN 'HIGH'
        WHEN t.today_pass_rate < b.baseline_pass_rate - 5 THEN 'MEDIUM'
        ELSE 'OK'
    END as severity,
    CASE 
        WHEN t.today_pass_rate < b.baseline_pass_rate - 15 THEN 
            'CRITICAL: Test pass rate dropped by ' || ROUND(b.baseline_pass_rate - t.today_pass_rate, 1) || '% - investigate immediately'
        WHEN t.today_pass_rate < b.baseline_pass_rate - 10 THEN 
            'WARNING: Test pass rate degradation detected'
        ELSE 'Test pass rate below baseline'
    END as alert_description
FROM today_rate t
CROSS JOIN baseline b
WHERE t.today_pass_rate < b.baseline_pass_rate - 5;

COMMENT ON VIEW ALERT_TEST_PASS_RATE_DROP IS 
    'Alerts when test pass rate drops significantly from baseline';

-- Alert 1.3: Recurring Test Failures
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_RECURRING_TEST_FAILURES AS
WITH test_failure_history AS (
    SELECT 
        node_id,
        SPLIT_PART(node_id, '.', -1) as test_name,
        COUNT(*) as failure_count,
        MIN(generated_at) as first_failure,
        MAX(generated_at) as last_failure,
        DATEDIFF('day', MIN(generated_at), MAX(generated_at)) as days_failing
    FROM DBT_ARTIFACTS.TEST_EXECUTIONS
    WHERE status IN ('fail', 'error')
      AND generated_at >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY 1, 2
    HAVING COUNT(*) >= 3 -- Failed at least 3 times
)
SELECT 
    node_id,
    test_name,
    failure_count,
    first_failure,
    last_failure,
    days_failing,
    CASE 
        WHEN failure_count >= 10 THEN 'CRITICAL'
        WHEN failure_count >= 5 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as severity,
    'Test has failed ' || failure_count || ' times in the last 7 days - may indicate systemic issue' as alert_description
FROM test_failure_history
ORDER BY failure_count DESC, last_failure DESC;

COMMENT ON VIEW ALERT_RECURRING_TEST_FAILURES IS 
    'Tests that are failing repeatedly - indicates systemic data quality issues';

-- ============================================================================
-- SECTION 2: PERFORMANCE ALERTS
-- ============================================================================

-- Alert 2.1: Critical Performance Degradation
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_CRITICAL_PERFORMANCE AS
WITH model_baseline AS (
    SELECT 
        node_id,
        AVG(execution_time) as baseline_avg,
        STDDEV(execution_time) as baseline_stddev,
        COUNT(*) as baseline_runs
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at BETWEEN DATEADD(day, -14, CURRENT_DATE()) 
                           AND DATEADD(day, -7, CURRENT_DATE())
      AND status = 'success'
    GROUP BY node_id
    HAVING COUNT(*) >= 3 -- Need at least 3 runs for baseline
),
recent_runs AS (
    SELECT 
        node_id,
        AVG(execution_time) as recent_avg,
        MAX(execution_time) as recent_max,
        MIN(execution_time) as recent_min,
        COUNT(*) as recent_run_count
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
      AND status = 'success'
    GROUP BY node_id
)
SELECT 
    SPLIT_PART(r.node_id, '.', -1) as model_name,
    r.node_id,
    ROUND(b.baseline_avg, 2) as baseline_seconds,
    ROUND(r.recent_avg, 2) as recent_avg_seconds,
    ROUND(r.recent_max, 2) as recent_max_seconds,
    ROUND(r.recent_min, 2) as recent_min_seconds,
    r.recent_run_count,
    ROUND((r.recent_avg - b.baseline_avg), 2) as seconds_slower,
    ROUND((r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) * 100, 1) as percent_slower,
    CASE 
        WHEN r.recent_avg > 300 AND (r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) > 0.5 
            THEN 'CRITICAL'
        WHEN (r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) > 1.0 
            THEN 'HIGH'
        WHEN (r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) > 0.5 
            THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    CASE 
        WHEN r.recent_avg > 300 AND (r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) > 0.5 
            THEN 'Model taking ' || ROUND(r.recent_avg/60, 1) || ' minutes - ' || ROUND(percent_slower, 0) || '% slower than baseline'
        ELSE 'Performance degradation: ' || ROUND(percent_slower, 0) || '% slower than baseline'
    END as alert_description
FROM recent_runs r
JOIN model_baseline b ON r.node_id = b.node_id
WHERE r.recent_avg > b.baseline_avg + (2 * COALESCE(b.baseline_stddev, 0))
   OR r.recent_avg > 300 -- Always alert if > 5 minutes
ORDER BY 
    CASE 
        WHEN r.recent_avg > 300 AND (r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) > 0.5 THEN 1
        WHEN (r.recent_avg - b.baseline_avg) / NULLIF(b.baseline_avg, 0) > 1.0 THEN 2
        ELSE 3
    END,
    percent_slower DESC;

COMMENT ON VIEW ALERT_CRITICAL_PERFORMANCE IS 
    'Models running significantly slower than baseline (2-sigma threshold)';

-- Alert 2.2: Model Execution Failures
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_MODEL_FAILURES AS
SELECT 
    generated_at,
    node_id,
    SPLIT_PART(node_id, '.', -1) as model_name,
    status,
    execution_time,
    message,
    -- Check if this is a recurring failure
    (SELECT COUNT(*) 
     FROM DBT_ARTIFACTS.MODEL_EXECUTIONS m2 
     WHERE m2.node_id = m1.node_id 
       AND m2.status = 'error'
       AND m2.generated_at >= DATEADD(day, -7, CURRENT_DATE())
    ) as failure_count_last_7_days,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM DBT_ARTIFACTS.MODEL_EXECUTIONS m2 
              WHERE m2.node_id = m1.node_id 
                AND m2.status = 'error'
                AND m2.generated_at >= DATEADD(day, -7, CURRENT_DATE())
             ) >= 3 THEN 'CRITICAL'
        ELSE 'HIGH'
    END as severity,
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM DBT_ARTIFACTS.MODEL_EXECUTIONS m2 
              WHERE m2.node_id = m1.node_id 
                AND m2.status = 'error'
                AND m2.generated_at >= DATEADD(day, -7, CURRENT_DATE())
             ) >= 3 THEN 'Recurring failure - failed ' || 
                        (SELECT COUNT(*) 
                         FROM DBT_ARTIFACTS.MODEL_EXECUTIONS m2 
                         WHERE m2.node_id = m1.node_id 
                           AND m2.status = 'error'
                           AND m2.generated_at >= DATEADD(day, -7, CURRENT_DATE())
                        ) || ' times in last 7 days'
        ELSE 'Model execution failed'
    END as alert_description
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS m1
WHERE status = 'error'
  AND generated_at >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
ORDER BY failure_count_last_7_days DESC, generated_at DESC;

COMMENT ON VIEW ALERT_MODEL_FAILURES IS 
    'Model execution failures with recurring failure detection';

-- Alert 2.3: Long-Running Queries
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_LONG_RUNNING_QUERIES AS
SELECT 
    query_id,
    query_text,
    user_name,
    warehouse_name,
    database_name,
    schema_name,
    start_time,
    end_time,
    execution_time/1000 as execution_seconds,
    ROUND(execution_time/1000/60, 2) as execution_minutes,
    total_elapsed_time/1000 as total_seconds,
    rows_produced,
    bytes_scanned,
    CASE 
        WHEN execution_time/1000 > 1800 THEN 'CRITICAL' -- > 30 minutes
        WHEN execution_time/1000 > 900 THEN 'HIGH'      -- > 15 minutes
        WHEN execution_time/1000 > 300 THEN 'MEDIUM'    -- > 5 minutes
        ELSE 'LOW'
    END as severity,
    'Query ran for ' || ROUND(execution_time/1000/60, 1) || ' minutes' as alert_description
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE '%dbt%'
  AND start_time >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
  AND execution_time/1000 > 300 -- > 5 minutes
ORDER BY execution_time DESC
LIMIT 20;

COMMENT ON VIEW ALERT_LONG_RUNNING_QUERIES IS 
    'DBT queries taking longer than 5 minutes';

-- ============================================================================
-- SECTION 3: DATA FRESHNESS ALERTS
-- ============================================================================

-- Alert 3.1: Stale Data Sources
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_STALE_SOURCES AS
SELECT 
    node_id,
    SPLIT_PART(node_id, '.', -1) as source_name,
    max_loaded_at,
    snapshotted_at,
    CURRENT_TIMESTAMP() as checked_at,
    DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) as hours_stale,
    DATEDIFF('day', max_loaded_at, CURRENT_TIMESTAMP()) as days_stale,
    status,
    CASE 
        WHEN DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) > 72 THEN 'CRITICAL' -- > 3 days
        WHEN DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) > 48 THEN 'HIGH'     -- > 2 days
        WHEN DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) > 24 THEN 'MEDIUM'   -- > 1 day
        ELSE 'LOW'
    END as severity,
    CASE 
        WHEN DATEDIFF('day', max_loaded_at, CURRENT_TIMESTAMP()) >= 1 THEN
            'Data is ' || DATEDIFF('day', max_loaded_at, CURRENT_TIMESTAMP()) || ' days stale'
        ELSE
            'Data is ' || DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) || ' hours stale'
    END as alert_description
FROM DBT_ARTIFACTS.SOURCE_FRESHNESS_EXECUTIONS
WHERE generated_at >= DATEADD(day, -1, CURRENT_DATE())
  AND (status = 'error' OR DATEDIFF('hour', max_loaded_at, CURRENT_TIMESTAMP()) > 24)
QUALIFY ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY generated_at DESC) = 1
ORDER BY hours_stale DESC;

COMMENT ON VIEW ALERT_STALE_SOURCES IS 
    'Data sources that are stale (not updated in expected timeframe)';

-- Alert 3.2: Missing Source Data Loads
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_MISSING_DATA_LOADS AS
WITH expected_loads AS (
    -- Get sources that normally load daily
    SELECT 
        node_id,
        SPLIT_PART(node_id, '.', -1) as source_name,
        MAX(max_loaded_at) as last_load_time,
        COUNT(DISTINCT DATE_TRUNC('day', max_loaded_at)) as load_days_last_week
    FROM DBT_ARTIFACTS.SOURCE_FRESHNESS_EXECUTIONS
    WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT DATE_TRUNC('day', max_loaded_at)) >= 5 -- Loaded at least 5 days
)
SELECT 
    node_id,
    source_name,
    last_load_time,
    DATEDIFF('hour', last_load_time, CURRENT_TIMESTAMP()) as hours_since_last_load,
    load_days_last_week,
    CASE 
        WHEN DATEDIFF('hour', last_load_time, CURRENT_TIMESTAMP()) > 48 THEN 'CRITICAL'
        WHEN DATEDIFF('hour', last_load_time, CURRENT_TIMESTAMP()) > 36 THEN 'HIGH'
        WHEN DATEDIFF('hour', last_load_time, CURRENT_TIMESTAMP()) > 24 THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    'Expected daily load missing - last load was ' || 
    DATEDIFF('hour', last_load_time, CURRENT_TIMESTAMP()) || ' hours ago' as alert_description
FROM expected_loads
WHERE DATEDIFF('hour', last_load_time, CURRENT_TIMESTAMP()) > 24
ORDER BY hours_since_last_load DESC;

COMMENT ON VIEW ALERT_MISSING_DATA_LOADS IS 
    'Sources with expected regular loads that are missing';

-- ============================================================================
-- SECTION 4: COST & RESOURCE ALERTS
-- ============================================================================

-- Alert 4.1: Cost Spikes
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_COST_SPIKES AS
WITH daily_costs AS (
    SELECT 
        DATE_TRUNC('day', start_time) as cost_date,
        SUM(credits_used) as daily_credits,
        COUNT(*) as query_count,
        SUM(execution_time)/1000/60 as total_minutes
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE query_tag LIKE '%dbt%'
      AND start_time >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY 1
),
baseline AS (
    SELECT 
        AVG(daily_credits) as avg_credits,
        STDDEV(daily_credits) as stddev_credits,
        MAX(daily_credits) as max_credits
    FROM daily_costs
    WHERE cost_date BETWEEN DATEADD(day, -14, CURRENT_DATE()) 
                        AND DATEADD(day, -7, CURRENT_DATE())
),
today_cost AS (
    SELECT 
        daily_credits as today_credits,
        query_count as today_queries,
        total_minutes as today_minutes
    FROM daily_costs
    WHERE cost_date = CURRENT_DATE()
)
SELECT 
    CURRENT_DATE() as alert_date,
    ROUND(t.today_credits, 4) as today_credits,
    ROUND(b.avg_credits, 4) as baseline_avg_credits,
    ROUND(b.stddev_credits, 4) as baseline_stddev,
    ROUND(t.today_credits - b.avg_credits, 4) as credits_above_baseline,
    ROUND((t.today_credits - b.avg_credits) / NULLIF(b.avg_credits, 0) * 100, 1) as percent_increase,
    t.today_queries,
    ROUND(t.today_minutes, 2) as today_minutes,
    CASE 
        WHEN t.today_credits > b.avg_credits + (3 * b.stddev_credits) THEN 'CRITICAL'
        WHEN t.today_credits > b.avg_credits + (2 * b.stddev_credits) THEN 'HIGH'
        WHEN t.today_credits > b.avg_credits + b.stddev_credits THEN 'MEDIUM'
        ELSE 'OK'
    END as severity,
    CASE 
        WHEN t.today_credits > b.avg_credits + (3 * b.stddev_credits) THEN
            'CRITICAL: Cost spike detected - ' || ROUND(percent_increase, 0) || '% above baseline (' || 
            ROUND(t.today_credits - b.avg_credits, 2) || ' extra credits)'
        WHEN t.today_credits > b.avg_credits + (2 * b.stddev_credits) THEN
            'WARNING: Elevated costs - ' || ROUND(percent_increase, 0) || '% above baseline'
        ELSE
            'Costs above normal - ' || ROUND(percent_increase, 0) || '% increase'
    END as alert_description
FROM today_cost t
CROSS JOIN baseline b
WHERE t.today_credits > b.avg_credits + b.stddev_credits;

COMMENT ON VIEW ALERT_COST_SPIKES IS 
    'Alerts when daily DBT costs spike above baseline (statistical anomaly detection)';

-- Alert 4.2: Expensive Individual Queries
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_EXPENSIVE_QUERIES AS
SELECT 
    query_id,
    LEFT(query_text, 200) || '...' as query_text_preview,
    user_name,
    warehouse_name,
    database_name,
    start_time,
    execution_time/1000 as execution_seconds,
    credits_used,
    bytes_scanned,
    rows_produced,
    CASE 
        WHEN credits_used > 10 THEN 'CRITICAL'
        WHEN credits_used > 5 THEN 'HIGH'
        WHEN credits_used > 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    'Query consumed ' || ROUND(credits_used, 4) || ' credits - investigate for optimization' as alert_description
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE '%dbt%'
  AND start_time >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
  AND credits_used > 2
ORDER BY credits_used DESC
LIMIT 20;

COMMENT ON VIEW ALERT_EXPENSIVE_QUERIES IS 
    'Individual queries consuming excessive credits (>2 credits)';

-- Alert 4.3: Warehouse Queuing
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_WAREHOUSE_QUEUING AS
SELECT 
    warehouse_name,
    start_time,
    query_id,
    user_name,
    queued_provisioning_time/1000 as provisioning_seconds,
    queued_repair_time/1000 as repair_seconds,
    queued_overload_time/1000 as overload_seconds,
    (queued_provisioning_time + queued_repair_time + queued_overload_time)/1000 as total_queue_seconds,
    execution_time/1000 as execution_seconds,
    CASE 
        WHEN (queued_provisioning_time + queued_repair_time + queued_overload_time)/1000 > 300 THEN 'CRITICAL'
        WHEN (queued_provisioning_time + queued_repair_time + queued_overload_time)/1000 > 120 THEN 'HIGH'
        WHEN (queued_provisioning_time + queued_repair_time + queued_overload_time)/1000 > 60 THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    'Query waited ' || ROUND((queued_provisioning_time + queued_repair_time + queued_overload_time)/1000/60, 1) || 
    ' minutes in queue - consider warehouse scaling' as alert_description
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE '%dbt%'
  AND start_time >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
  AND (queued_provisioning_time + queued_repair_time + queued_overload_time) > 60000 -- > 1 minute
ORDER BY total_queue_seconds DESC
LIMIT 20;

COMMENT ON VIEW ALERT_WAREHOUSE_QUEUING IS 
    'Queries experiencing significant queue times - warehouse may be undersized';

-- ============================================================================
-- SECTION 5: SLA & EXECUTION TIMELINE ALERTS
-- ============================================================================

-- Alert 5.1: SLA Violations (Models not completing on time)
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_SLA_VIOLATIONS AS
WITH model_sla AS (
    -- Define SLA thresholds (customize per your requirements)
    SELECT 'dm_fin_ar_aging_simple' as model_pattern, 120 as sla_seconds
    UNION ALL SELECT 'dim_customer', 60
    UNION ALL SELECT 'dim_fiscal_calendar', 30
    -- Add more SLAs as needed
),
recent_executions AS (
    SELECT 
        node_id,
        SPLIT_PART(node_id, '.', -1) as model_name,
        execution_time,
        generated_at,
        status
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATEADD(hour, -4, CURRENT_TIMESTAMP())
      AND status = 'success'
)
SELECT 
    r.model_name,
    r.node_id,
    r.execution_time as actual_seconds,
    s.sla_seconds,
    r.execution_time - s.sla_seconds as seconds_over_sla,
    ROUND((r.execution_time - s.sla_seconds) / s.sla_seconds * 100, 1) as percent_over_sla,
    r.generated_at,
    CASE 
        WHEN r.execution_time > s.sla_seconds * 2 THEN 'CRITICAL'
        WHEN r.execution_time > s.sla_seconds * 1.5 THEN 'HIGH'
        WHEN r.execution_time > s.sla_seconds THEN 'MEDIUM'
        ELSE 'OK'
    END as severity,
    'Model exceeded SLA by ' || ROUND(r.execution_time - s.sla_seconds, 1) || 
    ' seconds (' || ROUND(percent_over_sla, 0) || '%)' as alert_description
FROM recent_executions r
JOIN model_sla s ON r.model_name LIKE '%' || s.model_pattern || '%'
WHERE r.execution_time > s.sla_seconds
ORDER BY percent_over_sla DESC;

COMMENT ON VIEW ALERT_SLA_VIOLATIONS IS 
    'Models that exceeded their defined SLA thresholds';

-- Alert 5.2: Late Running Jobs
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_LATE_RUNNING_JOBS AS
WITH hourly_execution_pattern AS (
    -- Identify typical execution times
    SELECT 
        HOUR(generated_at) as typical_hour,
        COUNT(*) as execution_count
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE generated_at >= DATEADD(day, -14, CURRENT_DATE())
      AND generated_at < DATEADD(day, -1, CURRENT_DATE())
    GROUP BY 1
    HAVING COUNT(*) >= 5 -- Need consistent pattern
),
expected_completion AS (
    SELECT MAX(typical_hour) + 1 as expected_hour
    FROM hourly_execution_pattern
    WHERE execution_count = (SELECT MAX(execution_count) FROM hourly_execution_pattern)
),
today_completion AS (
    SELECT MAX(HOUR(generated_at)) as actual_hour
    FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
    WHERE DATE(generated_at) = CURRENT_DATE()
)
SELECT 
    CURRENT_TIMESTAMP() as alert_time,
    e.expected_hour,
    t.actual_hour,
    HOUR(CURRENT_TIMESTAMP()) as current_hour,
    CASE 
        WHEN t.actual_hour IS NULL AND HOUR(CURRENT_TIMESTAMP()) > e.expected_hour + 2 THEN 'CRITICAL'
        WHEN t.actual_hour IS NULL AND HOUR(CURRENT_TIMESTAMP()) > e.expected_hour + 1 THEN 'HIGH'
        WHEN t.actual_hour > e.expected_hour + 1 THEN 'MEDIUM'
        ELSE 'OK'
    END as severity,
    CASE 
        WHEN t.actual_hour IS NULL THEN 
            'DBT job has not completed yet - expected by hour ' || e.expected_hour
        ELSE 
            'DBT job completed late at hour ' || t.actual_hour || ' (expected: ' || e.expected_hour || ')'
    END as alert_description
FROM expected_completion e
CROSS JOIN today_completion t
WHERE t.actual_hour IS NULL AND HOUR(CURRENT_TIMESTAMP()) > e.expected_hour + 1
   OR t.actual_hour > e.expected_hour + 1;

COMMENT ON VIEW ALERT_LATE_RUNNING_JOBS IS 
    'Alerts when DBT jobs are running later than usual';

-- ============================================================================
-- SECTION 6: COMPOSITE ALERT VIEWS (For Notification Systems)
-- ============================================================================

-- Composite Alert 6.1: All Critical Alerts (Single View)
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_ALL_CRITICAL AS
SELECT 'TEST_FAILURE' as alert_category, severity, test_name as alert_subject, 
       alert_description, generated_at as alert_time
FROM ALERT_CRITICAL_TEST_FAILURES
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

SELECT 'PERFORMANCE' as alert_category, severity, model_name as alert_subject,
       alert_description, CURRENT_TIMESTAMP() as alert_time
FROM ALERT_CRITICAL_PERFORMANCE
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

SELECT 'MODEL_FAILURE' as alert_category, severity, model_name as alert_subject,
       alert_description, generated_at as alert_time
FROM ALERT_MODEL_FAILURES
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

SELECT 'DATA_FRESHNESS' as alert_category, severity, source_name as alert_subject,
       alert_description, checked_at as alert_time
FROM ALERT_STALE_SOURCES
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

SELECT 'COST_SPIKE' as alert_category, severity, 'Daily Cost' as alert_subject,
       alert_description, alert_date as alert_time
FROM ALERT_COST_SPIKES
WHERE severity IN ('CRITICAL', 'HIGH')

UNION ALL

SELECT 'SLA_VIOLATION' as alert_category, severity, model_name as alert_subject,
       alert_description, generated_at as alert_time
FROM ALERT_SLA_VIOLATIONS
WHERE severity IN ('CRITICAL', 'HIGH')

ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        ELSE 4
    END,
    alert_time DESC;

COMMENT ON VIEW ALERT_ALL_CRITICAL IS 
    'Unified view of all critical and high severity alerts across all categories';

-- Composite Alert 6.2: Alert Summary Dashboard
-- ============================================================================
CREATE OR REPLACE VIEW ALERT_SUMMARY_DASHBOARD AS
SELECT 
    CURRENT_TIMESTAMP() as snapshot_time,
    
    -- Test Failures
    (SELECT COUNT(*) FROM ALERT_CRITICAL_TEST_FAILURES WHERE severity = 'CRITICAL') as critical_test_failures,
    (SELECT COUNT(*) FROM ALERT_CRITICAL_TEST_FAILURES WHERE severity = 'HIGH') as high_test_failures,
    (SELECT COUNT(*) FROM ALERT_RECURRING_TEST_FAILURES) as recurring_test_failures,
    
    -- Performance Issues
    (SELECT COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE WHERE severity = 'CRITICAL') as critical_performance_issues,
    (SELECT COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE WHERE severity = 'HIGH') as high_performance_issues,
    (SELECT COUNT(*) FROM ALERT_MODEL_FAILURES) as model_failures,
    
    -- Data Freshness
    (SELECT COUNT(*) FROM ALERT_STALE_SOURCES WHERE severity IN ('CRITICAL', 'HIGH')) as stale_sources,
    (SELECT COUNT(*) FROM ALERT_MISSING_DATA_LOADS) as missing_data_loads,
    
    -- Cost & Resources
    (SELECT COUNT(*) FROM ALERT_COST_SPIKES) as cost_spikes,
    (SELECT COUNT(*) FROM ALERT_EXPENSIVE_QUERIES) as expensive_queries,
    (SELECT COUNT(*) FROM ALERT_WAREHOUSE_QUEUING WHERE severity IN ('CRITICAL', 'HIGH')) as warehouse_queuing_issues,
    
    -- SLA
    (SELECT COUNT(*) FROM ALERT_SLA_VIOLATIONS WHERE severity IN ('CRITICAL', 'HIGH')) as sla_violations,
    
    -- Overall Health Score (0-100)
    100 - (
        (SELECT COUNT(*) FROM ALERT_CRITICAL_TEST_FAILURES WHERE severity = 'CRITICAL') * 10 +
        (SELECT COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE WHERE severity = 'CRITICAL') * 8 +
        (SELECT COUNT(*) FROM ALERT_MODEL_FAILURES) * 15 +
        (SELECT COUNT(*) FROM ALERT_STALE_SOURCES WHERE severity = 'CRITICAL') * 7 +
        (SELECT COUNT(*) FROM ALERT_COST_SPIKES WHERE severity = 'CRITICAL') * 5 +
        (SELECT COUNT(*) FROM ALERT_SLA_VIOLATIONS WHERE severity = 'CRITICAL') * 5
    ) as health_score;

COMMENT ON VIEW ALERT_SUMMARY_DASHBOARD IS 
    'Summary of all alert counts and overall system health score';

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 
    'COMPREHENSIVE ALERTS SETUP COMPLETE' as status,
    COUNT(*) as alert_views_created
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'DBT_MONITORING'
  AND TABLE_NAME LIKE 'ALERT_%';

-- Test all alert views
SELECT 'ALERT_CRITICAL_TEST_FAILURES' as view_name, COUNT(*) as alert_count FROM ALERT_CRITICAL_TEST_FAILURES
UNION ALL SELECT 'ALERT_TEST_PASS_RATE_DROP', COUNT(*) FROM ALERT_TEST_PASS_RATE_DROP
UNION ALL SELECT 'ALERT_RECURRING_TEST_FAILURES', COUNT(*) FROM ALERT_RECURRING_TEST_FAILURES
UNION ALL SELECT 'ALERT_CRITICAL_PERFORMANCE', COUNT(*) FROM ALERT_CRITICAL_PERFORMANCE
UNION ALL SELECT 'ALERT_MODEL_FAILURES', COUNT(*) FROM ALERT_MODEL_FAILURES
UNION ALL SELECT 'ALERT_LONG_RUNNING_QUERIES', COUNT(*) FROM ALERT_LONG_RUNNING_QUERIES
UNION ALL SELECT 'ALERT_STALE_SOURCES', COUNT(*) FROM ALERT_STALE_SOURCES
UNION ALL SELECT 'ALERT_MISSING_DATA_LOADS', COUNT(*) FROM ALERT_MISSING_DATA_LOADS
UNION ALL SELECT 'ALERT_COST_SPIKES', COUNT(*) FROM ALERT_COST_SPIKES
UNION ALL SELECT 'ALERT_EXPENSIVE_QUERIES', COUNT(*) FROM ALERT_EXPENSIVE_QUERIES
UNION ALL SELECT 'ALERT_WAREHOUSE_QUEUING', COUNT(*) FROM ALERT_WAREHOUSE_QUEUING
UNION ALL SELECT 'ALERT_SLA_VIOLATIONS', COUNT(*) FROM ALERT_SLA_VIOLATIONS
UNION ALL SELECT 'ALERT_ALL_CRITICAL', COUNT(*) FROM ALERT_ALL_CRITICAL
UNION ALL SELECT 'ALERT_SUMMARY_DASHBOARD', COUNT(*) FROM ALERT_SUMMARY_DASHBOARD
ORDER BY view_name;

