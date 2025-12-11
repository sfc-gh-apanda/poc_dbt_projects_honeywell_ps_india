-- ═══════════════════════════════════════════════════════════════════════════════
-- O2C ENHANCED - COST & PERFORMANCE MONITORING VIEWS
-- ═══════════════════════════════════════════════════════════════════════════════
-- 
-- Purpose: Cost attribution, query performance, and model efficiency monitoring
-- 
-- Views Created (11 total):
--   COST MONITORING:
--     1. O2C_ENH_COST_DAILY              - Daily credit consumption
--     2. O2C_ENH_COST_BY_MODEL           - Cost attribution by model
--     3. O2C_ENH_COST_MONTHLY            - Monthly cost summary
--     4. O2C_ENH_ALERT_COST              - Cost anomaly alerts
--
--   QUERY PERFORMANCE:
--     5. O2C_ENH_LONG_RUNNING_QUERIES    - Queries >1 minute
--     6. O2C_ENH_QUEUE_TIME_ANALYSIS     - Queue time by hour
--     7. O2C_ENH_COMPILATION_ANALYSIS    - Compilation time trends
--     8. O2C_ENH_ALERT_QUEUE             - Queue time alerts
--     9. O2C_ENH_ALERT_LONG_QUERY        - Long query alerts
--
--   MODEL PERFORMANCE:
--    10. O2C_ENH_MODEL_PERFORMANCE_TREND - Execution time trends
--    11. O2C_ENH_INCREMENTAL_EFFICIENCY  - Rows/second efficiency
--
-- Prerequisites:
--   - O2C_ENHANCED_MONITORING_SETUP.sql executed
--   - dbt_o2c_enhanced project has run at least once
-- 
-- Idempotent: YES - Safe to run multiple times
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;
USE SCHEMA O2C_ENHANCED_MONITORING;

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '🚀 STARTING: Cost & Performance Monitoring Setup' AS status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 1: COST MONITORING VIEWS
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 1: COST ANALYSIS - Daily Credit Consumption
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_COST_DAILY AS
WITH daily_credits AS (
    SELECT
        DATE(start_time) AS usage_date,
        warehouse_name,
        SUM(total_elapsed_time) / 1000 / 3600 AS compute_hours,
        -- Credit estimation based on warehouse size
        SUM(
            CASE warehouse_size
                WHEN 'X-Small' THEN total_elapsed_time / 1000 / 3600 * 1
                WHEN 'Small' THEN total_elapsed_time / 1000 / 3600 * 2
                WHEN 'Medium' THEN total_elapsed_time / 1000 / 3600 * 4
                WHEN 'Large' THEN total_elapsed_time / 1000 / 3600 * 8
                WHEN 'X-Large' THEN total_elapsed_time / 1000 / 3600 * 16
                WHEN '2X-Large' THEN total_elapsed_time / 1000 / 3600 * 32
                WHEN '3X-Large' THEN total_elapsed_time / 1000 / 3600 * 64
                WHEN '4X-Large' THEN total_elapsed_time / 1000 / 3600 * 128
                ELSE total_elapsed_time / 1000 / 3600 * 1
            END
        ) AS estimated_credits,
        COUNT(DISTINCT query_id) AS query_count,
        SUM(bytes_scanned) / 1024 / 1024 / 1024 AS gb_scanned
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE database_name = 'EDW'
      AND schema_name LIKE 'O2C_ENHANCED%'
      AND start_time >= DATEADD('day', -30, CURRENT_DATE())
      AND execution_status = 'SUCCESS'
    GROUP BY usage_date, warehouse_name
)
SELECT
    usage_date,
    warehouse_name,
    ROUND(compute_hours, 4) AS compute_hours,
    ROUND(estimated_credits, 4) AS estimated_credits,
    ROUND(estimated_credits * 3.0, 2) AS estimated_cost_usd,  -- Assume $3/credit
    query_count,
    ROUND(gb_scanned, 2) AS gb_scanned,
    -- 7-day moving average for anomaly detection
    ROUND(AVG(estimated_credits) OVER (
        PARTITION BY warehouse_name 
        ORDER BY usage_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 4) AS credits_7day_avg,
    -- Variance from average
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

COMMENT ON VIEW O2C_ENH_COST_DAILY IS 
    'Daily credit consumption for O2C Enhanced with 7-day moving average and variance';

SELECT '✅ VIEW 1 CREATED: O2C_ENH_COST_DAILY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 2: COST ANALYSIS - Per Model Cost Attribution
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_COST_BY_MODEL AS
SELECT 
    model_name,
    schema_name,
    COUNT(*) AS executions,
    ROUND(SUM(total_node_runtime), 2) AS total_seconds,
    ROUND(AVG(total_node_runtime), 2) AS avg_seconds,
    ROUND(MAX(total_node_runtime), 2) AS max_seconds,
    ROUND(MIN(total_node_runtime), 2) AS min_seconds,
    SUM(rows_affected) AS total_rows,
    -- Credit estimation (assuming 1 credit per hour)
    ROUND(SUM(total_node_runtime) / 3600, 4) AS estimated_credits,
    ROUND(SUM(total_node_runtime) / 3600 * 3.0, 2) AS estimated_cost_usd,
    -- Cost per execution
    ROUND((SUM(total_node_runtime) / 3600 * 3.0) / NULLIF(COUNT(*), 0), 4) AS cost_per_execution,
    -- Cost per 1000 rows
    ROUND((SUM(total_node_runtime) / 3600 * 3.0) / NULLIF(SUM(rows_affected), 0) * 1000, 4) AS cost_per_1k_rows,
    -- Rank by cost
    RANK() OVER (ORDER BY SUM(total_node_runtime) DESC) AS cost_rank,
    -- Performance tier
    CASE 
        WHEN SUM(total_node_runtime) / 3600 * 3.0 > 10 THEN '🔴 HIGH COST'
        WHEN SUM(total_node_runtime) / 3600 * 3.0 > 5 THEN '🟡 MODERATE COST'
        WHEN SUM(total_node_runtime) / 3600 * 3.0 > 1 THEN '🟢 LOW COST'
        ELSE '⚪ MINIMAL'
    END AS cost_tier,
    MAX(run_started_at) AS last_run
FROM O2C_ENH_MODEL_EXECUTIONS
WHERE run_started_at >= DATEADD('day', -7, CURRENT_DATE())
  AND status = 'SUCCESS'
GROUP BY model_name, schema_name
ORDER BY estimated_cost_usd DESC;

COMMENT ON VIEW O2C_ENH_COST_BY_MODEL IS 
    'Cost attribution by O2C Enhanced model with efficiency metrics';

SELECT '✅ VIEW 2 CREATED: O2C_ENH_COST_BY_MODEL' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 3: COST ANALYSIS - Monthly Summary with MoM Comparison
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_COST_MONTHLY AS
WITH monthly_data AS (
    SELECT
        DATE_TRUNC('month', usage_date) AS month,
        SUM(estimated_credits) AS total_credits,
        SUM(estimated_cost_usd) AS total_cost_usd,
        SUM(query_count) AS total_queries,
        SUM(gb_scanned) AS total_gb_scanned,
        COUNT(DISTINCT usage_date) AS active_days
    FROM O2C_ENH_COST_DAILY
    GROUP BY DATE_TRUNC('month', usage_date)
)
SELECT
    month,
    ROUND(total_credits, 2) AS total_credits,
    ROUND(total_cost_usd, 2) AS total_cost_usd,
    total_queries,
    ROUND(total_gb_scanned, 2) AS total_gb_scanned,
    active_days,
    ROUND(total_cost_usd / NULLIF(active_days, 0), 2) AS avg_daily_cost_usd,
    -- Previous month comparison
    LAG(total_credits) OVER (ORDER BY month) AS prev_month_credits,
    LAG(total_cost_usd) OVER (ORDER BY month) AS prev_month_cost_usd,
    -- Month-over-month change
    ROUND(
        (total_credits - LAG(total_credits) OVER (ORDER BY month)) 
        / NULLIF(LAG(total_credits) OVER (ORDER BY month), 0) * 100, 1
    ) AS mom_credits_change_pct,
    ROUND(
        (total_cost_usd - LAG(total_cost_usd) OVER (ORDER BY month)) 
        / NULLIF(LAG(total_cost_usd) OVER (ORDER BY month), 0) * 100, 1
    ) AS mom_cost_change_pct,
    -- Budget tracking
    CASE 
        WHEN total_cost_usd > 200 THEN '🔴 OVER BUDGET'
        WHEN total_cost_usd > 100 THEN '🟡 NEAR BUDGET'
        ELSE '🟢 UNDER BUDGET'
    END AS budget_status
FROM monthly_data
ORDER BY month DESC;

COMMENT ON VIEW O2C_ENH_COST_MONTHLY IS 
    'Monthly cost summary with month-over-month comparison';

SELECT '✅ VIEW 3 CREATED: O2C_ENH_COST_MONTHLY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 4: COST ALERT VIEW - Anomaly Detection
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_COST AS
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
FROM O2C_ENH_COST_DAILY
WHERE usage_date >= DATEADD('day', -3, CURRENT_DATE())
  AND ABS(COALESCE(variance_from_avg_pct, 0)) > 25
ORDER BY variance_from_avg_pct DESC;

COMMENT ON VIEW O2C_ENH_ALERT_COST IS 
    'Cost anomaly alerts (>25% variance from 7-day average)';

SELECT '✅ VIEW 4 CREATED: O2C_ENH_ALERT_COST' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 2: QUERY PERFORMANCE MONITORING VIEWS
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 5: LONG RUNNING QUERIES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_LONG_RUNNING_QUERIES AS
SELECT 
    query_id,
    start_time,
    end_time,
    ROUND(total_elapsed_time / 1000, 2) AS elapsed_seconds,
    ROUND(execution_time / 1000, 2) AS execution_seconds,
    ROUND(compilation_time / 1000, 2) AS compilation_seconds,
    ROUND(queued_overload_time / 1000, 2) AS queue_seconds,
    ROUND(queued_provisioning_time / 1000, 2) AS provisioning_seconds,
    warehouse_name,
    warehouse_size,
    user_name,
    role_name,
    query_type,
    ROUND(bytes_scanned / 1024 / 1024, 2) AS mb_scanned,
    ROUND(bytes_written / 1024 / 1024, 2) AS mb_written,
    rows_produced,
    rows_inserted,
    rows_updated,
    rows_deleted,
    LEFT(query_text, 500) AS query_preview,
    -- Classification
    CASE 
        WHEN total_elapsed_time / 1000 > 600 THEN 'CRITICAL (>10 min)'
        WHEN total_elapsed_time / 1000 > 300 THEN 'HIGH (5-10 min)'
        WHEN total_elapsed_time / 1000 > 120 THEN 'MEDIUM (2-5 min)'
        ELSE 'LOW (1-2 min)'
    END AS severity,
    -- Time breakdown analysis
    CASE 
        WHEN queued_overload_time / 1000 > execution_time / 1000 * 0.2 THEN '⚠️ HIGH QUEUE TIME'
        WHEN compilation_time / 1000 > execution_time / 1000 * 0.3 THEN '⚠️ HIGH COMPILATION'
        ELSE '✅ EXECUTION DOMINATED'
    END AS bottleneck_analysis
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND schema_name LIKE 'O2C_ENHANCED%'
  AND total_elapsed_time / 1000 > 60  -- More than 1 minute
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND execution_status = 'SUCCESS'
ORDER BY total_elapsed_time DESC
LIMIT 100;

COMMENT ON VIEW O2C_ENH_LONG_RUNNING_QUERIES IS 
    'Long-running O2C Enhanced queries (>1 minute) with bottleneck analysis';

SELECT '✅ VIEW 5 CREATED: O2C_ENH_LONG_RUNNING_QUERIES' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 6: QUEUE TIME ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_QUEUE_TIME_ANALYSIS AS
WITH queue_stats AS (
    SELECT
        DATE_TRUNC('hour', start_time) AS hour_bucket,
        warehouse_name,
        warehouse_size,
        COUNT(*) AS query_count,
        AVG(queued_overload_time) / 1000 AS avg_queue_seconds,
        MAX(queued_overload_time) / 1000 AS max_queue_seconds,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY queued_overload_time) / 1000 AS p95_queue_seconds,
        SUM(CASE WHEN queued_overload_time > 5000 THEN 1 ELSE 0 END) AS queries_with_queue,
        AVG(total_elapsed_time) / 1000 AS avg_total_seconds
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE database_name = 'EDW'
      AND schema_name LIKE 'O2C_ENHANCED%'
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
    queries_with_queue,
    ROUND(queries_with_queue * 100.0 / NULLIF(query_count, 0), 1) AS queue_rate_pct,
    -- Queue impact on total time
    ROUND(avg_queue_seconds * 100.0 / NULLIF(avg_total_seconds, 0), 1) AS queue_impact_pct,
    CASE 
        WHEN avg_queue_seconds > 30 THEN '🔴 CRITICAL - Scale Up Immediately'
        WHEN avg_queue_seconds > 10 THEN '🟠 HIGH - Consider Scaling'
        WHEN avg_queue_seconds > 5 THEN '🟡 WARNING - Monitor Closely'
        WHEN avg_queue_seconds > 0 THEN '🟢 MODERATE'
        ELSE '⚪ HEALTHY - No Queue'
    END AS queue_status,
    -- Recommendation
    CASE 
        WHEN avg_queue_seconds > 30 THEN 'Increase warehouse size or add multi-cluster'
        WHEN avg_queue_seconds > 10 THEN 'Monitor during peak hours, consider scaling'
        WHEN avg_queue_seconds > 5 THEN 'Acceptable but watch for trends'
        ELSE 'No action needed'
    END AS recommendation
FROM queue_stats
ORDER BY hour_bucket DESC, avg_queue_seconds DESC;

COMMENT ON VIEW O2C_ENH_QUEUE_TIME_ANALYSIS IS 
    'Query queue time analysis by hour and warehouse with scaling recommendations';

SELECT '✅ VIEW 6 CREATED: O2C_ENH_QUEUE_TIME_ANALYSIS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 7: COMPILATION TIME ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_COMPILATION_ANALYSIS AS
SELECT
    DATE(start_time) AS query_date,
    COUNT(*) AS query_count,
    ROUND(AVG(compilation_time) / 1000, 2) AS avg_compilation_seconds,
    ROUND(MAX(compilation_time) / 1000, 2) AS max_compilation_seconds,
    ROUND(MIN(compilation_time) / 1000, 2) AS min_compilation_seconds,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY compilation_time) / 1000, 2) AS median_compilation_seconds,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY compilation_time) / 1000, 2) AS p95_compilation_seconds,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY compilation_time) / 1000, 2) AS p99_compilation_seconds,
    SUM(CASE WHEN compilation_time > 5000 THEN 1 ELSE 0 END) AS slow_compile_count,
    ROUND(SUM(CASE WHEN compilation_time > 5000 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS slow_compile_pct,
    -- Total compilation overhead
    ROUND(SUM(compilation_time) / 1000, 0) AS total_compilation_seconds,
    ROUND(SUM(compilation_time) / 1000 / 60, 1) AS total_compilation_minutes,
    CASE 
        WHEN AVG(compilation_time) / 1000 > 10 THEN '🔴 CRITICAL - Simplify Queries'
        WHEN AVG(compilation_time) / 1000 > 5 THEN '🟠 HIGH - Review Complex Queries'
        WHEN AVG(compilation_time) / 1000 > 2 THEN '🟡 WARNING'
        ELSE '🟢 HEALTHY'
    END AS compile_health
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND schema_name LIKE 'O2C_ENHANCED%'
  AND start_time >= DATEADD('day', -14, CURRENT_DATE())
  AND execution_status = 'SUCCESS'
GROUP BY query_date
ORDER BY query_date DESC;

COMMENT ON VIEW O2C_ENH_COMPILATION_ANALYSIS IS 
    'Query compilation time analysis with percentiles and health indicators';

SELECT '✅ VIEW 7 CREATED: O2C_ENH_COMPILATION_ANALYSIS' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 8: QUEUE TIME ALERT VIEW
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_QUEUE AS
SELECT
    hour_bucket,
    warehouse_name,
    warehouse_size,
    avg_queue_seconds,
    max_queue_seconds,
    p95_queue_seconds,
    query_count,
    queue_rate_pct,
    queue_status,
    recommendation,
    CASE 
        WHEN avg_queue_seconds > 30 THEN 'CRITICAL'
        WHEN avg_queue_seconds > 10 THEN 'HIGH'
        WHEN avg_queue_seconds > 5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    'Avg queue: ' || ROUND(avg_queue_seconds, 1) || 's, Max: ' || 
        ROUND(max_queue_seconds, 1) || 's, ' ||
        ROUND(queue_rate_pct, 0) || '% of queries queued' AS alert_description,
    CURRENT_TIMESTAMP() AS detected_at
FROM O2C_ENH_QUEUE_TIME_ANALYSIS
WHERE hour_bucket >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
  AND avg_queue_seconds > 5
ORDER BY avg_queue_seconds DESC;

COMMENT ON VIEW O2C_ENH_ALERT_QUEUE IS 
    'Queue time alerts (>5 second average queue)';

SELECT '✅ VIEW 8 CREATED: O2C_ENH_ALERT_QUEUE' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 9: LONG RUNNING QUERY ALERT VIEW
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_ALERT_LONG_QUERY AS
SELECT
    query_id,
    start_time,
    elapsed_seconds,
    execution_seconds,
    compilation_seconds,
    queue_seconds,
    warehouse_name,
    warehouse_size,
    user_name,
    query_type,
    mb_scanned,
    rows_produced,
    severity,
    bottleneck_analysis,
    'Query ran for ' || ROUND(elapsed_seconds / 60, 1) || ' min. ' ||
        'Execution: ' || execution_seconds || 's, ' ||
        'Queue: ' || queue_seconds || 's, ' ||
        'Compile: ' || compilation_seconds || 's' AS alert_description,
    query_preview
FROM O2C_ENH_LONG_RUNNING_QUERIES
WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  AND severity IN ('CRITICAL (>10 min)', 'HIGH (5-10 min)')
ORDER BY elapsed_seconds DESC;

COMMENT ON VIEW O2C_ENH_ALERT_LONG_QUERY IS 
    'Long running query alerts (>5 minutes)';

SELECT '✅ VIEW 9 CREATED: O2C_ENH_ALERT_LONG_QUERY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 3: MODEL PERFORMANCE MONITORING VIEWS
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 10: MODEL PERFORMANCE TREND
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_MODEL_PERFORMANCE_TREND AS
WITH daily_perf AS (
    SELECT
        DATE(run_started_at) AS run_date,
        model_name,
        schema_name,
        ROUND(AVG(total_node_runtime), 2) AS avg_seconds,
        ROUND(MAX(total_node_runtime), 2) AS max_seconds,
        ROUND(MIN(total_node_runtime), 2) AS min_seconds,
        ROUND(STDDEV(total_node_runtime), 2) AS stddev_seconds,
        COUNT(*) AS run_count,
        SUM(rows_affected) AS total_rows
    FROM O2C_ENH_MODEL_EXECUTIONS
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
    stddev_seconds,
    run_count,
    total_rows,
    -- 7-day moving average
    ROUND(AVG(avg_seconds) OVER (
        PARTITION BY model_name 
        ORDER BY run_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS avg_7day_ma,
    -- Baseline (first 7 days average)
    ROUND(AVG(avg_seconds) OVER (
        PARTITION BY model_name 
        ORDER BY run_date 
        ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
    ), 2) AS baseline_avg,
    -- Variance from baseline
    ROUND(
        (avg_seconds - AVG(avg_seconds) OVER (
            PARTITION BY model_name 
            ORDER BY run_date 
            ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
        )) / NULLIF(AVG(avg_seconds) OVER (
            PARTITION BY model_name 
            ORDER BY run_date 
            ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
        ), 0) * 100, 1
    ) AS variance_from_baseline_pct,
    -- Performance trend indicator
    CASE 
        WHEN avg_seconds > 1.5 * AVG(avg_seconds) OVER (
            PARTITION BY model_name 
            ORDER BY run_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) THEN '🔴 DEGRADED (>50% slower)'
        WHEN avg_seconds > 1.2 * AVG(avg_seconds) OVER (
            PARTITION BY model_name 
            ORDER BY run_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) THEN '🟠 SLOWING (>20% slower)'
        WHEN avg_seconds < 0.8 * AVG(avg_seconds) OVER (
            PARTITION BY model_name 
            ORDER BY run_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) THEN '🟢 IMPROVED (>20% faster)'
        ELSE '⚪ STABLE'
    END AS performance_trend
FROM daily_perf
ORDER BY run_date DESC, model_name;

COMMENT ON VIEW O2C_ENH_MODEL_PERFORMANCE_TREND IS 
    'Model execution time trend with 7-day moving average and baseline comparison';

SELECT '✅ VIEW 10 CREATED: O2C_ENH_MODEL_PERFORMANCE_TREND' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW 11: INCREMENTAL MODEL EFFICIENCY
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW O2C_ENH_INCREMENTAL_EFFICIENCY AS
SELECT
    model_name,
    schema_name,
    -- Determine load strategy from schema
    CASE 
        WHEN schema_name LIKE '%CORE%' THEN 'MERGE (Upsert)'
        WHEN schema_name LIKE '%EVENT%' THEN 'APPEND Only'
        WHEN schema_name LIKE '%PARTITION%' THEN 'DELETE+INSERT'
        WHEN schema_name LIKE '%DIMENSION%' THEN 'TRUNCATE+LOAD'
        WHEN schema_name LIKE '%AGGREGATE%' THEN 'TRUNCATE+LOAD'
        WHEN schema_name LIKE '%STAGING%' THEN 'VIEW (No Load)'
        ELSE 'UNKNOWN'
    END AS load_strategy,
    COUNT(*) AS run_count,
    ROUND(AVG(total_node_runtime), 2) AS avg_seconds,
    ROUND(MAX(total_node_runtime), 2) AS max_seconds,
    ROUND(MIN(total_node_runtime), 2) AS min_seconds,
    ROUND(AVG(rows_affected), 0) AS avg_rows,
    SUM(rows_affected) AS total_rows,
    -- Efficiency metrics (only for models with rows)
    CASE 
        WHEN AVG(rows_affected) > 0 AND AVG(total_node_runtime) > 0 
        THEN ROUND(AVG(rows_affected) / AVG(total_node_runtime), 0)
        ELSE NULL
    END AS rows_per_second,
    -- Time per 1000 rows
    CASE 
        WHEN AVG(rows_affected) > 0 AND AVG(total_node_runtime) > 0 
        THEN ROUND(AVG(total_node_runtime) / AVG(rows_affected) * 1000, 4)
        ELSE NULL
    END AS seconds_per_1k_rows,
    -- Cost per 1000 rows (assuming $3/credit-hour)
    CASE 
        WHEN AVG(rows_affected) > 0 AND AVG(total_node_runtime) > 0 
        THEN ROUND((AVG(total_node_runtime) / 3600 * 3.0) / AVG(rows_affected) * 1000, 6)
        ELSE NULL
    END AS cost_per_1k_rows,
    -- Efficiency status
    CASE 
        WHEN AVG(rows_affected) = 0 OR AVG(total_node_runtime) = 0 THEN '⚪ N/A (View or No Rows)'
        WHEN AVG(rows_affected) / NULLIF(AVG(total_node_runtime), 0) > 50000 THEN '🟢 HIGHLY EFFICIENT (>50K/s)'
        WHEN AVG(rows_affected) / NULLIF(AVG(total_node_runtime), 0) > 10000 THEN '🟢 EFFICIENT (10-50K/s)'
        WHEN AVG(rows_affected) / NULLIF(AVG(total_node_runtime), 0) > 1000 THEN '🟡 MODERATE (1-10K/s)'
        WHEN AVG(rows_affected) / NULLIF(AVG(total_node_runtime), 0) > 100 THEN '🟠 SLOW (100-1K/s)'
        ELSE '🔴 INEFFICIENT (<100/s) - Review'
    END AS efficiency_status,
    -- Recommendation
    CASE 
        WHEN AVG(rows_affected) = 0 THEN 'N/A'
        WHEN AVG(rows_affected) / NULLIF(AVG(total_node_runtime), 0) < 100 
        THEN 'Consider batch processing, add clustering, or review merge keys'
        WHEN AVG(rows_affected) / NULLIF(AVG(total_node_runtime), 0) < 1000 
        THEN 'Monitor performance, consider incremental optimizations'
        ELSE 'Performance is good'
    END AS recommendation,
    MAX(run_started_at) AS last_run
FROM O2C_ENH_MODEL_EXECUTIONS
WHERE status = 'SUCCESS'
  AND run_started_at >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY model_name, schema_name
ORDER BY 
    CASE efficiency_status
        WHEN '🔴 INEFFICIENT (<100/s) - Review' THEN 1
        WHEN '🟠 SLOW (100-1K/s)' THEN 2
        WHEN '🟡 MODERATE (1-10K/s)' THEN 3
        WHEN '🟢 EFFICIENT (10-50K/s)' THEN 4
        WHEN '🟢 HIGHLY EFFICIENT (>50K/s)' THEN 5
        ELSE 6
    END,
    avg_seconds DESC;

COMMENT ON VIEW O2C_ENH_INCREMENTAL_EFFICIENCY IS 
    'Incremental model efficiency analysis (rows/second) with recommendations';

SELECT '✅ VIEW 11 CREATED: O2C_ENH_INCREMENTAL_EFFICIENCY' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- GRANT PERMISSIONS
-- ═══════════════════════════════════════════════════════════════════════════════

GRANT SELECT ON ALL VIEWS IN SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE DBT_O2C_DEVELOPER;

SELECT '✅ PERMISSIONS GRANTED' AS status;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' AS separator;
SELECT '✅ COST & PERFORMANCE MONITORING SETUP COMPLETE!' AS final_status;
SELECT '═══════════════════════════════════════════════════════════════' AS separator;

-- List all views created
SELECT 
    TABLE_NAME AS view_name,
    COMMENT AS description
FROM EDW.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
  AND (TABLE_NAME LIKE '%COST%' 
       OR TABLE_NAME LIKE '%QUEUE%' 
       OR TABLE_NAME LIKE '%LONG%' 
       OR TABLE_NAME LIKE '%COMPILATION%'
       OR TABLE_NAME LIKE '%TREND%'
       OR TABLE_NAME LIKE '%EFFICIENCY%')
ORDER BY TABLE_NAME;

