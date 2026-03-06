# DWS Client Reporting -- Snowsight Dashboard Tiles Guide

## Overview

This document provides the complete set of queries, visualization configurations, and layout guidance for building the **DWS Client Reporting Observability Dashboard** in Snowsight. The dashboard covers **8 monitoring domains** with **22 tiles** providing end-to-end visibility into execution, freshness, quality, performance, cost, and alerting.

**Prerequisites:**
- `DWS_LOAD_SAMPLE_DATA.sql` executed (databases + source data)
- `DWS_AUDIT_SETUP.sql` executed (audit tables in all environments)
- `dbt build --target dev` completed successfully (at least once)
- `DWS_MONITORING_DASHBOARD.sql` executed (creates monitoring views)

**Database:** All queries target `DWS_EDWDEV` (dev environment).

---

## Dashboard Layout

| Row | Tiles | Purpose |
|-----|-------|---------|
| **Row 1** | Platform Health, Last Run Status, Active Alerts | At-a-glance health |
| **Row 2** | Daily Execution Trend, Success Rate Trend | Execution over time |
| **Row 3** | Model Execution Log, Slowest Models | Execution details |
| **Row 4** | Source Freshness, Model Freshness | Data currency |
| **Row 5** | Test Pass Rate Trend, Test Summary by Type | Quality gates |
| **Row 6** | Row Count Tracking, Data Reconciliation | Data completeness |
| **Row 7** | Audit Run History, Audit Model Log | dbt hook-based logging |
| **Row 8** | Daily Cost, Cost by Model | Cost attribution |
| **Row 9** | Performance Trend, Long Running Queries | Query-level insights |
| **Row 10** | Queue Time Analysis, Recurring Test Failures, Cost Alerts | Deep diagnostics |

---

## Section 1: Platform Health (Row 1)

### Tile 1 -- Platform Health Scorecard

**Domain:** Overall Health
**Source:** `DWS_MONITORING.DWS_OPERATIONAL_SUMMARY`
**Chart Type:** Scorecard
**Primary Metric:** `platform_health`

```sql
SELECT
    platform_health,
    active_alerts,
    last_run_date,
    last_run_success_rate AS success_rate_pct,
    ROUND(last_run_minutes, 1) AS last_run_minutes,
    last_run_rows,
    fresh_sources || '/' || (fresh_sources + stale_sources) AS source_health,
    fresh_models  || '/' || (fresh_models  + stale_models)  AS model_health,
    cost_last_7_days_usd
FROM DWS_EDWDEV.DWS_MONITORING.DWS_OPERATIONAL_SUMMARY;
```

**Visualization:**
- Scorecard showing `platform_health` (GREEN / AMBER / RED)
- Subtitle: `active_alerts` count
- Width: 4 columns

---

### Tile 2 -- Last Run Status

**Domain:** Execution Tracking
**Source:** `DWS_AUDIT.DBT_RUN_LOG`
**Chart Type:** Scorecard
**Primary Metric:** `run_status`

```sql
SELECT
    run_status,
    environment,
    run_started_at,
    run_duration_seconds,
    models_run,
    models_success,
    models_failed,
    models_skipped,
    ROUND(models_success * 100.0 / NULLIF(models_run, 0), 1) AS success_rate_pct
FROM DWS_EDWDEV.DWS_AUDIT.DBT_RUN_LOG
ORDER BY run_started_at DESC
LIMIT 1;
```

**Visualization:**
- Scorecard showing `run_status` (SUCCESS / FAILED)
- Subtitle: `models_success` || '/' || `models_run` || ' models passed'
- Width: 4 columns

---

### Tile 3 -- Alert Summary

**Domain:** Alerting
**Source:** `DWS_MONITORING.DWS_ALERT_SUMMARY`
**Chart Type:** Scorecard
**Primary Metric:** `total_alerts`

```sql
SELECT
    total_alerts,
    critical_alerts,
    high_alerts,
    medium_alerts,
    performance_alerts,
    failure_alerts,
    stale_source_alerts,
    checked_at
FROM DWS_EDWDEV.DWS_MONITORING.DWS_ALERT_SUMMARY;
```

**Visualization:**
- Scorecard showing `total_alerts`
- Subtitle: `critical_alerts` || ' critical, ' || `high_alerts` || ' high'
- Width: 4 columns

---

## Section 2: Execution Tracking (Row 2-3)

### Tile 4 -- Daily Execution Summary

**Domain:** Execution Tracking
**Source:** `DWS_MONITORING.DWS_DAILY_EXECUTION_SUMMARY`
**Chart Type:** Stacked Bar Chart
**X-Axis:** `execution_date`
**Y-Axis:** `successful_models` (green), `failed_models` (red)

```sql
SELECT
    execution_date,
    models_run,
    successful_models,
    failed_models,
    ROUND(total_minutes, 2) AS total_minutes,
    ROUND(avg_execution_seconds, 2) AS avg_seconds,
    total_rows_affected,
    success_rate_pct
FROM DWS_EDWDEV.DWS_MONITORING.DWS_DAILY_EXECUTION_SUMMARY
ORDER BY execution_date DESC;
```

**Visualization:**
- Stacked bar chart: green = successful, red = failed
- X-axis: `execution_date`
- Overlay line: `success_rate_pct` on secondary Y-axis
- Width: 6 columns

---

### Tile 5 -- Success Rate Trend

**Domain:** Execution Tracking
**Source:** `DWS_MONITORING.DWS_DAILY_EXECUTION_SUMMARY`
**Chart Type:** Line Chart
**X-Axis:** `execution_date`
**Y-Axis:** `success_rate_pct`

```sql
SELECT
    execution_date,
    success_rate_pct,
    models_run,
    ROUND(avg_execution_seconds, 2) AS avg_seconds
FROM DWS_EDWDEV.DWS_MONITORING.DWS_DAILY_EXECUTION_SUMMARY
ORDER BY execution_date ASC;
```

**Visualization:**
- Line chart with `success_rate_pct` on Y-axis (range: 0-100)
- Reference line at 95% (SLA threshold)
- Width: 6 columns

---

### Tile 6 -- Model Execution Log

**Domain:** Execution Tracking
**Source:** `DWS_MONITORING.DWS_MODEL_EXECUTIONS`
**Chart Type:** Table
**Sort:** `run_started_at` DESC

```sql
SELECT
    model_name,
    schema_name,
    status,
    ROUND(total_node_runtime, 2) AS runtime_seconds,
    rows_affected,
    warehouse_name,
    query_tag,
    run_started_at
FROM DWS_EDWDEV.DWS_MONITORING.DWS_MODEL_EXECUTIONS
ORDER BY run_started_at DESC
LIMIT 50;
```

**Visualization:**
- Table with conditional formatting: `status` = FAIL in red
- Width: 8 columns

---

### Tile 7 -- Slowest Models (Top 10)

**Domain:** Execution Tracking
**Source:** `DWS_MONITORING.DWS_SLOWEST_MODELS`
**Chart Type:** Horizontal Bar Chart
**X-Axis:** `avg_seconds`
**Y-Axis:** `model_name`

```sql
SELECT
    model_name,
    performance_tier,
    run_count,
    avg_seconds,
    max_seconds,
    min_seconds,
    total_seconds,
    estimated_cost_usd
FROM DWS_EDWDEV.DWS_MONITORING.DWS_SLOWEST_MODELS
LIMIT 10;
```

**Visualization:**
- Horizontal bar chart sorted by `avg_seconds` descending
- Color by `performance_tier`: CRITICAL=red, SLOW=orange, MODERATE=yellow, FAST=green
- Width: 4 columns

---

## Section 3: Data Freshness (Row 4)

### Tile 8 -- Source Table Freshness

**Domain:** Freshness
**Source:** `DWS_MONITORING.DWS_SOURCE_FRESHNESS`
**Chart Type:** Table (heatmap style)

```sql
SELECT
    source_table,
    source_type,
    schema_name,
    row_count,
    last_load_timestamp,
    hours_since_load,
    freshness_status
FROM DWS_EDWDEV.DWS_MONITORING.DWS_SOURCE_FRESHNESS
ORDER BY hours_since_load DESC;
```

**Visualization:**
- Table with conditional formatting on `freshness_status`:
  - Fresh = green
  - Warning = yellow
  - Stale = red
- Width: 6 columns

---

### Tile 9 -- dbt Model Freshness

**Domain:** Freshness
**Source:** `DWS_MONITORING.DWS_MODEL_FRESHNESS`
**Chart Type:** Table (heatmap style)

```sql
SELECT
    model_name,
    layer,
    schema_name,
    row_count,
    last_refresh,
    minutes_since_refresh,
    freshness_status
FROM DWS_EDWDEV.DWS_MONITORING.DWS_MODEL_FRESHNESS
ORDER BY
    CASE layer
        WHEN 'DIMENSION' THEN 1
        WHEN 'CORE' THEN 2
        WHEN 'EVENTS' THEN 3
        WHEN 'AGGREGATE' THEN 4
    END,
    model_name;
```

**Visualization:**
- Table with conditional formatting on `freshness_status`:
  - Fresh = green, Warning = yellow, Stale = red
- Group rows by `layer`
- Width: 6 columns

---

## Section 4: Test Quality (Row 5)

### Tile 10 -- Test Pass Rate Trend

**Domain:** Test Insights
**Source:** `DWS_MONITORING.DWS_TEST_PASS_RATE_TREND`
**Chart Type:** Line Chart with Area
**X-Axis:** `test_date`
**Y-Axis:** `pass_rate_pct`

```sql
SELECT
    test_date,
    total_tests,
    passed,
    failed,
    errors,
    pass_rate_pct,
    quality_tier
FROM DWS_EDWDEV.DWS_MONITORING.DWS_TEST_PASS_RATE_TREND
ORDER BY test_date ASC;
```

**Visualization:**
- Area chart: `pass_rate_pct` on Y-axis (0-100 range)
- Color bands: EXCELLENT (>=99%) green, GOOD (>=95%) blue, WARNING (>=90%) yellow, CRITICAL (<90%) red
- Width: 6 columns

---

### Tile 11 -- Test Summary by Type

**Domain:** Test Insights
**Source:** `DWS_MONITORING.DWS_TEST_SUMMARY_BY_TYPE`
**Chart Type:** Stacked Bar Chart
**X-Axis:** `test_type`
**Y-Axis:** `passed` (green), `failed` (red), `errors` (orange)

```sql
SELECT
    test_type,
    total_executions,
    passed,
    failed,
    errors,
    pass_rate_pct,
    ROUND(avg_test_seconds, 2) AS avg_seconds,
    last_run
FROM DWS_EDWDEV.DWS_MONITORING.DWS_TEST_SUMMARY_BY_TYPE
ORDER BY pass_rate_pct ASC;
```

**Visualization:**
- Stacked bar chart per test type
- Labels: `pass_rate_pct` on each bar
- Width: 6 columns

---

## Section 5: Data Quality & Reconciliation (Row 6)

### Tile 12 -- Row Count Tracking (All Layers)

**Domain:** Data Quality
**Source:** `DWS_MONITORING.DWS_ROW_COUNT_TRACKING`
**Chart Type:** Table

```sql
SELECT
    layer,
    table_name,
    row_count,
    checked_at
FROM DWS_EDWDEV.DWS_MONITORING.DWS_ROW_COUNT_TRACKING;
```

**Visualization:**
- Table grouped by `layer` (SOURCE > DIMENSION > CORE > EVENTS > AGGREGATE)
- Bar sparkline on `row_count` column
- Width: 6 columns

---

### Tile 13 -- Data Reconciliation (Source vs Mart)

**Domain:** Data Quality
**Source:** `DWS_MONITORING.DWS_DATA_RECONCILIATION`
**Chart Type:** Table

```sql
SELECT
    data_domain,
    source_distinct_keys,
    source_rows,
    mart_distinct_keys,
    mart_rows,
    key_difference,
    reconciliation_status,
    checked_at
FROM DWS_EDWDEV.DWS_MONITORING.DWS_DATA_RECONCILIATION;
```

**Visualization:**
- Table with conditional formatting on `reconciliation_status`:
  - RECONCILED = green
  - WITHIN TOLERANCE = yellow
  - MISMATCH = red
- Width: 6 columns

---

## Section 6: Audit Logging (Row 7)

### Tile 14 -- dbt Run History (Hook-based)

**Domain:** Audit / Observability
**Source:** `DWS_AUDIT.DBT_RUN_LOG`
**Chart Type:** Table

```sql
SELECT
    run_id,
    environment,
    run_status,
    run_started_at,
    run_ended_at,
    run_duration_seconds,
    models_run,
    models_success,
    models_failed,
    models_skipped,
    warehouse_name,
    user_name,
    run_command
FROM DWS_EDWDEV.DWS_AUDIT.DBT_RUN_LOG
ORDER BY run_started_at DESC
LIMIT 20;
```

**Visualization:**
- Table with conditional formatting on `run_status`: SUCCESS=green, FAILED/ERROR=red
- Width: 6 columns

---

### Tile 15 -- dbt Model Execution Log (Hook-based)

**Domain:** Audit / Observability
**Source:** `DWS_AUDIT.DBT_MODEL_LOG`
**Chart Type:** Table

```sql
SELECT
    model_name,
    schema_name,
    materialization,
    status,
    rows_affected,
    started_at,
    ended_at,
    DATEDIFF('second', started_at, ended_at) AS duration_seconds,
    database_name,
    is_incremental,
    incremental_strategy
FROM DWS_EDWDEV.DWS_AUDIT.DBT_MODEL_LOG
ORDER BY started_at DESC
LIMIT 50;
```

**Visualization:**
- Table with conditional formatting on `status`: SUCCESS=green, FAIL/ERROR=red
- Width: 6 columns

---

## Section 7: Cost Monitoring (Row 8)

### Tile 16 -- Daily Credit Consumption

**Domain:** Cost Monitoring
**Source:** `DWS_MONITORING.DWS_COST_DAILY`
**Chart Type:** Line Chart with Bar Overlay
**X-Axis:** `usage_date`
**Y-Axis (bar):** `estimated_cost_usd`
**Y-Axis (line):** `credits_7day_avg`

```sql
SELECT
    usage_date,
    warehouse_name,
    ROUND(compute_hours, 4) AS compute_hours,
    ROUND(estimated_credits, 4) AS credits,
    ROUND(estimated_cost_usd, 2) AS cost_usd,
    query_count,
    ROUND(gb_scanned, 2) AS gb_scanned,
    ROUND(credits_7day_avg, 4) AS credits_7day_avg,
    ROUND(variance_from_avg_pct, 1) AS variance_pct
FROM DWS_EDWDEV.DWS_MONITORING.DWS_COST_DAILY
ORDER BY usage_date DESC;
```

**Visualization:**
- Combined: bar = `cost_usd`, line = `credits_7day_avg`
- X-axis: `usage_date`
- Width: 6 columns

---

### Tile 17 -- Cost by Model (Top Consumers)

**Domain:** Cost Monitoring
**Source:** `DWS_MONITORING.DWS_COST_BY_MODEL`
**Chart Type:** Horizontal Bar Chart
**X-Axis:** `estimated_cost_usd`
**Y-Axis:** `model_name`

```sql
SELECT
    model_name,
    schema_name,
    executions,
    total_seconds,
    avg_seconds,
    total_rows,
    estimated_cost_usd,
    cost_per_execution,
    cost_per_1k_rows,
    cost_rank,
    cost_tier
FROM DWS_EDWDEV.DWS_MONITORING.DWS_COST_BY_MODEL
ORDER BY estimated_cost_usd DESC
LIMIT 15;
```

**Visualization:**
- Horizontal bar chart sorted by `estimated_cost_usd` descending
- Color by `cost_tier`: HIGH=red, MODERATE=orange, LOW=yellow, MINIMAL=green
- Width: 6 columns

---

## Section 8: Query Performance (Row 9)

### Tile 18 -- Model Performance Trend

**Domain:** Query Performance
**Source:** `DWS_MONITORING.DWS_MODEL_PERFORMANCE_TREND`
**Chart Type:** Line Chart (multi-series)
**X-Axis:** `run_date`
**Y-Axis:** `avg_seconds`
**Series:** `model_name`

```sql
SELECT
    run_date,
    model_name,
    avg_seconds,
    max_seconds,
    avg_7day_ma,
    performance_trend,
    run_count,
    total_rows
FROM DWS_EDWDEV.DWS_MONITORING.DWS_MODEL_PERFORMANCE_TREND
ORDER BY run_date DESC, model_name;
```

**Visualization:**
- Multi-line chart: one line per `model_name`
- Dashed overlay for `avg_7day_ma` (moving average)
- Width: 6 columns

---

### Tile 19 -- Long Running Queries (>1 min)

**Domain:** Query Performance
**Source:** `DWS_MONITORING.DWS_LONG_RUNNING_QUERIES`
**Chart Type:** Table

```sql
SELECT
    start_time,
    elapsed_seconds,
    execution_seconds,
    queue_seconds,
    compilation_seconds,
    warehouse_name,
    warehouse_size,
    severity,
    bottleneck_analysis,
    ROUND(mb_scanned, 1) AS mb_scanned,
    rows_produced,
    query_preview
FROM DWS_EDWDEV.DWS_MONITORING.DWS_LONG_RUNNING_QUERIES
ORDER BY elapsed_seconds DESC
LIMIT 20;
```

**Visualization:**
- Table with conditional formatting on `severity`: CRITICAL=red, HIGH=orange, MEDIUM=yellow, LOW=green
- Width: 6 columns

---

## Section 9: Deep Diagnostics (Row 10)

### Tile 20 -- Queue Time Analysis

**Domain:** Query Performance
**Source:** `DWS_MONITORING.DWS_QUEUE_TIME_ANALYSIS`
**Chart Type:** Heatmap or Table
**X-Axis:** `hour_bucket`

```sql
SELECT
    hour_bucket,
    warehouse_name,
    warehouse_size,
    query_count,
    avg_queue_seconds,
    max_queue_seconds,
    p95_queue_seconds,
    avg_total_seconds,
    ROUND(queue_impact_pct, 1) AS queue_impact_pct,
    queue_status
FROM DWS_EDWDEV.DWS_MONITORING.DWS_QUEUE_TIME_ANALYSIS
ORDER BY hour_bucket DESC;
```

**Visualization:**
- Table or heatmap with `queue_status` coloring
- CRITICAL=red, HIGH=orange, WARNING=yellow, MODERATE=blue, HEALTHY=green
- Width: 4 columns

---

### Tile 21 -- Recurring Test Failures

**Domain:** Test Insights
**Source:** `DWS_MONITORING.DWS_RECURRING_TEST_FAILURES`
**Chart Type:** Table

```sql
SELECT
    test_type,
    failure_count,
    first_failure,
    last_failure,
    failure_span_days,
    severity,
    LEFT(query_preview, 200) AS test_query_preview
FROM DWS_EDWDEV.DWS_MONITORING.DWS_RECURRING_TEST_FAILURES
ORDER BY failure_count DESC;
```

**Visualization:**
- Table with conditional formatting on `severity`
- Width: 4 columns

---

### Tile 22 -- Cost Anomaly Alerts

**Domain:** Cost Monitoring
**Source:** `DWS_MONITORING.DWS_ALERT_COST`
**Chart Type:** Table

```sql
SELECT
    usage_date,
    warehouse_name,
    ROUND(estimated_credits, 4) AS credits,
    ROUND(credits_7day_avg, 4) AS avg_credits,
    ROUND(variance_from_avg_pct, 1) AS variance_pct,
    ROUND(estimated_cost_usd, 2) AS cost_usd,
    severity,
    alert_description
FROM DWS_EDWDEV.DWS_MONITORING.DWS_ALERT_COST
ORDER BY variance_from_avg_pct DESC;
```

**Visualization:**
- Table with conditional formatting on `severity`: CRITICAL=red, HIGH=orange, MEDIUM=yellow
- Width: 4 columns

---

## Monitoring Coverage Matrix

| Domain | What It Monitors | Source | Tiles |
|--------|-----------------|--------|-------|
| **Execution Tracking** | Model runs, success/failure, duration | `QUERY_HISTORY` + `DBT_RUN_LOG` | 2, 4, 5, 6, 7 |
| **Source Freshness** | How current is the raw data | Source table timestamps | 8 |
| **Model Freshness** | When were dbt models last refreshed | `dbt_loaded_at` audit column | 9 |
| **Alerting** | Performance degradation, failures, stale data | Derived from all sources | 1, 3 |
| **Test Quality** | Test pass rates, failure patterns | `QUERY_HISTORY` test detection | 10, 11, 21 |
| **Data Quality** | Row counts, source-to-mart reconciliation | Direct table queries | 12, 13 |
| **Audit Logging** | dbt hook-based run and model logs | `DBT_RUN_LOG`, `DBT_MODEL_LOG` | 14, 15 |
| **Cost Monitoring** | Credit consumption, cost per model | `QUERY_HISTORY` compute time | 16, 17, 22 |
| **Query Performance** | Slow queries, queue times, trends | `QUERY_HISTORY` elapsed/queue time | 18, 19, 20 |

---

## Snowsight Dashboard Setup Steps

1. Navigate to **Dashboards** in Snowsight
2. Click **+ Dashboard** and name it `DWS Client Reporting - Observability`
3. For each tile:
   - Click **+** > **New Tile from SQL Worksheet**
   - Paste the query from this guide
   - Name the tile (use the tile name from this guide)
   - Select the chart type as specified
   - Configure axes, colors, and formatting as described
4. Arrange tiles following the **Dashboard Layout** table above
5. Set auto-refresh to **5 minutes** for near-real-time monitoring

---

## Quick Health Check Queries

Run these independently to verify the monitoring stack is working:

```sql
-- Overall health (single row)
SELECT * FROM DWS_EDWDEV.DWS_MONITORING.DWS_OPERATIONAL_SUMMARY;

-- Last 5 dbt runs
SELECT run_id, run_status, run_started_at, models_run, models_failed
FROM DWS_EDWDEV.DWS_AUDIT.DBT_RUN_LOG
ORDER BY run_started_at DESC LIMIT 5;

-- Any active alerts?
SELECT * FROM DWS_EDWDEV.DWS_MONITORING.DWS_ALERT_SUMMARY;

-- Source-to-mart reconciliation
SELECT * FROM DWS_EDWDEV.DWS_MONITORING.DWS_DATA_RECONCILIATION;

-- Test pass rate (today)
SELECT * FROM DWS_EDWDEV.DWS_MONITORING.DWS_TEST_PASS_RATE_TREND
WHERE test_date = CURRENT_DATE();
```
