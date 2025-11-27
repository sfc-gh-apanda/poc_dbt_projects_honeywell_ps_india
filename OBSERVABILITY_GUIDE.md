# DBT Observability Guide for Snowflake Native

## Quick Answer: Best Solution

**For Snowflake Native DBT, use:**

```yaml
packages:
  - brooklyn-data/dbt_artifacts        # Tracks dbt execution metadata
  - get-select/dbt-snowflake-monitoring # Tracks Snowflake costs & performance
```

**Then visualize with Snowsight dashboards** (no CLI needed!)

---

## Why Not Elementary?

| Feature | Elementary | dbt_artifacts + Snowsight |
|---------|-----------|---------------------------|
| **Dashboard** | ❌ Needs CLI (`edr monitor`) | ✅ Native Snowsight |
| **Alerts** | ❌ Needs CLI (`edr send-report`) | ✅ Snowflake alerts |
| **Tests** | ✅ Works | ✅ Works |
| **Snowflake Native** | ⚠️ Partial support | ✅ Full support |
| **Setup complexity** | Medium | Low |

**Verdict:** Elementary needs CLI access which Snowflake Native DBT doesn't have. You lose 60% of Elementary's value (the dashboard).

---

## Recommended Stack

### 1. Install Packages

```yaml
# packages.yml
packages:
  # Core testing
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  
  - package: calogica/dbt_expectations
    version: 0.10.1
  
  # Observability (THE IMPORTANT ONES)
  - package: brooklyn-data/dbt_artifacts
    version: 2.6.1
  
  - package: get-select/dbt-snowflake-monitoring
    version: 3.0.0
```

### 2. Run Setup

```bash
# Install packages
dbt deps

# Run dbt (creates artifact tables)
dbt run

# Run setup script (creates monitoring views)
# Execute: setup_observability_dashboard.sql
```

### 3. Build Snowsight Dashboard

```
Snowsight → Dashboards → New Dashboard → "DBT Observability"

Add tiles:
├─ Daily Execution Summary (line chart)
├─ Test Pass Rate (stacked area)
├─ Top 10 Slowest Models (bar chart)
├─ Daily Costs (line chart)
└─ Failed Tests Alert (table)
```

---

## What You Get

### From dbt_artifacts:

✅ Model execution history  
✅ Test results over time  
✅ Performance trends  
✅ Failed test details  
✅ Source freshness status  

**Stored in tables:**
- `DBT_ARTIFACTS.MODEL_EXECUTIONS`
- `DBT_ARTIFACTS.TEST_EXECUTIONS`
- `DBT_ARTIFACTS.SOURCE_FRESHNESS_EXECUTIONS`

### From dbt_snowflake_monitoring:

✅ Query costs  
✅ Warehouse utilization  
✅ Credit consumption  
✅ Query performance  
✅ Cost by model (estimated)  

**Stored in tables:**
- `SNOWFLAKE_MONITORING.QUERY_HISTORY`
- `SNOWFLAKE_MONITORING.WAREHOUSE_METERING_HISTORY`

---

## Key Monitoring Queries

### 1. Daily Performance Summary

```sql
SELECT * FROM DBT_MONITORING.DAILY_EXECUTION_SUMMARY
ORDER BY execution_date DESC;
```

**Shows:**
- Models run per day
- Success/failure counts
- Total execution time

---

### 2. Slowest Models

```sql
SELECT * FROM DBT_MONITORING.SLOWEST_MODELS_CURRENT_WEEK
LIMIT 10;
```

**Shows:**
- Top 10 slowest models
- Average execution time
- Performance tier (CRITICAL/SLOW/MODERATE/FAST)

---

### 3. Test Health

```sql
SELECT * FROM DBT_MONITORING.TEST_RESULTS_HEALTH
WHERE test_date >= DATEADD(day, -7, CURRENT_DATE());
```

**Shows:**
- Daily pass/fail rates
- Test count trends
- Quality score

---

### 4. Cost Analysis

```sql
SELECT * FROM DBT_MONITORING.DBT_QUERY_COSTS
ORDER BY total_credits DESC;
```

**Shows:**
- Daily DBT query costs
- Credit consumption
- Most expensive queries

---

### 5. Failed Tests (Last 24 Hours)

```sql
SELECT * FROM DBT_MONITORING.ALERT_RECENT_TEST_FAILURES;
```

**Shows:**
- Recent test failures
- Error messages
- Which models/tests failed

---

### 6. Performance Anomalies

```sql
SELECT * FROM DBT_MONITORING.ALERT_SLOW_MODELS;
```

**Shows:**
- Models running slower than usual
- Percent slower than baseline
- Statistical anomalies (2-sigma threshold)

---

## Snowsight Dashboard Template

### Tile 1: Execution Summary (Scorecard)

```sql
SELECT 
    COUNT(DISTINCT node_id) as models_run_today,
    SUM(execution_time)/60 as total_minutes,
    AVG(execution_time) as avg_seconds
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE DATE(generated_at) = CURRENT_DATE();
```

---

### Tile 2: Test Pass Rate (Pie Chart)

```sql
SELECT 
    status,
    COUNT(*) as test_count
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE DATE(generated_at) = CURRENT_DATE()
GROUP BY status;
```

---

### Tile 3: Execution Trend (Line Chart)

```sql
SELECT * FROM DBT_MONITORING.DAILY_EXECUTION_SUMMARY
WHERE execution_date >= DATEADD(day, -30, CURRENT_DATE());
```

---

### Tile 4: Top Slow Models (Bar Chart)

```sql
SELECT 
    model_name,
    avg_seconds
FROM DBT_MONITORING.SLOWEST_MODELS_CURRENT_WEEK
ORDER BY avg_seconds DESC
LIMIT 10;
```

---

### Tile 5: Cost Trend (Line Chart)

```sql
SELECT 
    query_date,
    total_credits
FROM DBT_MONITORING.DBT_QUERY_COSTS
ORDER BY query_date DESC;
```

---

### Tile 6: Alert - Failed Tests (Table)

```sql
SELECT * FROM DBT_MONITORING.ALERT_RECENT_TEST_FAILURES
ORDER BY generated_at DESC;
```

**Add alert:** Email when row count > 0

---

## Setting Up Alerts

### Option 1: Snowflake Tasks (Automated)

```sql
CREATE OR REPLACE TASK DBT_MONITORING.DAILY_HEALTH_CHECK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 8 * * * America/New_York'
AS
DECLARE
    failure_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO :failure_count 
    FROM DBT_MONITORING.ALERT_RECENT_TEST_FAILURES;
    
    IF (:failure_count > 0) THEN
        CALL SYSTEM$SEND_EMAIL(
            'dbt_alerts',
            'data-team@company.com',
            'DBT Test Failures',
            'Check dashboard for details'
        );
    END IF;
END;

ALTER TASK DBT_MONITORING.DAILY_HEALTH_CHECK RESUME;
```

---

### Option 2: Snowsight Dashboard Alerts

1. Create dashboard tile
2. Click "..." → "Set Alert"
3. Configure:
   - Condition: Row count > 0
   - Recipients: team@company.com
   - Frequency: Every hour

---

## Comparison: Observability Tools

| Tool | Dashboard | Snowflake Native | Cost | Setup Time |
|------|-----------|------------------|------|------------|
| **dbt_artifacts + Snowsight** | ✅ Native | ✅ Full | Free | 30 min |
| **dbt_snowflake_monitoring** | ✅ Native | ✅ Full | Free | 15 min |
| **Elementary** | ❌ Needs CLI | ⚠️ Partial | Free | 1 hour |
| **Monte Carlo** | ✅ External | ✅ Full | $$$$ | Days |
| **Datafold** | ✅ External | ✅ Full | $$$ | Days |

**Winner:** dbt_artifacts + dbt_snowflake_monitoring + Snowsight ✅

---

## What About Other Tools?

### re_data
❌ Same issue as Elementary - needs CLI for UI

### Great Expectations
❌ Python-based, needs external orchestration

### Soda Core
❌ CLI-based, not native to Snowflake DBT

### Monte Carlo / Datafold / Lightup
✅ Work with Snowflake Native BUT expensive ($10K-$100K+/year)

**For 90% of teams:** Free packages + Snowsight is enough!

---

## Quick Start (5 Minutes)

### Step 1: Add Packages

```bash
cd dbt_foundation

cat >> packages.yml << 'EOF'
  - package: brooklyn-data/dbt_artifacts
    version: 2.6.1
EOF

dbt deps
```

### Step 2: Run DBT

```bash
dbt run
# Creates: DBT_ARTIFACTS.MODEL_EXECUTIONS table
```

### Step 3: Query in Snowsight

```sql
-- See your slowest models
SELECT 
    SPLIT_PART(node_id, '.', -1) as model_name,
    AVG(execution_time) as avg_seconds,
    COUNT(*) as run_count
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS
WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```

### Step 4: Save as Dashboard

Snowsight → Save Query → Add to Dashboard → Done! ✅

---

## Advanced: Full Setup

### 1. Install Both Packages

```yaml
packages:
  - brooklyn-data/dbt_artifacts
  - get-select/dbt-snowflake-monitoring
```

### 2. Run Setup Script

Execute: `setup_observability_dashboard.sql`

This creates:
- 11 monitoring views
- 2 alert queries
- Cost analysis views

### 3. Build Snowsight Dashboard

Use the monitoring views to create:
- Execution summary
- Test health trends
- Cost analysis
- Performance alerts
- Failed test tracking

### 4. Set Up Alerts

Configure email/Slack notifications for:
- Test failures
- Performance degradation
- Cost thresholds exceeded

---

## Monitoring Best Practices

### Daily Checks

```sql
-- 1. Check test health
SELECT COUNT(*) as failures
FROM DBT_MONITORING.ALERT_RECENT_TEST_FAILURES;

-- 2. Check performance
SELECT COUNT(*) as slow_models
FROM DBT_MONITORING.ALERT_SLOW_MODELS;

-- 3. Check costs
SELECT SUM(total_credits) as today_cost
FROM DBT_MONITORING.DBT_QUERY_COSTS
WHERE query_date = CURRENT_DATE();
```

### Weekly Reviews

```sql
-- 1. Top 10 slowest models
SELECT * FROM DBT_MONITORING.SLOWEST_MODELS_CURRENT_WEEK;

-- 2. Cost trends
SELECT * FROM DBT_MONITORING.DBT_QUERY_COSTS
WHERE query_date >= DATEADD(day, -7, CURRENT_DATE());

-- 3. Test pass rate
SELECT 
    AVG(CASE WHEN status = 'pass' THEN 1.0 ELSE 0.0 END) * 100 as pass_rate
FROM DBT_ARTIFACTS.TEST_EXECUTIONS
WHERE generated_at >= DATEADD(day, -7, CURRENT_DATE());
```

### Monthly Optimization

```sql
-- 1. Models to optimize (biggest cost impact)
SELECT * FROM DBT_MONITORING.COST_BY_MODEL_ESTIMATED
ORDER BY estimated_cost_usd DESC
LIMIT 20;

-- 2. Performance regression
SELECT * FROM DBT_MONITORING.MODEL_EXECUTION_TRENDS
WHERE moving_avg_7day > recent_avg * 1.5; -- 50% slower

-- 3. Unused models (ran but not queried downstream)
-- (Requires additional lineage tracking)
```

---

## Summary

### For Snowflake Native DBT:

**✅ DO Use:**
- dbt_artifacts (execution tracking)
- dbt_snowflake_monitoring (cost tracking)
- Snowsight dashboards (visualization)
- Snowflake alerts (notifications)

**❌ DON'T Use:**
- Elementary (needs CLI)
- re_data (needs CLI)
- Great Expectations (Python-based)
- Expensive commercial tools (overkill)

**Result:**
- Complete observability
- Zero additional cost
- Native Snowflake experience
- 30-minute setup
- Industry-proven

---

## Next Steps

1. ✅ Install packages (if not done): `packages.yml` updated
2. ⏭️ Run `dbt deps` to install packages
3. ⏭️ Run `dbt run` to create artifact tables
4. ⏭️ Execute `setup_observability_dashboard.sql` to create monitoring views
5. ⏭️ Build Snowsight dashboard using monitoring views
6. ⏭️ Configure alerts for critical metrics
7. ✅ Done! You have world-class observability!

---

## Resources

- **dbt_artifacts docs:** https://github.com/brooklyn-data/dbt_artifacts
- **dbt_snowflake_monitoring:** https://github.com/get-select/dbt-snowflake-monitoring
- **Snowsight dashboards:** https://docs.snowflake.com/en/user-guide/ui-snowsight-dashboards
- **Snowflake alerting:** https://docs.snowflake.com/en/user-guide/alerts

**You're all set for production-grade DBT observability on Snowflake! 🚀**

