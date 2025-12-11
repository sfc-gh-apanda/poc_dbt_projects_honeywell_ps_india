# O2C Enhanced - Complete Observability Implementation Guide

## 📋 Overview

This guide covers the implementation of the complete observability stack for O2C Enhanced:

| Component | Views | Alerts | Purpose |
|-----------|-------|--------|---------|
| **Cost Monitoring** | 4 | 3 | Credit/cost tracking, budget alerts |
| **Query Performance** | 5 | 2 | Queue time, long queries, compilation |
| **Model Performance** | 2 | 3 | Execution trends, efficiency analysis |
| **Schema Drift** | 4 | 1 | DDL changes, column tracking |
| **dbt Observability** | 5 | 1 | Test coverage, dependencies, orphans |
| **Data Integrity** | 6 | 1 | PK/FK validation, duplicates, nulls |
| **TOTAL** | **26** | **11** | |

---

## 🏗️ Implementation Steps

### Phase 1: Prerequisites Check

Before starting, ensure the following are in place:

```sql
-- Step 1.1: Verify base setup exists
USE DATABASE EDW;

-- Check audit schema exists
SHOW SCHEMAS LIKE 'O2C_AUDIT';

-- Check monitoring schema exists
SHOW SCHEMAS LIKE 'O2C_ENHANCED_MONITORING';

-- Check base tables exist
SELECT COUNT(*) FROM EDW.O2C_AUDIT.DBT_RUN_LOG;
SELECT COUNT(*) FROM EDW.O2C_AUDIT.DBT_MODEL_LOG;

-- Check model executions view exists
SELECT COUNT(*) FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_MODEL_EXECUTIONS;
```

**If any are missing, run in order:**
1. `O2C_ENHANCED_AUDIT_SETUP.sql`
2. `O2C_ENHANCED_MONITORING_SETUP.sql`

---

### Phase 2: Deploy Monitoring Views

Execute the SQL files in order:

#### Step 2.1: Cost & Performance Monitoring

```bash
# File: O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql
# Creates: 11 views for cost and performance analysis
```

```sql
-- Run in Snowflake:
USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- Execute the script
@O2C/docs_o2c_enhanced/O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql

-- Verify views created
SELECT TABLE_NAME, COMMENT 
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
  AND TABLE_NAME LIKE '%COST%' OR TABLE_NAME LIKE '%QUEUE%';
```

**Views Created:**
| View | Description |
|------|-------------|
| `O2C_ENH_COST_DAILY` | Daily credit consumption with 7-day MA |
| `O2C_ENH_COST_BY_MODEL` | Cost attribution by model |
| `O2C_ENH_COST_MONTHLY` | Monthly cost with MoM comparison |
| `O2C_ENH_ALERT_COST` | Cost anomaly detection |
| `O2C_ENH_LONG_RUNNING_QUERIES` | Queries >1 minute |
| `O2C_ENH_QUEUE_TIME_ANALYSIS` | Queue time by hour/warehouse |
| `O2C_ENH_COMPILATION_ANALYSIS` | Compilation time trends |
| `O2C_ENH_ALERT_QUEUE` | Queue time alerts |
| `O2C_ENH_ALERT_LONG_QUERY` | Long query alerts |
| `O2C_ENH_MODEL_PERFORMANCE_TREND` | Model execution trends |
| `O2C_ENH_INCREMENTAL_EFFICIENCY` | Rows/second efficiency |

---

#### Step 2.2: Schema Drift, dbt & Data Integrity Monitoring

```bash
# File: O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql
# Creates: 15 views for schema, dbt, and integrity monitoring
```

```sql
-- Run in Snowflake:
USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- Execute the script
@O2C/docs_o2c_enhanced/O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql

-- Verify views created
SELECT TABLE_NAME, COMMENT 
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
  AND (TABLE_NAME LIKE '%SCHEMA%' 
       OR TABLE_NAME LIKE '%DBT%'
       OR TABLE_NAME LIKE '%PK%'
       OR TABLE_NAME LIKE '%FK%');
```

**Views Created:**

| Category | View | Description |
|----------|------|-------------|
| **Schema Drift** | `O2C_ENH_SCHEMA_CURRENT_STATE` | Current schema snapshot |
| | `O2C_ENH_DDL_CHANGES` | DDL change history |
| | `O2C_ENH_COLUMN_CHANGES` | Column-level changes |
| | `O2C_ENH_ALERT_SCHEMA_DRIFT` | Schema drift alerts |
| **dbt Observability** | `O2C_ENH_DBT_TEST_COVERAGE` | Test coverage by model |
| | `O2C_ENH_DBT_MODEL_DEPENDENCIES` | Model dependency analysis |
| | `O2C_ENH_DBT_RUN_HISTORY` | dbt run history |
| | `O2C_ENH_DBT_ORPHAN_MODELS` | Inactive/orphan models |
| | `O2C_ENH_ALERT_DBT_COVERAGE` | Coverage gap alerts |
| **Data Integrity** | `O2C_ENH_PK_VALIDATION` | Primary key validation |
| | `O2C_ENH_FK_VALIDATION` | Foreign key validation |
| | `O2C_ENH_DUPLICATE_DETECTION` | Duplicate detection |
| | `O2C_ENH_NULL_TREND_ANALYSIS` | Null rate analysis |
| | `O2C_ENH_DATA_CONSISTENCY` | Cross-table consistency |
| | `O2C_ENH_ALERT_DATA_INTEGRITY` | Integrity issue alerts |

---

### Phase 3: Configure Notification Integrations

Before deploying alerts, configure your notification channels:

#### Step 3.1: Email Integration

```sql
-- Update with actual email addresses
CREATE OR REPLACE NOTIFICATION INTEGRATION O2C_EMAIL_NOTIFICATION
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = (
        'your-data-ops-email@company.com',
        'your-dbt-alerts-email@company.com'
    )
    COMMENT = 'O2C Enhanced email notifications';
```

#### Step 3.2: Slack Integration (Optional)

```sql
-- Get webhook URL from Slack App settings
CREATE OR REPLACE NOTIFICATION INTEGRATION O2C_SLACK_NOTIFICATION
    TYPE = WEBHOOK
    ENABLED = TRUE
    WEBHOOK_URL = 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
    WEBHOOK_BODY_TEMPLATE = '{
        "channel": "#data-alerts",
        "username": "Snowflake O2C Bot",
        "text": "🚨 *{{ ALERT_NAME }}*\nSeverity: {{ ALERT_SEVERITY }}\nMessage: {{ ALERT_MESSAGE }}"
    }'
    COMMENT = 'O2C Enhanced Slack notifications';
```

#### Step 3.3: Microsoft Teams Integration (Optional)

```sql
CREATE OR REPLACE NOTIFICATION INTEGRATION O2C_TEAMS_NOTIFICATION
    TYPE = WEBHOOK
    ENABLED = TRUE
    WEBHOOK_URL = 'https://outlook.office.com/webhook/YOUR/TEAMS/WEBHOOK'
    WEBHOOK_BODY_TEMPLATE = '{
        "@type": "MessageCard",
        "summary": "{{ ALERT_NAME }}",
        "sections": [{
            "activityTitle": "🚨 {{ ALERT_NAME }}",
            "facts": [
                {"name": "Severity", "value": "{{ ALERT_SEVERITY }}"},
                {"name": "Message", "value": "{{ ALERT_MESSAGE }}"}
            ]
        }]
    }'
    COMMENT = 'O2C Enhanced Teams notifications';
```

---

### Phase 4: Deploy Native Alerts

```bash
# File: O2C_ENHANCED_NATIVE_ALERTS.sql
# Creates: 11 native Snowflake alerts
```

```sql
-- Run in Snowflake:
USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- Execute the script
@O2C/docs_o2c_enhanced/O2C_ENHANCED_NATIVE_ALERTS.sql

-- Verify alerts created
SHOW ALERTS IN SCHEMA O2C_AUDIT;
```

**Alerts Created:**

| Alert | Category | Trigger | Schedule |
|-------|----------|---------|----------|
| `ALERT_COST_SPIKE` | Cost | >50% above 7-day avg | 8am & 6pm UTC |
| `ALERT_HIGH_COST_MODEL` | Cost | Model >$5/week | Daily 9am UTC |
| `ALERT_MONTHLY_BUDGET` | Cost | Monthly >$100 | Daily 9am UTC |
| `ALERT_QUEUE_TIME` | Query | Avg queue >10s | Every 30 min |
| `ALERT_LONG_RUNNING_QUERY` | Query | Query >5 min | Every 15 min |
| `ALERT_MODEL_PERFORMANCE` | Model | >20% slower | Every 4 hours |
| `ALERT_SLOW_MODEL` | Model | Avg >5 min | Daily 10am UTC |
| `ALERT_INCREMENTAL_INEFFICIENCY` | Model | <1000 rows/sec | Weekly Monday |
| `ALERT_SCHEMA_DRIFT` | Schema | DDL changes | Every 30 min |
| `ALERT_DBT_COVERAGE` | dbt | No tests | Daily 11am UTC |
| `ALERT_DATA_INTEGRITY` | Integrity | PK/FK issues | Daily 8am UTC |

---

### Phase 5: Verify Deployment

Run these verification queries:

```sql
-- ═══════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES
-- ═══════════════════════════════════════════════════════════════

-- 1. Count all monitoring views
SELECT 
    'Total Monitoring Views' AS metric,
    COUNT(*) AS count
FROM EDW.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING';

-- 2. Count all alerts
SELECT 
    'Total Alerts' AS metric,
    COUNT(*) AS count
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
UNION ALL
SELECT 
    'Alerts in O2C_AUDIT',
    COUNT(*)
FROM TABLE(RESULT_SCAN((SELECT LAST_QUERY_ID() FROM TABLE(SHOW ALERTS IN SCHEMA O2C_AUDIT))));

-- 3. Test a few key views
SELECT 'O2C_ENH_COST_DAILY' AS view_name, COUNT(*) AS rows FROM O2C_ENHANCED_MONITORING.O2C_ENH_COST_DAILY
UNION ALL
SELECT 'O2C_ENH_PK_VALIDATION', COUNT(*) FROM O2C_ENHANCED_MONITORING.O2C_ENH_PK_VALIDATION
UNION ALL
SELECT 'O2C_ENH_DBT_TEST_COVERAGE', COUNT(*) FROM O2C_ENHANCED_MONITORING.O2C_ENH_DBT_TEST_COVERAGE;

-- 4. Check alert history table
SELECT COUNT(*) AS alert_history_rows FROM O2C_AUDIT.O2C_ALERT_HISTORY;

-- 5. View active alerts
SELECT * FROM O2C_AUDIT.V_ACTIVE_ALERTS;
```

---

### Phase 6: Configure Monte Carlo (Passive Consumption)

Monte Carlo will automatically consume from these views when deployed as Snowflake Native App:

**Views for MC Consumption:**
- `O2C_AUDIT.V_ACTIVE_ALERTS` - Current open alerts
- `O2C_AUDIT.O2C_ALERT_HISTORY` - Alert audit trail
- All `O2C_ENHANCED_MONITORING.*` views - Monitoring data

**MC Auto-Discovery:**
- Lineage: From `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`
- Schema Changes: From `INFORMATION_SCHEMA`
- Freshness: From monitoring views
- Volume: From monitoring views

---

## 📊 Dashboard Setup

### Snowsight Dashboard Tiles

Add these tiles to your Snowsight dashboard:

#### Tile 1: Platform Health Score
```sql
SELECT 
    health_score,
    health_status,
    total_critical_alerts,
    snapshot_time
FROM O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_SUMMARY;
```

#### Tile 2: Cost Trend (Last 14 Days)
```sql
SELECT 
    usage_date,
    SUM(estimated_cost_usd) AS daily_cost,
    SUM(estimated_credits) AS daily_credits
FROM O2C_ENHANCED_MONITORING.O2C_ENH_COST_DAILY
WHERE usage_date >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY usage_date
ORDER BY usage_date;
```

#### Tile 3: Model Performance
```sql
SELECT 
    model_name,
    avg_seconds,
    estimated_cost_usd,
    cost_tier
FROM O2C_ENHANCED_MONITORING.O2C_ENH_COST_BY_MODEL
ORDER BY estimated_cost_usd DESC
LIMIT 10;
```

#### Tile 4: Data Integrity Status
```sql
SELECT 
    table_name,
    pk_status,
    duplicate_count,
    null_pk_count
FROM O2C_ENHANCED_MONITORING.O2C_ENH_PK_VALIDATION;
```

#### Tile 5: Active Alerts
```sql
SELECT 
    alert_name,
    alert_type,
    severity,
    alert_message,
    triggered_at
FROM O2C_AUDIT.V_ACTIVE_ALERTS
ORDER BY triggered_at DESC
LIMIT 20;
```

---

## 🔧 Maintenance

### Weekly Tasks
1. Review `O2C_AUDIT.V_ACTIVE_ALERTS` and acknowledge/resolve alerts
2. Check `O2C_ENH_COST_MONTHLY` for budget tracking
3. Review `O2C_ENH_DBT_ORPHAN_MODELS` for cleanup opportunities

### Monthly Tasks
1. Review alert thresholds and adjust as needed
2. Archive old alert history (>90 days)
3. Review cost trends and optimize expensive models

### Threshold Adjustment
```sql
-- Example: Update monthly budget threshold in alert
-- (Requires recreating the alert with new threshold)
ALTER ALERT O2C_AUDIT.ALERT_MONTHLY_BUDGET SUSPEND;
-- Modify CREATE ALERT statement with new threshold
-- ALTER ALERT O2C_AUDIT.ALERT_MONTHLY_BUDGET RESUME;
```

---

## 📁 File Reference

| File | Purpose | Views | Alerts |
|------|---------|-------|--------|
| `O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql` | Cost & performance | 11 | 0 |
| `O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql` | Schema, dbt, integrity | 15 | 0 |
| `O2C_ENHANCED_NATIVE_ALERTS.sql` | All native alerts | 1 | 11 |
| `O2C_ENHANCED_MONITORING_SETUP.sql` | Base monitoring | 25 | 0 |
| `O2C_ENHANCED_AUDIT_SETUP.sql` | Audit tables | 3 | 0 |

---

## ✅ Implementation Checklist

- [ ] **Phase 1**: Verify prerequisites (schemas, base tables)
- [ ] **Phase 2.1**: Deploy cost & performance monitoring views
- [ ] **Phase 2.2**: Deploy schema, dbt & integrity monitoring views
- [ ] **Phase 3**: Configure notification integrations (email, Slack, Teams)
- [ ] **Phase 4**: Deploy native alerts
- [ ] **Phase 5**: Run verification queries
- [ ] **Phase 6**: Configure Monte Carlo SNA (if applicable)
- [ ] **Dashboard**: Create Snowsight dashboard tiles
- [ ] **Documentation**: Update runbooks with alert response procedures

---

## 🚀 Quick Start Commands

```sql
-- Run all setup in order:
USE ROLE ACCOUNTADMIN;
USE DATABASE EDW;

-- 1. Base setup (if not already done)
-- @O2C/docs_o2c_enhanced/O2C_ENHANCED_AUDIT_SETUP.sql
-- @O2C/docs_o2c_enhanced/O2C_ENHANCED_MONITORING_SETUP.sql

-- 2. Deploy new monitoring views
@O2C/docs_o2c_enhanced/O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql
@O2C/docs_o2c_enhanced/O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql

-- 3. Deploy alerts
@O2C/docs_o2c_enhanced/O2C_ENHANCED_NATIVE_ALERTS.sql

-- 4. Verify
SHOW ALERTS IN SCHEMA O2C_AUDIT;
SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING';
```

---

**Implementation Complete! 🎉**

Your O2C Enhanced observability stack now includes:
- ✅ 26 monitoring views
- ✅ 11 native Snowflake alerts
- ✅ Cost, query, model performance tracking
- ✅ Schema drift detection
- ✅ dbt observability
- ✅ Data integrity monitoring
- ✅ Monte Carlo-ready (passive consumption)

