# ⚡ Quick Start: DBT Comprehensive Monitoring

**5-Minute Setup Guide** | **Last Updated:** 2025-11-30

---

## 🎯 What You're Getting

- ✅ **8 Enhanced Packages** (codegen, audit_helper, project_evaluator, metrics, etc.)
- ✅ **20+ Monitoring Views** (performance, quality, costs, trends)
- ✅ **14 Alert Views** (test failures, performance, freshness, costs)
- ✅ **6 Automated Tasks** (hourly/daily notifications)
- ✅ **30+ Dashboard Tiles** (Snowsight ready)
- ✅ **100% Idempotent** (safe to run repeatedly)

---

## 🚀 3-Step Quick Setup

### **STEP 1: Install Packages (2 minutes)**

```bash
# Terminal - Run from project root
cd dbt_foundation
dbt deps  # Installs 8 packages

cd ../dbt_finance_core
dbt deps  # Installs same 8 packages
```

**What changed:**
- ✅ `packages.yml` in both projects now include:
  - `dbt_project_evaluator` - Code quality
  - `audit_helper` - Compare model versions
  - `codegen` - Auto-generate docs
  - `dbt_date` - Date utilities
  - `metrics` - Business KPIs
  - Plus existing: `dbt_utils`, `dbt_expectations`, `dbt_artifacts`

---

### **STEP 2: Run DBT to Create Artifact Tables (2 minutes)**

```bash
# Create monitoring data
cd dbt_foundation
dbt build  # Run models + tests

cd ../dbt_finance_core
dbt build  # Run models + tests
```

**What this does:**
- ✅ `dbt_artifacts` package creates tables:
  - `DBT_ARTIFACTS.MODEL_EXECUTIONS`
  - `DBT_ARTIFACTS.TEST_EXECUTIONS`
  - `DBT_ARTIFACTS.SOURCE_FRESHNESS_EXECUTIONS`

---

### **STEP 3: Run Master Setup Script in Snowflake (1 minute)**

**In Snowsight:**

```sql
-- Open and execute this file:
MASTER_SETUP_COMPLETE_OBSERVABILITY.sql
```

**What this creates:**
- ✅ `DBT_MONITORING` schema
- ✅ 5 base monitoring views
- ✅ 6 core alert views
- ✅ Notification procedures
- ✅ Alert audit table
- ✅ Permissions

**Verification:**
```sql
-- Check health status
SELECT * FROM DBT_MONITORING.ALERT_SUMMARY_DASHBOARD;

-- View any alerts
SELECT * FROM DBT_MONITORING.ALERT_ALL_CRITICAL;

-- See execution history
SELECT * FROM DBT_MONITORING.DAILY_EXECUTION_SUMMARY;
```

---

## ✅ You're Done! (Core Setup Complete)

**What you can do now:**

1. **View Metrics:**
   ```sql
   SELECT * FROM DBT_MONITORING.ALERT_SUMMARY_DASHBOARD;
   ```

2. **Check Alerts:**
   ```sql
   SELECT * FROM DBT_MONITORING.ALERT_ALL_CRITICAL;
   ```

3. **Monitor Performance:**
   ```sql
   SELECT * FROM DBT_MONITORING.SLOWEST_MODELS_CURRENT_WEEK;
   ```

4. **Track Tests:**
   ```sql
   SELECT * FROM DBT_MONITORING.TEST_RESULTS_HEALTH;
   ```

---

## 📊 Optional: Create Snowsight Dashboard (10 minutes)

**In Snowsight:**

1. **Dashboards** → **New Dashboard** → Name: "DBT Observability"

2. **Add tiles using queries from:** `SNOWSIGHT_DASHBOARD_QUERIES.md`

**Top 5 Must-Have Tiles:**

### **Tile 1: Health Scorecard**
```sql
SELECT * FROM DBT_MONITORING.ALERT_SUMMARY_DASHBOARD;
```
Display as: **Scorecard**

---

### **Tile 2: Critical Alerts**
```sql
SELECT * FROM DBT_MONITORING.ALERT_ALL_CRITICAL LIMIT 10;
```
Display as: **Table**  
Alert: Email when row count > 0

---

### **Tile 3: Daily Execution Trend**
```sql
SELECT * FROM DBT_MONITORING.DAILY_EXECUTION_SUMMARY
WHERE execution_date >= DATEADD(day, -30, CURRENT_DATE());
```
Display as: **Line Chart**  
X-axis: execution_date  
Y-axis: models_run, total_execution_seconds

---

### **Tile 4: Test Pass Rate**
```sql
SELECT * FROM DBT_MONITORING.TEST_RESULTS_HEALTH
WHERE test_date >= DATEADD(day, -30, CURRENT_DATE());
```
Display as: **Stacked Area Chart**  
X-axis: test_date  
Y-axis: test_count  
Group by: status

---

### **Tile 5: Slowest Models**
```sql
SELECT * FROM DBT_MONITORING.SLOWEST_MODELS_CURRENT_WEEK LIMIT 10;
```
Display as: **Bar Chart**  
X-axis: model_name  
Y-axis: avg_seconds

---

## 🔔 Optional: Enhanced Alerts & Notifications (5 minutes)

### **For Production - Add 14 Advanced Alerts:**

**In Snowsight, execute:**
```sql
-- Run this file:
setup_comprehensive_alerts.sql
```

**Adds alerts for:**
- ✅ Recurring test failures
- ✅ Test pass rate degradation
- ✅ Long-running queries
- ✅ Cost spikes
- ✅ Warehouse queuing
- ✅ SLA violations
- ✅ Missing data loads
- ✅ Plus 7 more...

---

### **For Email Notifications:**

**Step 1: Configure Email Integration (ACCOUNTADMIN only):**
```sql
CREATE OR REPLACE NOTIFICATION INTEGRATION dbt_email_integration
  TYPE=EMAIL
  ENABLED=TRUE
  ALLOWED_RECIPIENTS=('your-team@company.com');
```

**Step 2: Run Notifications Setup:**
```sql
-- Run this file:
setup_notifications.sql
```

**Step 3: Test Email:**
```sql
CALL DBT_MONITORING.SEND_TEST_EMAIL();
```

**Step 4: Enable Automated Tasks:**
```sql
CALL DBT_MONITORING.ENABLE_ALL_ALERT_TASKS();
```

**What you get:**
- ✅ Hourly critical alert emails
- ✅ Daily health report (8 AM)
- ✅ Test failure notifications (every 4 hours)
- ✅ Performance alerts (every 2 hours)
- ✅ Cost spike alerts (daily at noon)
- ✅ Data freshness alerts (every 6 hours)

---

## 🔁 Idempotency - Running Multiple Times

**All scripts are safe to run repeatedly:**

| Action | What Happens | Data Loss? |
|--------|--------------|------------|
| `dbt build` | ✅ Adds new execution records | ❌ No |
| `MASTER_SETUP_COMPLETE_OBSERVABILITY.sql` | ✅ Updates view definitions | ❌ No |
| `setup_comprehensive_alerts.sql` | ✅ Recreates alert views | ❌ No |
| `setup_notifications.sql` | ✅ Updates procedures/tasks | ❌ No |

**Why it's safe:**
- All objects use `CREATE OR REPLACE`
- Artifact tables accumulate data (no truncation)
- Views automatically reflect latest data
- No duplication

**Example:**
```bash
# Day 1
dbt build  # Creates execution records

# Day 2
dbt build  # Adds more records (doesn't duplicate)

# Anytime
# Re-run setup scripts to update definitions
# Data remains intact
```

---

## 📚 Key Files Reference

| File | Purpose | When to Use |
|------|---------|-------------|
| `COMPREHENSIVE_MONITORING_README.md` | **Complete documentation** | Detailed reference |
| `QUICK_START_MONITORING.md` | **This file - Quick start** | Fast setup |
| `MASTER_SETUP_COMPLETE_OBSERVABILITY.sql` | **Core monitoring setup** | First time + updates |
| `setup_comprehensive_alerts.sql` | **14 advanced alerts** | Production deployments |
| `setup_notifications.sql` | **Email/Slack automation** | Automated monitoring |
| `SNOWSIGHT_DASHBOARD_QUERIES.md` | **30+ dashboard queries** | Building dashboards |
| `packages.yml` (both projects) | **8 enhanced packages** | Already updated ✅ |

---

## 🎯 Monitoring Coverage

### **What's Automatically Tracked:**

✅ **Model Executions**
- Run count, duration, status
- Success/failure rates
- Performance trends
- 7-day moving averages

✅ **Test Results**
- Pass/fail counts
- Pass rate trends
- Recurring failures
- Test coverage per model

✅ **Data Freshness**
- Source staleness
- Missing data loads
- Freshness SLA compliance

✅ **Performance**
- Slowest models
- Performance degradation (statistical)
- Long-running queries
- Warehouse queuing

✅ **Costs**
- Daily cost trends
- Cost spikes (anomaly detection)
- Expensive queries
- Cost by model (estimated)

✅ **Overall Health**
- Health score (0-100)
- Critical alert counts
- System status

---

## 🔧 Common Commands

### **Check Current Status:**
```sql
-- Overall health
SELECT * FROM DBT_MONITORING.ALERT_SUMMARY_DASHBOARD;

-- Any issues?
SELECT * FROM DBT_MONITORING.ALERT_ALL_CRITICAL;

-- Recent runs
SELECT * FROM DBT_MONITORING.DAILY_EXECUTION_SUMMARY LIMIT 7;
```

### **Manual Notifications:**
```sql
-- Test email system
CALL DBT_MONITORING.SEND_TEST_EMAIL();

-- Send critical alerts now
CALL DBT_MONITORING.SEND_CRITICAL_ALERTS_EMAIL();

-- Get alert summary
SELECT * FROM TABLE(DBT_MONITORING.GET_ALERT_SUMMARY());
```

### **Task Management:**
```sql
-- Check task status
SELECT * FROM DBT_MONITORING.TASK_STATUS_MONITORING;

-- Enable all automated tasks
CALL DBT_MONITORING.ENABLE_ALL_ALERT_TASKS();

-- Disable all tasks (emergency)
CALL DBT_MONITORING.DISABLE_ALL_ALERT_TASKS();
```

---

## 📞 Need Help?

**Troubleshooting:**
- See section in `COMPREHENSIVE_MONITORING_README.md`
- Common issues and solutions provided

**Documentation:**
- `COMPREHENSIVE_MONITORING_README.md` - Full details
- `OBSERVABILITY_GUIDE.md` - Original setup
- `IMPLEMENTATION_SUMMARY.md` - Project overview

---

## ✅ Quick Verification

**After setup, verify everything works:**

```sql
-- 1. Check objects created
SELECT 
    'Views' as type, COUNT(*) as count
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'DBT_MONITORING'

UNION ALL

SELECT 'Procedures', COUNT(*)
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'DBT_MONITORING';

-- Expected: 
-- Views: 11+ (5 base + 6 alerts minimum)
-- Procedures: 2+ (notification procedures)

-- 2. Check health status
SELECT * FROM DBT_MONITORING.ALERT_SUMMARY_DASHBOARD;
-- Should return 1 row with health score and metrics

-- 3. Check for alerts
SELECT COUNT(*) as critical_alerts 
FROM DBT_MONITORING.ALERT_ALL_CRITICAL;
-- Should return count (may be 0 if all is healthy)

-- 4. Check artifact data
SELECT COUNT(*) as execution_records 
FROM DBT_ARTIFACTS.MODEL_EXECUTIONS;
-- Should be > 0 if you ran dbt build
```

---

## 🎉 You're All Set!

**You now have production-grade dbt observability!**

- ✅ Enhanced packages installed
- ✅ Comprehensive monitoring active
- ✅ Alerts configured
- ✅ Ready for Snowsight dashboards
- ✅ Optional: Automated notifications

**Every time you run `dbt build`:**
- Execution data automatically captured
- Metrics updated in real-time
- Alerts evaluated
- Trends calculated
- Health score computed

**No manual work required - fully automated! 🚀**

