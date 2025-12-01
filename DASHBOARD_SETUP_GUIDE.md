# Dashboard Setup Guide - Quick Start

**Time Required:** 10-15 minutes  
**Prerequisites:** Both dbt projects built successfully ✅

---

## 🚀 Step-by-Step Setup

### **Step 1: Run Monitoring Setup Script (5 minutes)**

In Snowsight, execute:

```sql
@MASTER_SETUP_QUERY_HISTORY.sql
```

**What this creates:**
- ✅ `EDW.DBT_MONITORING` schema
- ✅ 12+ monitoring views (MODEL_EXECUTIONS, TEST_EXECUTIONS, etc.)
- ✅ Alert views (performance, failures, health scores)
- ✅ Execution summary views

**Verify it worked:**
```sql
-- Should show data from your dbt runs
SELECT COUNT(*) FROM EDW.DBT_MONITORING.MODEL_EXECUTIONS;
SELECT COUNT(*) FROM EDW.DBT_MONITORING.TEST_EXECUTIONS;

-- Check health status
SELECT * FROM EDW.DBT_MONITORING.ALERT_SUMMARY_DASHBOARD;
```

---

### **Step 2: Create Snowsight Dashboard (5-10 minutes)**

1. **Open Snowsight**
   - Navigate to **Projects** → **Dashboards**
   - Click **+ Dashboard**
   - Name it: "DBT Observability Dashboard"

2. **Open Query Document**
   - Open file: `SNOWSIGHT_DASHBOARD_QUERIES.md`
   - This contains 30+ ready-to-use queries

3. **Add Your First Tile**
   - In the new dashboard, click **+ Tile**
   - Copy the **TILE 1** query from `SNOWSIGHT_DASHBOARD_QUERIES.md`
   - Paste into Snowsight
   - Click **Run**
   - Click **Save** and name the tile

4. **Repeat for More Tiles**
   - Recommended essential tiles:
     - ✅ **TILE 1:** Executive Summary Scorecard
     - ✅ **TILE 2:** Model Execution Trends
     - ✅ **TILE 3:** Test Coverage by Model
     - ✅ **TILE 6:** Failed Models & Tests
     - ✅ **TILE 11:** Performance Anomalies

---

### **Step 3: Verify Dashboard is Working**

Run these quick checks:

```sql
-- 1. Check if models are being tracked
SELECT 
    node_id,
    status,
    total_node_runtime,
    run_started_at
FROM EDW.DBT_MONITORING.MODEL_EXECUTIONS
ORDER BY run_started_at DESC
LIMIT 10;

-- 2. Check test results
SELECT 
    node_id,
    status,
    run_started_at
FROM EDW.DBT_MONITORING.TEST_EXECUTIONS
ORDER BY run_started_at DESC
LIMIT 10;

-- 3. Check for any alerts
SELECT * FROM EDW.DBT_MONITORING.ALERT_ALL_CRITICAL;

-- 4. View performance summary
SELECT * FROM EDW.DBT_MONITORING.SLOWEST_MODELS_CURRENT_WEEK;
```

**Expected Results:**
- ✅ MODEL_EXECUTIONS: Should have ~5 rows (your dbt models)
- ✅ TEST_EXECUTIONS: Should have rows for tests that ran
- ✅ Alerts may be empty (good - no issues!)
- ✅ Performance view should show your models

---

## 📊 Recommended Dashboard Layout

### **Section 1: Executive Summary (Top)**
- Tile 1: Executive Summary Scorecard (4-8 metrics)
- Tile 6: Failed Models & Tests (alert table)

### **Section 2: Performance & Trends**
- Tile 2: Model Execution Trends (line chart)
- Tile 11: Performance Anomalies (alert table)
- Tile 4: Slowest Models (bar chart)

### **Section 3: Quality & Testing**
- Tile 3: Test Coverage by Model (table)
- Tile 7: Test Pass Rate History (line chart)

### **Section 4: Resource Utilization**
- Tile 8: Warehouse Credits by Week (bar chart)
- Tile 5: Models Ranked by Compute (table)

---

## 🔍 Key Queries Reference

### **Quick Health Check**
```sql
SELECT * FROM EDW.DBT_MONITORING.ALERT_SUMMARY_DASHBOARD;
```

### **Today's Run Summary**
```sql
SELECT * FROM EDW.DBT_MONITORING.DAILY_EXECUTION_SUMMARY
WHERE execution_date = CURRENT_DATE();
```

### **Recent Model Runs**
```sql
SELECT 
    SPLIT_PART(node_id, '.', -1) as model_name,
    status,
    ROUND(total_node_runtime, 2) as seconds,
    run_started_at
FROM EDW.DBT_MONITORING.MODEL_EXECUTIONS
WHERE run_started_at >= CURRENT_DATE()
ORDER BY run_started_at DESC;
```

### **Test Results Today**
```sql
SELECT 
    node_id as test_name,
    status,
    run_started_at
FROM EDW.DBT_MONITORING.TEST_EXECUTIONS
WHERE run_started_at >= CURRENT_DATE()
ORDER BY status, run_started_at DESC;
```

---

## ⚠️ Troubleshooting

### **Problem:** Monitoring views are empty

**Solution:**
```sql
-- Check if Query History has data
SELECT COUNT(*) 
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'EDW'
  AND start_time >= CURRENT_DATE();

-- If this returns 0, run your dbt projects again
```

### **Problem:** "Object does not exist" error

**Solution:**
```sql
-- Verify monitoring schema exists
SHOW SCHEMAS LIKE 'DBT_MONITORING' IN DATABASE EDW;

-- If missing, run the setup script again
@MASTER_SETUP_QUERY_HISTORY.sql
```

### **Problem:** Some queries return errors

**Solution:** The updated queries in `SNOWSIGHT_DASHBOARD_QUERIES.md` (committed just now) are correct. Make sure you're using the latest version from git.

---

## 📚 Related Documentation

| Document | Purpose |
|----------|---------|
| **SNOWSIGHT_DASHBOARD_QUERIES.md** | All 30+ dashboard queries (main document) |
| **MASTER_SETUP_QUERY_HISTORY.sql** | One-time monitoring setup script |
| **COMPREHENSIVE_MONITORING_README.md** | Complete monitoring system documentation |
| **QUICK_START_MONITORING.md** | 5-minute monitoring quick start |

---

## ✅ Success Checklist

- [ ] `MASTER_SETUP_QUERY_HISTORY.sql` executed successfully
- [ ] `EDW.DBT_MONITORING` schema exists and has views
- [ ] `MODEL_EXECUTIONS` view has data
- [ ] `TEST_EXECUTIONS` view has data (if tests ran)
- [ ] Snowsight dashboard created
- [ ] At least 5 essential tiles added to dashboard
- [ ] All tiles showing data (not empty)
- [ ] Dashboard refreshing on schedule

---

## 🎉 You're Done!

Your dashboard is now tracking:
- ✅ Model execution performance
- ✅ Test pass/fail rates
- ✅ Warehouse costs
- ✅ Performance anomalies
- ✅ Data quality health

**Next Steps:**
- Set up Snowflake Alerts for critical failures (see `setup_notifications.sql`)
- Schedule dashboard to refresh every 15 minutes
- Share dashboard with team

**Monitoring runs automatically** - Query History captures everything! 🚀

