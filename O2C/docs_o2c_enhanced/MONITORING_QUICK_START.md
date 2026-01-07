# O2C Enhanced Monitoring - Quick Start Guide

**Last Updated:** January 2025  
**Status:** ✅ Ready to Use

---

## 🎯 What You Get

A complete **observability and monitoring stack** with:

- **75+ monitoring views** covering all aspects of your data platform
- **25 dashboard queries** for Snowsight dashboards
- **100% test coverage metrics** including all DQ test cases
- **Complete data observability** (freshness, errors, reconciliation, completeness)
- **Run, model, performance, error, and cost metrics**

---

## 🚀 Quick Start (5 Minutes)

### Step 1: Create All Views (One Command)

```bash
cd O2C/docs_o2c_enhanced
snowsql -f O2C_ALL_IN_ONE_MONITORING.sql
```

**What this does:**
- Creates `EDW.O2C_AUDIT` schema with 3 audit tables
- Points you to run 5 scripts that create 70+ monitoring views
- Provides verification queries

**Duration:** ~5 minutes total

---

### Step 2: Run the 5 Monitoring Scripts

Execute these in order:

```bash
# Navigate to scripts directory
cd O2C/docs_o2c_enhanced

# Run each script (each is idempotent - safe to re-run)
snowsql -f O2C_ENHANCED_TELEMETRY_SETUP.sql                    # 4 views
snowsql -f O2C_ENHANCED_MONITORING_SETUP.sql                   # 25 views
snowsql -f O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql        # 11 views
snowsql -f O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql    # 15 views
snowsql -f O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql          # 20 views
```

**Total:** 75+ views created

---

### Step 3: Verify Setup

```sql
-- Run this in Snowsight
SELECT 
    'Audit Tables' as component,
    COUNT(*) as count
FROM EDW.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'O2C_AUDIT' AND TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 
    'Audit Views',
    COUNT(*)
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_AUDIT'
UNION ALL
SELECT 
    'Monitoring Views',
    COUNT(*)
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING';

-- Expected Results:
-- Audit Tables: 3 ✅
-- Audit Views: 5 ✅
-- Monitoring Views: 70-75 ✅
```

---

### Step 4: Create Snowsight Dashboard

1. **Open Snowsight** → Dashboards → New Dashboard
2. **Name it:** "O2C Enhanced Monitoring"
3. **Copy dashboard queries** from `O2C_ALL_IN_ONE_MONITORING.sql` (TILE 1-25)
4. **Add tiles** (start with top 10, expand to all 25)
5. **Set refresh schedules:**
   - Real-time tiles (Tile 1, 16): Every 5 minutes
   - Operational tiles (Tile 2-11): Every 15-30 minutes
   - Daily summary tiles (Tile 12-25): Every 4 hours

---

## 📊 What's Covered

### ✅ Run Metrics
- **Tile 2:** Daily run summary (30 days)
- **Tile 3:** Run-level details (last 7 days)
- **Tile 4:** Execution timeline (Gantt view)
- **Tile 7:** Daily run summary with build performance

### ✅ Model Metrics
- **Tile 5:** Model performance dashboard (avg, max, cost, efficiency)
- **Tile 3:** Slowest models analysis
- **Tile 22:** Incremental model efficiency (rows/second)
- **Tile 19:** Model dependencies and lineage

### ✅ Error Analysis
- **Tile 8:** Error dashboard (categorization, frequency)
- **Tile 9:** Error trend analysis (30 days with anomaly detection)
- **Tile 10:** Model failure analysis
- **Tile 11:** Build failure details with root cause

### ✅ Test Metrics & DQ Coverage
- **Tile 12:** Test execution dashboard (complete coverage metrics)
- **Tile 13:** Test pass rate trend (30 days)
- **Tile 14:** Test coverage by model
- **Tile 20:** Recurring test failures
- **Tile 10:** Primary key validation
- **Tile 11:** Foreign key validation

### ✅ Data Observability
- **Tile 6:** Data quality metrics (freshness, row counts)
- **Tile 7:** Data flow validation (source-to-staging reconciliation)
- **Tile 18:** Null rate analysis (completeness %)
- **Tile 24:** Cross-table consistency validation
- **Tile 12:** Schema drift detection

### ✅ Performance Monitoring
- **Tile 9:** Long running queries (>1 min)
- **Tile 21:** Queue time analysis (concurrency issues)
- **Tile 5:** Model performance trends
- **Tile 22:** Incremental efficiency

### ✅ Cost Monitoring
- **Tile 8:** Daily cost tracking with 7-day MA
- **Tile 25:** Monthly cost with MoM comparison
- **Tile 5:** Cost per model

### ✅ Infrastructure
- **Tile 14:** Warehouse utilization
- **Tile 15:** Storage usage and growth
- **Tile 21:** Queue and concurrency

### ✅ Alert Management
- **Tile 1:** Platform health overview (executive scorecard)
- **Tile 16:** Active alerts summary
- All monitoring views have built-in alert logic

---

## 🗂️ File Reference

| File | Purpose | When to Use |
|------|---------|-------------|
| **`O2C_ALL_IN_ONE_MONITORING.sql`** | Single executable file with all views + queries | **⭐ Start here** - Run once to set up everything |
| `O2C_COMPLETE_MONITORING_MASTER.md` | Detailed guide with explanations | Reference for understanding what each component does |
| `MONITORING_QUICK_START.md` | This file - quick setup guide | Quick reference for setup steps |
| `O2C_ENHANCED_MONITORING_SETUP.sql` | Core 25 monitoring views | Part of setup (called from all-in-one) |
| `O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql` | Cost & performance views (11) | Part of setup |
| `O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql` | Schema, dbt, data quality (15) | Part of setup |
| `O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql` | Warehouse, storage, tasks (20) | Part of setup |
| `O2C_ENHANCED_TELEMETRY_SETUP.sql` | Data validation views (4) | Part of setup |

---

## 📈 Dashboard Tile Priority

### Priority 1: Must-Have (Create These First)
1. **Tile 1** - Platform Health Overview (executive scorecard)
2. **Tile 2** - Daily Run Summary
3. **Tile 5** - Model Performance Dashboard
4. **Tile 12** - Test Execution Dashboard
5. **Tile 8** - Error Analysis Dashboard
6. **Tile 6** - Data Quality Metrics
7. **Tile 16** - Active Alerts Summary

### Priority 2: Operational Monitoring
8. **Tile 3** - Run-Level Details
9. **Tile 10** - Model Failure Analysis
10. **Tile 13** - Test Pass Rate Trend
11. **Tile 9** - Long Running Queries
12. **Tile 7** - Data Flow Validation
13. **Tile 17** - Business KPIs

### Priority 3: Deep Dive & Optimization
14. **Tile 11** - Build Failure Details
15. **Tile 14** - Test Coverage by Model
16. **Tile 18** - Null Rate Analysis
17. **Tile 22** - Incremental Efficiency
18. **Tile 12** - Schema Drift Detection
19. **Tile 19** - Model Dependencies
20. **Tile 20** - Recurring Test Failures

### Priority 4: Infrastructure & Cost
21. **Tile 8** - Cost Dashboard
22. **Tile 25** - Monthly Cost Summary
23. **Tile 14** - Warehouse Utilization
24. **Tile 15** - Storage Usage
25. **Tile 21** - Queue Time Analysis

---

## 🔍 Common Use Cases

### "How is my platform doing overall?"
→ **Tile 1** - Platform Health Overview

### "Why did my build fail?"
→ **Tile 10** (Model Failures) + **Tile 11** (Build Failure Details)

### "Which models are slow?"
→ **Tile 5** (Model Performance) + **Tile 9** (Long Running Queries)

### "Are my tests passing?"
→ **Tile 12** (Test Dashboard) + **Tile 13** (Pass Rate Trend)

### "Is my data fresh and accurate?"
→ **Tile 6** (Data Quality) + **Tile 7** (Flow Validation) + **Tile 18** (Null Rates)

### "How much am I spending?"
→ **Tile 8** (Daily Cost) + **Tile 25** (Monthly Cost)

### "Which tables are missing tests?"
→ **Tile 14** - Test Coverage by Model

### "What errors are happening?"
→ **Tile 8** (Error Dashboard) + **Tile 9** (Error Trends)

---

## ⚙️ Configuration Options

### Adjust Refresh Schedules
Edit tile refresh in Snowsight based on your needs:
- **Critical tiles (1, 16):** 5 minutes
- **Operational (2-11):** 15-30 minutes
- **Daily summaries (12-25):** 4-8 hours

### Add Alert Notifications
All monitoring views have built-in alert logic. To enable email notifications:

1. Create a notification integration
2. Create Snowflake alerts using view filters
3. Example:
```sql
CREATE ALERT critical_model_failures
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '5 MINUTE'
  IF( EXISTS(
      SELECT 1 
      FROM EDW.O2C_ENHANCED_MONITORING.O2C_ENH_ALERT_MODEL_FAILURES
      WHERE severity = 'CRITICAL' 
      AND failure_time >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
  ))
  THEN CALL SYSTEM$SEND_EMAIL(...);
```

### Customize Thresholds
Edit the view definitions to adjust thresholds:
- Error rate thresholds
- Performance SLAs
- Cost anomaly detection sensitivity
- Test pass rate targets

---

## 🆘 Troubleshooting

### "I don't see any data in the views"
**Solution:** You need to populate the audit tables first. Run a dbt build and log the results to `EDW.O2C_AUDIT.DBT_RUN_LOG` and `DBT_MODEL_LOG`.

### "Some views are missing"
**Solution:** Verify you ran all 5 setup scripts:
```sql
SELECT TABLE_NAME 
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
ORDER BY TABLE_NAME;
```

### "Dashboard queries are slow"
**Solution:**  
1. Ensure you're using a medium or large warehouse
2. Add `LIMIT` clauses to large result sets
3. Consider materializing frequently accessed views as tables

### "I need to add custom metrics"
**Solution:** All scripts are idempotent. Edit the view definitions and re-run the scripts. Your customizations won't be lost if you use `CREATE OR REPLACE VIEW`.

---

## 📞 Support & Documentation

- **Master Guide:** `O2C_COMPLETE_MONITORING_MASTER.md` (detailed explanations)
- **All-in-One SQL:** `O2C_ALL_IN_ONE_MONITORING.sql` (executable setup + queries)
- **Unified Dashboard Queries:** See TILE 1-25 comments in SQL file
- **Project README:** `O2C_README.md` (overall O2C Enhanced documentation)

---

## ✅ Success Checklist

- [ ] Ran `O2C_ALL_IN_ONE_MONITORING.sql` (audit foundation)
- [ ] Ran 5 monitoring setup scripts (75+ views)
- [ ] Verified view counts (75+ monitoring views)
- [ ] Created Snowsight dashboard
- [ ] Added Priority 1 tiles (Tile 1, 2, 5, 12, 8, 6, 16)
- [ ] Set refresh schedules
- [ ] Tested queries with sample data
- [ ] (Optional) Configured alert notifications
- [ ] (Optional) Added remaining tiles as needed

---

## 🎉 You're Done!

You now have a **complete monitoring stack** covering:
- ✅ Run metrics
- ✅ Model performance
- ✅ Error analysis
- ✅ Test metrics & DQ
- ✅ Data observability
- ✅ Cost tracking
- ✅ Infrastructure monitoring
- ✅ Alert management

**Next:** Use the dashboard daily to monitor your O2C Enhanced platform health!

