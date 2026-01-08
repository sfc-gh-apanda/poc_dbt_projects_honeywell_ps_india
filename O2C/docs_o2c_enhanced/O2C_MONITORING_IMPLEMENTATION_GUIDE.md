# O2C Enhanced: Monitoring & Observability Implementation Guide

**Purpose:** Simple, clear guide for implementing complete O2C Enhanced observability  
**Duration:** 30-45 minutes  
**Audience:** Anyone setting up monitoring from scratch  
**Updated:** January 2026

---

## 🎯 What You Get

After following this guide, you'll have:

✅ **81+ monitoring views** covering all aspects of your dbt project  
✅ **33 dashboard tiles** ready to deploy in Snowsight  
✅ **Complete visibility** into:
- Project deployments, compiles, and runs
- Test validation and coverage
- Errors and logs
- Data quality and observability
- Model performance and cost
- Telemetry and tracking

---

## 📋 What You Need

### Prerequisites
1. **Snowflake Account** with ACCOUNTADMIN role
2. **Database EDW** exists
3. **O2C Enhanced dbt project** deployed
4. **30-45 minutes** of uninterrupted time

### Files You'll Use
Located in: `/O2C/docs_o2c_enhanced/`

1. `O2C_MONITORING_COMPLETE_SETUP.sql` - Creates all views (foundation)
2. `O2C_ENHANCED_MONITORING_SETUP.sql` - Core monitoring views
3. `O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql` - Cost & performance views
4. `O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql` - Schema & dbt views
5. `O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql` - Infrastructure views
6. `O2C_MONITORING_COMPLETE_DASHBOARD.md` - All dashboard queries

---

## 🚀 Implementation Steps (In Order)

### STEP 1: Create Foundation (5-10 minutes)

Run the first setup script to create audit tables and telemetry views:

```bash
# Method 1: SnowSQL
snowsql -f O2C_MONITORING_COMPLETE_SETUP.sql

# Method 2: Snowsight
# - Open O2C_MONITORING_COMPLETE_SETUP.sql in Snowsight
# - Click "Run All"
```

**What this creates:**
- 3 audit tables (`DBT_RUN_LOG`, `DBT_MODEL_LOG`, `DBT_BATCH_LINEAGE`)
- 8 views in `O2C_AUDIT` schema
- Foundation for all other monitoring

**Verification:**
```sql
-- Should return 3 tables + 5 views = 8 objects
SELECT COUNT(*) AS object_count
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_AUDIT'
UNION ALL
SELECT COUNT(*)
FROM EDW.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'O2C_AUDIT' AND TABLE_TYPE = 'BASE TABLE';
```

---

### STEP 2: Create Core Monitoring Views (5-10 minutes)

Run the core monitoring script:

```bash
snowsql -f O2C_ENHANCED_MONITORING_SETUP.sql
```

**What this creates:**
- 25 views in `O2C_ENHANCED_MONITORING` schema
- Model execution tracking
- Test execution monitoring
- Daily execution summaries
- Performance anomaly detection
- Error logs and trends
- Data freshness checks
- Business KPIs

**Verification:**
```sql
-- Should return 25 views
SELECT COUNT(*) AS view_count
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING';
```

---

### STEP 3: Add Cost & Performance Monitoring (5 minutes)

Run the cost and performance script:

```bash
snowsql -f O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql
```

**What this creates:**
- 11 additional views
- Daily cost tracking
- Cost by model attribution
- Queue time analysis
- Long-running query detection
- Compilation time trends
- Model performance trends
- Incremental efficiency metrics

**Verification:**
```sql
-- Should show cost and performance views
SELECT TABLE_NAME 
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
  AND (TABLE_NAME LIKE '%COST%' OR TABLE_NAME LIKE '%PERFORMANCE%' OR TABLE_NAME LIKE '%QUEUE%')
ORDER BY TABLE_NAME;
```

---

### STEP 4: Add Schema & dbt Integrity Monitoring (5-10 minutes)

Run the schema and dbt integrity script:

```bash
snowsql -f O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql
```

**What this creates:**
- 15 additional views
- Schema drift detection
- DDL change tracking
- Column change monitoring
- dbt test coverage analysis
- Model dependency tracking
- Run history
- Primary/Foreign key validation
- Duplicate detection
- Null rate analysis

**Verification:**
```sql
-- Should show schema, dbt, and integrity views
SELECT TABLE_NAME 
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
  AND (TABLE_NAME LIKE '%SCHEMA%' OR TABLE_NAME LIKE '%DBT%' OR TABLE_NAME LIKE '%PK%' OR TABLE_NAME LIKE '%FK%')
ORDER BY TABLE_NAME;
```

---

### STEP 5: Add Infrastructure Monitoring (5-10 minutes)

Run the infrastructure monitoring script:

```bash
snowsql -f O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql
```

**What this creates:**
- 25 additional views
- Warehouse utilization
- Storage usage and growth
- Security monitoring (logins, access patterns)
- Task and stream monitoring
- Concurrency analysis
- Lock wait detection
- Disk spill analysis

**Verification:**
```sql
-- Should show infrastructure views
SELECT TABLE_NAME 
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
  AND (TABLE_NAME LIKE '%WAREHOUSE%' OR TABLE_NAME LIKE '%STORAGE%' OR TABLE_NAME LIKE '%SECURITY%' OR TABLE_NAME LIKE '%TASK%')
ORDER BY TABLE_NAME;
```

---

### STEP 6: Final Verification (2 minutes)

Check that all 81+ views were created successfully:

```sql
-- Total view count
SELECT 
    'O2C_AUDIT' AS schema_name,
    COUNT(*) AS view_count
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_AUDIT'
UNION ALL
SELECT 
    'O2C_ENHANCED_MONITORING',
    COUNT(*)
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'O2C_ENHANCED_MONITORING'
UNION ALL
SELECT 
    'TOTAL',
    COUNT(*)
FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA IN ('O2C_AUDIT', 'O2C_ENHANCED_MONITORING');

-- Should show:
-- O2C_AUDIT: ~8 views
-- O2C_ENHANCED_MONITORING: ~76 views  
-- TOTAL: ~84 views (including audit tables)
```

---

### STEP 7: Set Up Dashboard (10-15 minutes)

Open `O2C_MONITORING_COMPLETE_DASHBOARD.md` and follow the instructions to:

1. **Create Snowsight Dashboard**
   - Name: "O2C Enhanced Complete Monitoring"
   - Description: "Comprehensive observability dashboard"

2. **Add 33 Tiles** (all queries provided in the file)
   - Tile 1-2: Executive Summary
   - Tile 3-6: Cost Monitoring  
   - Tile 7-10: Query Performance
   - Tile 11-14: Model Performance
   - Tile 15-18: Schema Drift
   - Tile 19-23: dbt Observability
   - Tile 24-29: Data Integrity
   - Tile 30-33: Alerts

3. **Set Refresh Schedules**
   - Critical alerts: Every 5 minutes
   - Performance metrics: Every 15-30 minutes
   - Cost metrics: Daily
   - Data quality: Daily

---

## 🎯 Quick Reference: What to Implement

### For Your Specific Needs:

| Your Requirement | Setup Steps | Dashboard Tiles | Key Views |
|------------------|-------------|-----------------|-----------|
| **Project Deployment / Compile / Run** | Steps 1-2 | Tiles 19-21 | `O2C_ENH_DBT_RUN_HISTORY`, `V_DAILY_RUN_SUMMARY` |
| **Test Validation** | Steps 1-2, 4 | Tiles 19, 22-23 | `O2C_ENH_TEST_*`, `O2C_ENH_DBT_TEST_COVERAGE` |
| **Error / Log Analysis** | Steps 1-2 | Tiles 12-13, 30-33 | `O2C_ENH_ERROR_*`, `O2C_ENH_BUILD_FAILURE_*` |
| **Data Quality & Observability** | Steps 1-2, 4 | Tiles 24-29 | `O2C_ENH_PK_VALIDATION`, `O2C_ENH_DATA_*` |
| **Model Performance & Cost** | Steps 1-3 | Tiles 3-6, 11-14 | `O2C_ENH_COST_*`, `O2C_ENH_MODEL_PERFORMANCE_*` |
| **Telemetry** | Steps 1-2 | Implicit in all | `V_ROW_COUNT_TRACKING`, `V_BATCH_TRACKING` |

**TL;DR:** Run all 5 setup steps (Steps 1-5) to get complete coverage of all your requirements.

---

## 📂 File Organization

### Setup Files (Run Once)
```
O2C/docs_o2c_enhanced/
├── O2C_MONITORING_COMPLETE_SETUP.sql              # STEP 1: Foundation
├── O2C_ENHANCED_MONITORING_SETUP.sql              # STEP 2: Core monitoring
├── O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql   # STEP 3: Cost & performance
├── O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql # STEP 4: Schema & dbt
└── O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql     # STEP 5: Infrastructure
```

### Dashboard File (Reference)
```
O2C/docs_o2c_enhanced/
└── O2C_MONITORING_COMPLETE_DASHBOARD.md           # STEP 7: All dashboard queries
```

---

## ✅ Success Criteria

After completing all steps, you should have:

- ✅ 84 database objects created (3 tables + 81 views)
- ✅ 33 dashboard tiles deployed in Snowsight
- ✅ Visibility into all aspects of your O2C Enhanced platform:
  - ✅ dbt run history and performance
  - ✅ Test execution and coverage
  - ✅ Cost attribution by model and warehouse
  - ✅ Data quality metrics (PK/FK, nulls, duplicates)
  - ✅ Data freshness (source and model)
  - ✅ Schema drift detection
  - ✅ Infrastructure utilization
  - ✅ Security monitoring
  - ✅ Active alerts and trends

---

## 🆘 Troubleshooting

### Issue: Views not found
**Solution:** Ensure you've run all 5 setup scripts in order. Check:
```sql
SELECT COUNT(*) FROM EDW.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA IN ('O2C_AUDIT', 'O2C_ENHANCED_MONITORING');
```

### Issue: Permission errors
**Solution:** Ensure you're running as ACCOUNTADMIN or have appropriate permissions:
```sql
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE DBT_O2C_DEVELOPER;
GRANT SELECT ON ALL VIEWS IN SCHEMA EDW.O2C_ENHANCED_MONITORING TO ROLE DBT_O2C_DEVELOPER;
```

### Issue: No data in views
**Solution:** Ensure your dbt project has run at least once. The monitoring views rely on `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` which has a latency of up to 45 minutes.

---

## 📞 Quick Help

**File too confusing?**  
Just run these 5 files in order, then reference the dashboard file for queries. That's it!

1. `O2C_MONITORING_COMPLETE_SETUP.sql`
2. `O2C_ENHANCED_MONITORING_SETUP.sql`
3. `O2C_ENHANCED_COST_PERFORMANCE_MONITORING.sql`
4. `O2C_ENHANCED_SCHEMA_DBT_INTEGRITY_MONITORING.sql`
5. `O2C_ENHANCED_INFRASTRUCTURE_MONITORING.sql`

**Need just specific metrics?**  
See the "Quick Reference: What to Implement" table above for exactly which steps and tiles you need.

**Ready to start?**  
Begin with STEP 1 above! 🚀

