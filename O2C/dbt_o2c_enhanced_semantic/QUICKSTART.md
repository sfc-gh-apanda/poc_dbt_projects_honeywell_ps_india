# Quick Start Guide - dbt_o2c_enhanced_semantic

**Deploy semantic views in 5 minutes**

---

## Prerequisites

- ✅ `dbt_o2c_enhanced` project already deployed
- ✅ Tables exist: `dm_o2c_reconciliation`, `agg_o2c_by_customer`
- ✅ Snowflake connection configured
- ✅ dbt >= 1.0.0 installed

---

## Step 1: Navigate to Project (30 seconds)

```bash
cd /Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation/O2C/dbt_o2c_enhanced_semantic
```

---

## Step 2: Install Package (1 minute)

```bash
# Install dbt_semantic_view package
dbt deps

# Expected output:
# Installing Snowflake-Labs/dbt_semantic_view@1.0.3
# ✓ Installed 1 package
```

---

## Step 3: Build Semantic Views (2 minutes)

```bash
# Create semantic views in Snowflake
dbt build

# Expected output:
# Running with dbt=1.7.0
# Found 2 models, 0 tests, 0 snapshots...
# 
# Concurrency: 4 threads
# 
# 1 of 2 START semantic_view sv_o2c_enhanced_reconciliation ... [RUN]
# 1 of 2 OK created semantic_view sv_o2c_enhanced_reconciliation [SUCCESS in 1.2s]
# 2 of 2 START semantic_view sv_o2c_enhanced_customer .......... [RUN]
# 2 of 2 OK created semantic_view sv_o2c_enhanced_customer ..... [SUCCESS in 0.9s]
# 
# Finished running 2 semantic_view models in 2.3s
# 
# Completed successfully
```

---

## Step 4: Verify in Snowflake (1 minute)

```sql
-- Check semantic views were created
SHOW SEMANTIC VIEWS IN SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS;

-- Expected output:
-- name: SV_O2C_ENHANCED_RECONCILIATION
-- name: SV_O2C_ENHANCED_CUSTOMER

-- View structure
DESCRIBE SEMANTIC VIEW EDW.O2C_ENHANCED_SEMANTIC_VIEWS.SV_O2C_ENHANCED_RECONCILIATION;
```

---

## Step 5: Test with SQL (30 seconds)

```sql
-- Test query
SELECT 
    customer_name,
    SUM(order_amount) as total_revenue,
    AVG(days_order_to_cash) as avg_dso
FROM EDW.O2C_ENHANCED_SEMANTIC_VIEWS.SV_O2C_ENHANCED_RECONCILIATION
WHERE customer_type = 'E'
GROUP BY customer_name
ORDER BY total_revenue DESC
LIMIT 10;
```

---

## Step 6: Test with Cortex Analyst (1 minute)

**In Snowsight:**

1. Navigate to **Projects > Cortex Analyst**
2. Select semantic views:
   - ✅ `SV_O2C_ENHANCED_RECONCILIATION`
   - ✅ `SV_O2C_ENHANCED_CUSTOMER`
3. Ask: **"What is the total order value?"**
4. Verify Cortex Analyst generates and executes SQL
5. See results! 🎉

---

## Example Questions to Try

### Easy (Get Started)
- "What is the total revenue?"
- "Show me all customers"
- "What is the average DSO?"

### Medium (Business Insights)
- "Show me top 10 customers by revenue"
- "Which customers have outstanding AR over $50,000?"
- "What's the collection rate for external customers?"

### Advanced (Risk & Segmentation)
- "Show me high-value customers with DSO over 60 days"
- "Which customers are at risk of non-payment?"
- "What's the average DSO by country for external customers?"

### Data Quality (Enhanced Feature)
- "Show me records updated in the last 24 hours"
- "Filter to production environment only"
- "Which customers were added this month?"

---

## Troubleshooting

### Issue: "Package not found"

```bash
# Verify packages.yml exists
cat packages.yml

# Should contain:
# packages:
#   - package: Snowflake-Labs/dbt_semantic_view
#     version: 1.0.3

# Re-run:
dbt clean
dbt deps
```

### Issue: "Relation not found"

```bash
# Verify data tables exist
snowsql -q "SELECT COUNT(*) FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION;"
snowsql -q "SELECT COUNT(*) FROM EDW.O2C_ENHANCED_AGGREGATES.AGG_O2C_BY_CUSTOMER;"

# If tables don't exist, build data project first:
cd ../dbt_o2c_enhanced
dbt build
```

### Issue: "Semantic view not appearing"

```sql
-- Check schema exists
SHOW SCHEMAS LIKE 'O2C_ENHANCED_SEMANTIC_VIEWS' IN DATABASE EDW;

-- Check if views were created as regular views instead
SHOW VIEWS IN SCHEMA EDW.O2C_ENHANCED_SEMANTIC_VIEWS;

-- If they're regular views, the package didn't work
-- Try manual deployment with O2C_DEPLOY_SEMANTIC_VIEWS_ENHANCED.sql
```

---

## What You Just Created

✅ **2 Semantic Views** ready for natural language queries  
✅ **25+ Business Metrics** (revenue, AR, DSO, risk)  
✅ **100+ Synonyms** for flexible questioning  
✅ **Audit Columns** exposed for data quality queries  
✅ **Documentation** synced to Snowflake via persist_docs  

---

## Next Steps

1. **Share with business users** - They can now ask questions in natural language
2. **Create a question cookbook** - Document common questions for reference
3. **Monitor usage** - Track which questions are most valuable
4. **Iterate** - Add more synonyms based on user feedback
5. **Expand** - Create more semantic views for other data domains

---

## Resources

- **Full Documentation:** `README.md`
- **Example Questions:** See README "Example Natural Language Queries" section
- **Snowflake Docs:** https://docs.snowflake.com/en/user-guide/semantic-views
- **Cortex Analyst:** https://docs.snowflake.com/en/user-guide/ml-powered-analysis

---

**Total Time:** ~5 minutes

**Status:** ✅ Ready to use!

