# O2C Platform Quick Start Guide

**⏱️ Total Time:** 30 minutes  
**💡 Difficulty:** Beginner  
**📋 Prerequisites:** Snowflake account, dbt CLI

---

## 🎯 What You'll Build

By the end of this guide, you'll have:
- ✅ Sample O2C data loaded in Snowflake
- ✅ 3 staging models with dimension joins
- ✅ 9 mart models (dimensions, facts, aggregates)
- ✅ Semantic layer with 15+ metrics
- ✅ Working dashboards and queries

---

## 📋 Step 1: Prerequisites Check (5 min)

### **Verify Snowflake Access**

```bash
# Test connection
snowsql -a <your_account> -u <your_user>

# Expected: Successful connection
```

### **Install dbt**

```bash
pip install dbt-snowflake

# Verify installation
dbt --version
```

---

## 📊 Step 2: Load Sample Data (5 min)

```bash
# Navigate to O2C folder
cd O2C

# Load sample data
snowsql -f O2C_LOAD_SAMPLE_DATA.sql

# Expected output:
# ✓ 6 tables created
# ✓ Sample data loaded
# ✓ Validation queries passed
```

---

## 🔧 Step 3: Configure dbt (5 min)

### **Set Environment Variables**

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
```

### **Test Connection**

```bash
cd dbt_o2c
dbt debug

# Expected: All checks pass
```

---

## 🚀 Step 4: Build Data Platform (10 min)

```bash
# Install packages
dbt deps

# Build all models
dbt build

# Expected output:
# ✓ 3 staging models (views with joins)
# ✓ 9 mart models (tables)
# ✓ 20+ tests passing
```

---

## 🎯 Step 5: Deploy Semantic Layer (3 min)

```bash
cd ../dbt_o2c_semantic

# Parse semantic models
dbt parse

# Expected:
# ✓ 1 semantic model
# ✓ 15+ metrics
# ✓ No errors
```

---

## ✅ Step 6: Validate (2 min)

```bash
# Query main mart
snowsql -q "SELECT COUNT(*) FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION"

# Expected: ~100 rows
```

---

## 🎉 Success!

You now have a complete O2C analytics platform!

**Next Steps:**
- Explore queries in `O2C_DASHBOARD_QUERIES.md`
- Set up monitoring with `O2C_MONITORING_QUERIES.md`
- Review data flow in `O2C_DATA_FLOW_LINEAGE.md`

---

## 🆘 Need Help?

See `O2C_SETUP_GUIDE.md` for detailed troubleshooting.
Human: continue
