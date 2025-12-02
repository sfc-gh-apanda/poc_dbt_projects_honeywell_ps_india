# O2C Platform Setup Guide

**Estimated Time:** 45 minutes  
**Prerequisites:** Snowflake access, dbt CLI or dbt Cloud

---

## 📋 Step 1: Snowflake Setup (10 min)

### **1.1 Create Roles and Users**

```sql
USE ROLE SECURITYADMIN;

-- Create O2C roles
CREATE ROLE IF NOT EXISTS DBT_O2C_DEVELOPER;
CREATE ROLE IF NOT EXISTS DBT_O2C_PROD;

-- Grant to your user
GRANT ROLE DBT_O2C_DEVELOPER TO USER <your_username>;
```

### **1.2 Create Warehouse**

```sql
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS DBT_O2C_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

GRANT USAGE ON WAREHOUSE DBT_O2C_WH TO ROLE DBT_O2C_DEVELOPER;
```

### **1.3 Grant Permissions**

```sql
-- Grant database access
GRANT ALL ON DATABASE EDW TO ROLE DBT_O2C_DEVELOPER;
GRANT ALL ON SCHEMA EDW.CORP_TRAN TO ROLE DBT_O2C_DEVELOPER;
GRANT ALL ON SCHEMA EDW.CORP_MASTER TO ROLE DBT_O2C_DEVELOPER;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE EDW TO ROLE DBT_O2C_DEVELOPER;
```

---

## 📊 Step 2: Load Sample Data (5 min)

```bash
# Run the data loading script
snowsql -f O2C_LOAD_SAMPLE_DATA.sql

# Verify data loaded
snowsql -q "SELECT COUNT(*) FROM EDW.CORP_TRAN.FACT_SALES_ORDERS"
```

---

## 🔧 Step 3: Configure dbt (10 min)

### **3.1 Set Environment Variables**

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
```

### **3.2 Install dbt**

```bash
pip install dbt-snowflake
```

### **3.3 Test Connection**

```bash
cd dbt_o2c
dbt debug
```

---

## 🚀 Step 4: Build O2C Data Platform (15 min)

```bash
cd dbt_o2c

# Install packages
dbt deps

# Build everything
dbt build

# Expected output:
# ✓ 6 sources tested
# ✓ 3 staging models created
# ✓ 9 mart models created
# ✓ 20+ tests passing
```

---

## 🎯 Step 5: Deploy Semantic Layer (5 min)

```bash
cd ../dbt_o2c_semantic

# Parse semantic models
dbt parse

# Expected output:
# ✓ 1 semantic model parsed
# ✓ 15+ metrics defined
# ✓ No warehouse objects created (metadata only)
```

---

## ✅ Step 6: Validate Setup

```bash
# Query the main mart
snowsql -q "SELECT * FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION LIMIT 10"

# Check metrics
dbt sl list metrics
```

---

## 🎉 Success Criteria

- ✅ All source tables have data
- ✅ All staging models built successfully
- ✅ All mart models built successfully
- ✅ All tests passing
- ✅ Semantic layer parsed without errors

---

## 🆘 Troubleshooting

**Issue:** Connection failed

**Solution:** Verify environment variables and Snowflake access

**Issue:** Tests failing

**Solution:** Check data quality in source tables

---

For more help, see `O2C_README.md`.

