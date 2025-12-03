# Dynamic Warehouse Configuration Guide

**Project:** Honeywell O2C Analytics Platform  
**Feature:** Metadata-Driven Warehouse Configuration  
**Date:** December 2024

---

## 📊 Overview

This feature allows you to **change warehouse assignments without modifying any dbt code**. Simply update a configuration table in Snowflake, and the next dbt run will use the new warehouse.

### **Key Benefits**

| Benefit | Description |
|---------|-------------|
| **No Code Changes** | Just UPDATE a table in Snowflake |
| **Immediate Effect** | Next dbt run picks up the change |
| **No Deployment** | No CI/CD pipeline needed |
| **Audit Trail** | Built-in tracking of who changed what |
| **Hierarchical Fallback** | MODEL → LAYER → PROJECT → DEFAULT → profiles.yml |
| **Self-Service** | Data engineers can adjust without DevOps |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    WAREHOUSE RESOLUTION                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   dbt build (runs model)                                        │
│        │                                                         │
│        ▼                                                         │
│   get_warehouse() macro                                          │
│        │                                                         │
│        ▼                                                         │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │ Query: EDW.CONFIG.DBT_WAREHOUSE_CONFIG                  │   │
│   │                                                         │   │
│   │ Priority Order:                                         │   │
│   │   1. MODEL (dm_o2c_reconciliation) → priority 10       │   │
│   │   2. LAYER (marts, staging)        → priority 30       │   │
│   │   3. PROJECT (dbt_o2c)             → priority 40       │   │
│   │   4. ENVIRONMENT (dev, prod)       → priority 50       │   │
│   │   5. DEFAULT                       → priority 100      │   │
│   └─────────────────────────────────────────────────────────┘   │
│        │                                                         │
│        ▼                                                         │
│   Found in table? ──Yes──→ USE WAREHOUSE {from_table}           │
│        │                                                         │
│       No                                                         │
│        │                                                         │
│        ▼                                                         │
│   USE WAREHOUSE {from profiles.yml} (ultimate fallback)         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Setup Instructions

### **Step 1: Run Setup Script**

Execute in Snowflake:

```sql
-- Run the setup script
@O2C/O2C_WAREHOUSE_CONFIG_SETUP.sql
```

Or copy the contents of `O2C_WAREHOUSE_CONFIG_SETUP.sql` and execute in Snowsight.

This creates:
- `EDW.CONFIG.DBT_WAREHOUSE_CONFIG` - Main configuration table
- `EDW.CONFIG.DBT_WAREHOUSE_CONFIG_HISTORY` - Audit trail
- Stored procedures for safe updates
- Default configuration entries

### **Step 2: Verify Setup**

```sql
SELECT * FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG ORDER BY priority;
```

You should see default entries for:
- DEFAULT (global fallback)
- ENVIRONMENT (dev, prod)
- PROJECT (dbt_o2c)
- LAYER (staging, marts)

### **Step 3: Run dbt**

```bash
cd O2C/dbt_o2c
dbt deps
dbt build
```

The macro will automatically read from the config table.

---

## 📋 Configuration Table Schema

```sql
EDW.CONFIG.DBT_WAREHOUSE_CONFIG

| Column          | Type        | Description                                    |
|-----------------|-------------|------------------------------------------------|
| config_scope    | VARCHAR(50) | 'MODEL', 'LAYER', 'PROJECT', 'ENVIRONMENT', 'DEFAULT' |
| scope_name      | VARCHAR(200)| The identifier (model name, layer name, etc.)  |
| warehouse_name  | VARCHAR(100)| Target Snowflake warehouse                     |
| priority        | INTEGER     | Lower = higher priority (MODEL=10, DEFAULT=100)|
| is_active       | BOOLEAN     | Enable/disable this config                     |
| effective_from  | DATE        | Start date for this config                     |
| effective_to    | DATE        | End date (NULL = no end)                       |
| notes           | VARCHAR     | Documentation / change reason                  |
| updated_at      | TIMESTAMP   | Last modification time                         |
| updated_by      | VARCHAR     | User who made last change                      |
```

---

## 🔧 Common Operations

### **1. Change Warehouse for a Specific Model**

When a model times out or needs more resources:

```sql
-- Option A: Use stored procedure (logs to history)
CALL EDW.CONFIG.UPDATE_WAREHOUSE_CONFIG(
    'MODEL',                          -- scope
    'dm_o2c_reconciliation',          -- model name
    'COMPUTE_WH_LARGE',               -- new warehouse
    'Upgraded due to timeout'         -- notes
);

-- Option B: Direct UPDATE
UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
SET warehouse_name = 'COMPUTE_WH_LARGE',
    updated_at = CURRENT_TIMESTAMP(),
    updated_by = CURRENT_USER(),
    notes = 'Upgraded due to timeout'
WHERE config_scope = 'MODEL' 
  AND scope_name = 'dm_o2c_reconciliation';
```

### **2. Add Config for a New Model**

```sql
CALL EDW.CONFIG.ADD_WAREHOUSE_CONFIG(
    'MODEL',                          -- scope
    'agg_o2c_by_period',              -- model name
    'COMPUTE_WH_LARGE',               -- warehouse
    10,                               -- priority (MODEL level)
    'Heavy time-series aggregation'   -- notes
);
```

### **3. Change Default for All Marts**

```sql
UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
SET warehouse_name = 'COMPUTE_WH_LARGE'
WHERE config_scope = 'LAYER' 
  AND scope_name = 'marts';
```

### **4. Temporarily Disable a Config**

```sql
UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
SET is_active = FALSE
WHERE scope_name = 'dm_o2c_reconciliation';
-- Model will fall back to LAYER or PROJECT config
```

### **5. View Change History**

```sql
SELECT 
    scope_name,
    old_warehouse,
    new_warehouse,
    action,
    changed_at,
    changed_by,
    notes
FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG_HISTORY
ORDER BY changed_at DESC
LIMIT 20;
```

### **6. Check Current Config for a Model**

```sql
-- What warehouse will 'dm_o2c_reconciliation' use?
SELECT 
    config_scope,
    scope_name,
    warehouse_name,
    priority
FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
WHERE is_active = TRUE
  AND scope_name IN ('dm_o2c_reconciliation', 'marts', 'dbt_o2c', 'DEFAULT')
ORDER BY priority;
-- First row = what will be used
```

---

## 📊 Priority Reference

| config_scope | Priority | Example scope_name | Use Case |
|--------------|----------|-------------------|----------|
| **MODEL** | 10 | `dm_o2c_reconciliation` | Specific heavy models |
| **LAYER** | 30 | `staging`, `marts` | All models in a layer |
| **PROJECT** | 40 | `dbt_o2c` | All models in a project |
| **ENVIRONMENT** | 50 | `dev`, `prod` | Environment-specific |
| **DEFAULT** | 100 | `DEFAULT` | Global fallback |

**Lower priority number = Higher precedence**

---

## 🔄 Workflow: Handling a Timeout

```
1️⃣ Model dm_o2c_reconciliation times out on COMPUTE_WH

2️⃣ Check current config:
   SELECT * FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
   WHERE scope_name = 'dm_o2c_reconciliation';
   → Not found (using LAYER default)

3️⃣ Add model-specific config:
   CALL EDW.CONFIG.ADD_WAREHOUSE_CONFIG(
       'MODEL', 'dm_o2c_reconciliation', 
       'COMPUTE_WH_LARGE', 10, 'Timeout fix'
   );

4️⃣ Re-run dbt:
   dbt build --select dm_o2c_reconciliation

5️⃣ Success! ✅
   → No code changes needed
   → Change logged in history table
```

---

## 🛡️ Fallback Behavior

The system **never fails** due to missing configuration:

| Scenario | What Happens |
|----------|--------------|
| Model has config | Uses model-specific warehouse |
| Model missing, layer has config | Uses layer warehouse |
| Layer missing, project has config | Uses project warehouse |
| Project missing, DEFAULT exists | Uses DEFAULT warehouse |
| Config table empty | Uses `profiles.yml` warehouse |
| Config table doesn't exist | Uses `profiles.yml` warehouse |
| Query fails | Uses `profiles.yml` warehouse |

---

## 📁 Files Involved

```
O2C/
├── O2C_WAREHOUSE_CONFIG_SETUP.sql     ← Run this in Snowflake
├── O2C_DYNAMIC_WAREHOUSE_GUIDE.md     ← This documentation
│
└── dbt_o2c/
    ├── dbt_project.yml                 ← Uses get_warehouse() macro
    └── macros/
        └── get_warehouse.sql           ← Dynamic lookup macro
```

---

## ❓ FAQ

### Q: Do I need to re-deploy dbt after changing warehouse?
**A: No.** Just update the config table and re-run `dbt build`.

### Q: What if the config table doesn't exist?
**A:** dbt falls back to `profiles.yml` warehouse. It never fails.

### Q: Can different environments use different warehouses?
**A:** Yes, add ENVIRONMENT-level configs for 'dev' and 'prod'.

### Q: How do I see what warehouse a model will use?
**A:** Query the config table sorted by priority, or check dbt logs.

### Q: Can I use different warehouses for different runs of the same model?
**A:** Yes, update the config between runs.

### Q: Is there an audit trail?
**A:** Yes, all changes are logged to `DBT_WAREHOUSE_CONFIG_HISTORY`.

---

## 📚 Related Documentation

- `O2C_WAREHOUSE_CONFIG_SETUP.sql` - Setup script
- `dbt_o2c/macros/get_warehouse.sql` - Macro implementation
- `O2C_BUILD_INSTRUCTIONS.md` - Build process

---

## ✅ Quick Reference

```sql
-- View all configs
SELECT * FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG ORDER BY priority;

-- Change a model's warehouse
CALL EDW.CONFIG.UPDATE_WAREHOUSE_CONFIG('MODEL', 'model_name', 'NEW_WH', 'reason');

-- Add new config
CALL EDW.CONFIG.ADD_WAREHOUSE_CONFIG('MODEL', 'model_name', 'WH_NAME', 10, 'notes');

-- View history
SELECT * FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG_HISTORY ORDER BY changed_at DESC;
```

