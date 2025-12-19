# Auto-Restart Quick Reference

## Overview

**Lightweight auto-restart solution for Snowflake Native dbt** - overcomes the stateless limitation by using existing audit tables as external state store.

## How It Works

```
Task runs (scheduled or manual)
  ↓
Procedure: RUN_DBT_AUTO_RESTART executes
  ↓
Queries DBT_MODEL_LOG: "Any failures in last run?"
  ↓
├─ NO failures  → Full build: dbt build
└─ YES failures → Selective retry: dbt run --select failed_model+ another_failed+
  ↓
dbt executes
  ↓
Post-hooks log to DBT_MODEL_LOG (as always)
  ↓
Next run: Procedure checks state again...
```

## What Changed

### ✅ Created
- **Stored Procedure:** `EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART`
- **Monitoring View:** `EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY`

### ✅ Modified
- **Task:** `O2C_DAILY_BUILD` - Now calls procedure instead of direct SYSTEM$DBT_RUN
- **Task:** `O2C_HOURLY_INCREMENTAL` - Now uses auto-restart

### ❌ No Changes Required
- dbt project code (models, macros, configs)
- Audit tables (already exist)
- DBT_MODEL_LOG structure
- Any YAML files

## Setup (One-Time)

```sql
-- Run the setup script
@/path/to/O2C_ENHANCED_AUTO_RESTART_SETUP.sql

-- That's it! ✅
```

## Usage

### Automatic (Scheduled)
Tasks automatically use auto-restart - no action needed.

### Manual Execution
```sql
-- Run with auto-restart capability
CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced');

-- Returns one of:
-- "🔵 FULL BUILD: Normal scheduled run"
-- "🔄 RETRY #1: Rerunning failed models: model_a+ model_b+"
-- "🔵 FULL BUILD: Max retries reached, reset with full build"
```

## Monitoring

### View Run History
```sql
SELECT * FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY 
ORDER BY run_started_at DESC 
LIMIT 20;
```

**Columns:**
- `run_type` - Shows `🔄 SELECTIVE RETRY` or `🔵 FULL BUILD`
- `models_executed` - Which models ran
- `failed_models` - Which models failed (if any)
- `retry_sequence` - Retry attempt number

### Check Task Status
```sql
SHOW TASKS IN SCHEMA EDW.O2C_AUDIT;

-- View task execution history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'O2C_DAILY_BUILD',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC;
```

### Retry Statistics
```sql
SELECT 
    DATE(run_started_at) AS run_date,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN run_type = '🔄 SELECTIVE RETRY' THEN 1 ELSE 0 END) AS retries,
    SUM(CASE WHEN run_type = '🔵 FULL BUILD' THEN 1 ELSE 0 END) AS full_builds,
    SUM(CASE WHEN run_status = 'FAILED' THEN 1 ELSE 0 END) AS failures
FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY
GROUP BY DATE(run_started_at)
ORDER BY run_date DESC;
```

## Retry Logic

### When It Retries
- Previous run status = `FAILED`
- Failed models identified in `DBT_MODEL_LOG`
- Failure is less than 24 hours old (configurable)
- Less than 3 retry attempts already made

### When It Does Full Build
- Previous run status = `SUCCESS` (normal operation)
- No previous run (first time)
- Max retries (3) reached
- Failure is older than 24 hours

### What Gets Retried
**Failed models + all downstream dependencies**

Example:
```
If staging.stg_orders fails:
  Retry: stg_orders+
  
  Automatically includes:
  - stg_orders (the failed model)
  - dm_o2c_reconciliation (depends on stg_orders)
  - agg_o2c_by_customer (depends on dm_o2c_reconciliation)
  - All other downstream models
```

The `+` selector is dbt's native syntax for "model and its children"

## Configuration

### Retry Time Window
```sql
-- Default: 24 hours
CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced', 24);

-- Shorter window for hourly tasks: 2 hours
CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced', 2);
```

### Max Retry Attempts
Currently hardcoded to 3 in the procedure. To change:
```sql
-- Edit line in procedure:
AND retry_count < 3  -- Change this number
```

## Testing

### Test 1: Normal Run (No Failures)
```sql
-- Should execute full build
CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced');
-- Expected: "🔵 FULL BUILD: Normal scheduled run"
```

### Test 2: Simulate Failure and Retry
```sql
-- 1. Simulate a model failure
INSERT INTO EDW.O2C_AUDIT.DBT_MODEL_LOG (
    log_id, run_id, project_name, model_name, schema_name, database_name,
    materialization, batch_id, status, started_at, ended_at, is_incremental
) VALUES (
    'test-fail-001', 'test-run-001', 'dbt_o2c_enhanced', 'dm_o2c_reconciliation',
    'O2C_ENHANCED_CORE', 'EDW', 'incremental', 'batch-001', 'FAIL',
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE
);

INSERT INTO EDW.O2C_AUDIT.DBT_RUN_LOG (
    run_id, project_name, environment, run_started_at, run_status, models_run, models_failed
) VALUES (
    'test-run-001', 'dbt_o2c_enhanced', 'prod', CURRENT_TIMESTAMP(), 'FAILED', 25, 1
);

-- 2. Run procedure (should detect and retry)
CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced');
-- Expected: "🔄 RETRY #1: Rerunning failed models: dm_o2c_reconciliation+"

-- 3. Verify in monitoring view
SELECT * FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY LIMIT 5;

-- 4. Cleanup test data
DELETE FROM EDW.O2C_AUDIT.DBT_MODEL_LOG WHERE log_id = 'test-fail-001';
DELETE FROM EDW.O2C_AUDIT.DBT_RUN_LOG WHERE run_id = 'test-run-001';
```

### Test 3: Manual Task Execution
```sql
-- Trigger task manually
EXECUTE TASK O2C_DAILY_BUILD;

-- Check result
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'O2C_DAILY_BUILD',
    SCHEDULED_TIME_RANGE_START => DATEADD('minute', -10, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC
LIMIT 1;
```

## Troubleshooting

### Issue: Procedure not retrying despite failure

**Check 1:** Verify failure is logged in DBT_MODEL_LOG
```sql
SELECT * FROM EDW.O2C_AUDIT.DBT_MODEL_LOG 
WHERE status IN ('FAIL', 'ERROR')
ORDER BY ended_at DESC 
LIMIT 10;
```

**Check 2:** Verify run status is FAILED
```sql
SELECT * FROM EDW.O2C_AUDIT.DBT_RUN_LOG 
ORDER BY run_started_at DESC 
LIMIT 5;
```

**Check 3:** Check if failure is too old (>24 hours)
```sql
SELECT 
    run_id,
    run_started_at,
    DATEDIFF('hour', run_started_at, CURRENT_TIMESTAMP()) AS hours_ago,
    CASE 
        WHEN DATEDIFF('hour', run_started_at, CURRENT_TIMESTAMP()) > 24 
        THEN '⚠️ Too old for auto-retry'
        ELSE '✅ Within retry window'
    END AS retry_eligible
FROM EDW.O2C_AUDIT.DBT_RUN_LOG 
WHERE run_status = 'FAILED'
ORDER BY run_started_at DESC;
```

### Issue: Too many retries

**Check retry count:**
```sql
SELECT 
    DATE(run_started_at) AS date,
    COUNT(*) AS retry_attempts
FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY
WHERE run_type = '🔄 SELECTIVE RETRY'
GROUP BY DATE(run_started_at)
ORDER BY date DESC;
```

**Action:** Max is 3 retries, then automatic full build reset

### Issue: Retrying wrong models

**Check selector logic:**
```sql
-- See what selector was built
SELECT 
    r.run_id,
    LISTAGG(DISTINCT m.model_name || '+', ' ') AS selector_built
FROM EDW.O2C_AUDIT.DBT_RUN_LOG r
JOIN EDW.O2C_AUDIT.DBT_MODEL_LOG m ON r.run_id = m.run_id
WHERE r.run_status = 'FAILED'
  AND m.status IN ('FAIL', 'ERROR')
GROUP BY r.run_id
ORDER BY r.run_started_at DESC
LIMIT 1;
```

## Architecture Benefits

| Feature | Benefit |
|---------|---------|
| **Stateless-compatible** | Uses DB tables instead of target/ directory |
| **Lightweight** | 60 lines of SQL, no new infrastructure |
| **Zero project changes** | dbt code stays exactly as-is |
| **Compute efficient** | Only reruns failed models + downstream |
| **Self-healing** | Automatic detection and retry |
| **Observable** | Full visibility via monitoring view |
| **Fail-safe** | Falls back to full build on errors |

## Cost Savings Example

**Scenario:** Model 20 of 25 fails (model runtime: 10 sec each)

**Without auto-restart:**
- Manual full rebuild: 25 models × 10 sec = **250 seconds**

**With auto-restart:**
- Selective retry: 6 models (failed + 5 downstream) × 10 sec = **60 seconds**
- **Savings: 76% compute time**

## Next Steps

1. ✅ Setup complete - auto-restart is active
2. Monitor runs in `V_AUTO_RESTART_HISTORY`
3. Review retry patterns weekly
4. Adjust `max_retry_age_hours` if needed for your use case

## Support

For questions or issues:
1. Check `V_AUTO_RESTART_HISTORY` for run details
2. Review `DBT_MODEL_LOG` for failure patterns
3. Check task history in INFORMATION_SCHEMA.TASK_HISTORY
4. Review stored procedure code in `O2C_ENHANCED_AUTO_RESTART_SETUP.sql`

