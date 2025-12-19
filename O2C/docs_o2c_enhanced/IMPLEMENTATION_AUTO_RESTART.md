# Auto-Restart Implementation Summary

## Problem Statement

**Challenge:** Snowflake Native dbt is stateless - it doesn't maintain a persistent `target/` directory between runs. This means:
- No `manifest.json` or `run_results.json` files persist
- Cannot use dbt's native `result:error+` or `result:fail+` selectors
- Cannot automatically restart from failure point
- Must manually identify and rerun failed models

**Impact:** When model 15 of 25 fails, you must either:
1. Rerun all 25 models (wasting compute)
2. Manually identify failed models and their dependencies (time-consuming)

## Solution Implemented

**Lightweight Auto-Restart using Audit Tables as External State Store**

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Snowflake Native dbt (Stateless)                           │
│  ✗ No target/ directory                                     │
│  ✗ No manifest.json                                         │
│  ✗ No run_results.json                                      │
└─────────────────────────────────────────────────────────────┘
                          ↓
           ┌──────────────────────────┐
           │  SOLUTION: External State│
           │  (Database Tables)       │
           └──────────────────────────┘
                          ↓
    ┌──────────────────────────────────────────┐
    │  DBT_MODEL_LOG (Already Exists)          │
    │  - Tracks every model execution          │
    │  - Records success/failure status        │
    │  - Populated by existing post-hooks      │
    └──────────────────────────────────────────┘
                          ↓
    ┌──────────────────────────────────────────┐
    │  RUN_DBT_AUTO_RESTART Procedure (NEW)    │
    │  1. Query DBT_MODEL_LOG                  │
    │  2. Identify failed models               │
    │  3. Build dbt selector: "model_a+ model_b+"│
    │  4. Call SYSTEM$DBT_RUN with selector    │
    └──────────────────────────────────────────┘
                          ↓
    ┌──────────────────────────────────────────┐
    │  Tasks call procedure instead of direct  │
    │  CALL RUN_DBT_AUTO_RESTART(...)          │
    └──────────────────────────────────────────┘
```

### Key Insight

**DBT_MODEL_LOG is the persistent state that Snowflake Native dbt lacks.**

Your existing audit infrastructure already tracks:
- ✅ Which models ran
- ✅ Which models failed
- ✅ When they ran
- ✅ What run they belong to

We simply query this to reconstruct what dbt would have stored in `run_results.json`.

## What Was Created

### 1. Stored Procedure: `RUN_DBT_AUTO_RESTART`

**Location:** `EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART`

**Purpose:** Intelligent wrapper around `SYSTEM$DBT_RUN` that:
- Checks DBT_MODEL_LOG for recent failures
- Decides between full build vs. selective retry
- Builds dbt selector for failed models + downstream
- Executes appropriate dbt command

**Code:** 60 lines of SQL

**Logic Flow:**
```sql
DECLARE
    last_run_status VARCHAR;
    failed_models_selector VARCHAR;
BEGIN
    -- Query external state (DBT_MODEL_LOG)
    SELECT run_status, LISTAGG(model_name || '+', ' ')
    INTO last_run_status, failed_models_selector
    FROM DBT_RUN_LOG r
    JOIN DBT_MODEL_LOG m ON r.run_id = m.run_id
    WHERE m.status IN ('FAIL', 'ERROR')
    ...
    
    -- Decision logic
    IF (last_run_status = 'FAILED' AND failed_models_selector IS NOT NULL) THEN
        -- Selective retry
        CALL SYSTEM$DBT_RUN('dbt_o2c_enhanced', 
            'dbt run --select ' || failed_models_selector);
    ELSE
        -- Full build
        CALL SYSTEM$DBT_RUN('dbt_o2c_enhanced', 'dbt build');
    END IF;
END;
```

**Parameters:**
- `project_name` - dbt project name (default: 'dbt_o2c_enhanced')
- `max_retry_age_hours` - How old failures to consider (default: 24)

**Returns:** Status message indicating action taken

### 2. Monitoring View: `V_AUTO_RESTART_HISTORY`

**Location:** `EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY`

**Purpose:** Shows run history with retry detection

**Key Columns:**
- `run_type` - `🔄 SELECTIVE RETRY` or `🔵 FULL BUILD`
- `models_executed` - Which models ran
- `failed_models` - Which models failed
- `retry_sequence` - Retry attempt number

**Query:**
```sql
SELECT * FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY 
ORDER BY run_started_at DESC 
LIMIT 20;
```

### 3. Updated Tasks

**Before:**
```sql
CREATE TASK O2C_DAILY_BUILD AS
    CALL SYSTEM$DBT_RUN('dbt_o2c_enhanced', 'dbt build');
```

**After:**
```sql
CREATE TASK O2C_DAILY_BUILD AS
    CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced');
```

**Tasks Updated:**
- `O2C_DAILY_BUILD` - Daily full build with auto-restart
- `O2C_HOURLY_INCREMENTAL` - Hourly incremental with auto-restart

## What Did NOT Change

✅ **No changes to dbt project code:**
- Models stay exactly the same
- Macros unchanged
- dbt_project.yml unchanged
- YAML files unchanged
- Post-hooks unchanged

✅ **No new tables created:**
- Uses existing `DBT_MODEL_LOG`
- Uses existing `DBT_RUN_LOG`

✅ **No new infrastructure:**
- Just 1 stored procedure
- Just 1 monitoring view
- Uses existing Snowflake Tasks

## How It Works (Step-by-Step)

### Scenario 1: Normal Run (No Previous Failures)

```
6:00 AM - Task O2C_DAILY_BUILD triggers
  ↓
Procedure: RUN_DBT_AUTO_RESTART executes
  ↓
Query: SELECT status FROM DBT_RUN_LOG WHERE ... ORDER BY run_started_at DESC LIMIT 1
Result: last_run_status = 'SUCCESS' (or NULL for first run)
  ↓
Decision: Run full build
  ↓
Execute: CALL SYSTEM$DBT_RUN('dbt_o2c_enhanced', 'dbt build')
  ↓
dbt runs all 25 models
  ↓
Post-hooks populate DBT_MODEL_LOG (as always)
  ↓
Done
```

### Scenario 2: Retry After Failure

```
Day 1, 6:00 AM - Task runs
  ↓
Model 15 (dm_o2c_reconciliation) fails
  ↓
Post-hook logs: INSERT INTO DBT_MODEL_LOG (..., status='FAIL', ...)
DBT_RUN_LOG updated: run_status = 'FAILED'
  ↓
Day 2, 6:00 AM - Task runs again
  ↓
Procedure: RUN_DBT_AUTO_RESTART executes
  ↓
Query: SELECT status FROM DBT_RUN_LOG ...
Result: last_run_status = 'FAILED'
  ↓
Query: SELECT LISTAGG(model_name || '+') FROM DBT_MODEL_LOG WHERE status='FAIL'
Result: failed_models_selector = 'dm_o2c_reconciliation+ agg_o2c_by_customer+'
  ↓
Decision: Selective retry
  ↓
Execute: CALL SYSTEM$DBT_RUN('dbt_o2c_enhanced', 
    'dbt run --select dm_o2c_reconciliation+ agg_o2c_by_customer+')
  ↓
dbt runs ONLY:
  - dm_o2c_reconciliation (the failed model)
  - agg_o2c_by_customer (depends on dm_o2c_reconciliation)
  - Any other downstream models
  ↓
Skips models 1-14, 16-25 (already successful)
  ↓
Done - 76% compute time saved!
```

## Retry Logic Details

### When Auto-Retry Happens

✅ Retry occurs when ALL conditions met:
1. Previous run status = `FAILED`
2. Failed models identified in `DBT_MODEL_LOG`
3. Failure is less than 24 hours old
4. Less than 3 retry attempts already made

### What Gets Retried

**Failed models + all downstream dependencies**

The `+` selector is dbt's native syntax:
- `model_a+` = model_a AND all children (downstream models)
- Multiple models: `model_a+ model_b+` = both trees

**Example:**
```
Lineage:
  stg_orders (staging)
    ↓
  dm_o2c_reconciliation (depends on stg_orders)
    ↓
  agg_o2c_by_customer (depends on dm_o2c_reconciliation)

If dm_o2c_reconciliation fails:
  Selector: "dm_o2c_reconciliation+"
  Runs:
    - dm_o2c_reconciliation ✓
    - agg_o2c_by_customer ✓
    - Any other downstream models ✓
  Skips:
    - stg_orders (upstream, already successful)
    - Other unrelated models
```

### Max Retry Limit

**3 attempts maximum**, then automatic reset with full build

**Why?** Prevents infinite retry loops for persistent failures

**Example:**
```
Run 1: dm_o2c_reconciliation fails
Run 2: 🔄 RETRY #1: dm_o2c_reconciliation+
Run 3: 🔄 RETRY #2: dm_o2c_reconciliation+ (still failing)
Run 4: 🔄 RETRY #3: dm_o2c_reconciliation+ (still failing)
Run 5: 🔵 FULL BUILD (max retries reached, reset with fresh build)
```

### Age Limit

**24 hours maximum** (configurable)

**Why?** Don't retry stale failures - better to do fresh build

**Example:**
```
Monday 6 AM: Model fails
Tuesday 6 AM: Auto-retry ✓
Wednesday 6 AM: Auto-retry ✓
Thursday 7 AM: Full build (>24h old, reset)
```

## Benefits

### Compute Efficiency

**Example Scenario:**
- Total models: 25
- Average runtime per model: 10 seconds
- Model 15 fails

**Without Auto-Restart:**
```
Manual full rebuild: 25 models × 10 sec = 250 seconds
```

**With Auto-Restart:**
```
Selective retry: 6 models × 10 sec = 60 seconds
Savings: 190 seconds (76% reduction)
```

### Time Savings

**Without Auto-Restart:**
```
1. Check logs (5 min)
2. Identify failed model (5 min)
3. Identify downstream models (10 min)
4. Write selector command (2 min)
5. Execute manually (1 min)
Total: 23 minutes + compute time
```

**With Auto-Restart:**
```
1. Task runs automatically
Total: 0 minutes human time
```

### Cost Savings

**Compute Credits:**
```
Full rebuild: 250 sec on COMPUTE_WH = ~$0.50
Selective retry: 60 sec on COMPUTE_WH = ~$0.12
Savings per retry: $0.38 (76%)

If 10 retries per month: ~$3.80 saved
If 100 retries per month: ~$38.00 saved
```

## Architecture Benefits

| Aspect | Benefit |
|--------|---------|
| **Stateless-Compatible** | Works despite no persistent target/ directory |
| **Zero Project Changes** | dbt code stays exactly as-is |
| **Lightweight** | 60 lines of SQL, no new infrastructure |
| **Self-Healing** | Automatic detection and retry |
| **Observable** | Full visibility via monitoring view |
| **Fail-Safe** | Falls back to full build on any error |
| **Compute-Efficient** | Only reruns what's needed |
| **Time-Efficient** | Zero human intervention |

## Deployment

### One-Time Setup

```sql
-- Step 1: Run setup script
@/path/to/O2C_ENHANCED_AUTO_RESTART_SETUP.sql

-- That's it! ✅
```

**What it does:**
1. Creates `RUN_DBT_AUTO_RESTART` procedure
2. Creates `V_AUTO_RESTART_HISTORY` view
3. Updates `O2C_DAILY_BUILD` task
4. Updates `O2C_HOURLY_INCREMENTAL` task
5. Grants permissions
6. Resumes tasks

**Duration:** ~30 seconds

### Verification

```sql
-- Test the procedure
CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced');

-- Check monitoring view
SELECT * FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY LIMIT 10;

-- Verify task updated
SHOW TASKS IN SCHEMA EDW.O2C_AUDIT;
```

## Monitoring & Observability

### Daily Monitoring

```sql
-- Today's runs
SELECT * FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY 
WHERE DATE(run_started_at) = CURRENT_DATE()
ORDER BY run_started_at DESC;
```

### Weekly Statistics

```sql
SELECT 
    DATE(run_started_at) AS date,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN run_type = '🔄 SELECTIVE RETRY' THEN 1 ELSE 0 END) AS retries,
    SUM(CASE WHEN run_type = '🔵 FULL BUILD' THEN 1 ELSE 0 END) AS full_builds,
    SUM(CASE WHEN run_status = 'FAILED' THEN 1 ELSE 0 END) AS failures
FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY
WHERE run_started_at > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY DATE(run_started_at)
ORDER BY date DESC;
```

### Compute Savings

```sql
-- Estimate compute time saved by selective retries
SELECT 
    SUM(CASE WHEN run_type = '🔄 SELECTIVE RETRY' 
        THEN (25 - models_run) * 10 END) AS seconds_saved,
    COUNT(CASE WHEN run_type = '🔄 SELECTIVE RETRY' THEN 1 END) AS retry_count
FROM EDW.O2C_AUDIT.V_AUTO_RESTART_HISTORY
WHERE run_started_at > DATEADD('day', -30, CURRENT_TIMESTAMP());
```

## Testing

See `AUTO_RESTART_QUICK_REFERENCE.md` for detailed testing instructions.

**Quick Test:**
```sql
-- Simulate failure
INSERT INTO EDW.O2C_AUDIT.DBT_MODEL_LOG (...) VALUES (..., 'FAIL', ...);
UPDATE EDW.O2C_AUDIT.DBT_RUN_LOG SET run_status = 'FAILED' WHERE ...;

-- Run procedure (should detect and retry)
CALL EDW.O2C_AUDIT.RUN_DBT_AUTO_RESTART('dbt_o2c_enhanced');
-- Expected: "🔄 RETRY #1: ..."

-- Cleanup
DELETE FROM DBT_MODEL_LOG WHERE log_id = 'test-...';
```

## Files Created

1. **`O2C_ENHANCED_AUTO_RESTART_SETUP.sql`** (260 lines)
   - Complete setup script with procedure, view, task updates
   
2. **`AUTO_RESTART_QUICK_REFERENCE.md`** (350 lines)
   - Usage guide, monitoring queries, troubleshooting
   
3. **`IMPLEMENTATION_AUTO_RESTART.md`** (this file)
   - Implementation summary and architecture explanation

## Updates to Existing Files

1. **`REQUIREMENTS_ANALYSIS.md`**
   - Line 124: Updated status to ✅ IMPLEMENTED
   - Line 129: Updated count to 10/11 implemented

2. **`O2C_ENHANCED_SCHEDULING_SETUP.sql`**
   - Added note referencing auto-restart setup

## Summary

**Problem:** Snowflake Native dbt is stateless, cannot auto-restart from failures

**Solution:** Use existing audit tables (DBT_MODEL_LOG) as external state store

**Implementation:**
- ✅ 1 stored procedure (60 lines)
- ✅ 1 monitoring view
- ✅ 2 task updates
- ✅ 0 dbt project changes
- ✅ 0 new tables
- ✅ 0 new infrastructure

**Result:** Automatic, intelligent restart from failure point with minimal overhead

**Status:** ✅ Implemented and ready for production use

