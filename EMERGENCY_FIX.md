# Emergency Fix: Cross-Project Reference Issue

## Quick Diagnostics

**Tell me your answers to these questions:**

### 1. How are you running dbt?
- [ ] Snowflake Native DBT Projects (in Snowflake UI)
- [ ] Local dbt CLI (on your machine)
- [ ] dbt Cloud
- [ ] Other: _______

### 2. Where did you run the failing command?
- Command: _______
- Location: _______

### 3. You mentioned "files are present under compiled models"
- Where did you see this?
- Can you see `stg_ar_invoice` in the compiled output?

---

## Solution A: If Using LOCAL dbt CLI

### Step 1: Check Project Locations
```bash
cd /Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation

# Verify both projects exist
ls -la
# Should show: dbt_foundation/ and dbt_finance_core/

# Check you're in the right directory structure
pwd
```

### Step 2: Build Foundation First (CRITICAL)
```bash
# Build foundation to create manifest.json
cd dbt_foundation
dbt deps
dbt build --target dev

# Verify manifest was created
ls -la target/manifest.json
# Should show the file with recent timestamp

# Check staging model is in manifest
cat target/manifest.json | grep "stg_ar_invoice"
# Should show the model
```

### Step 3: Build Finance Core
```bash
cd ../dbt_finance_core
dbt deps
dbt build --target dev
```

### Step 4: If Still Fails - Try This
```bash
cd dbt_finance_core

# Option A: Point to foundation manifest explicitly
dbt build --state ../dbt_foundation/target

# Option B: Use selector to ensure dependency order
dbt build --select +dm_fin_ar_aging_simple
```

---

## Solution B: If Using Snowflake Native DBT

### Problem: Snowflake Native DBT May Not Support Cross-Project refs Yet

Snowflake's native DBT feature (as of late 2024) may have limitations with cross-project references.

### Workaround 1: Use Views Instead of Cross-Project refs

**In `dbt_finance_core/models/marts/finance/dm_fin_ar_aging_simple.sql`:**

Replace the cross-project refs with direct table references:

```sql
-- OLD (cross-project ref):
with ar_invoice as (
    select * from {{ ref('dbt_foundation', 'stg_ar_invoice') }}
),

-- NEW (direct table reference):
with ar_invoice as (
    select * from {{ source('foundation', 'stg_ar_invoice') }}
    -- OR
    select * from EDW.DBT_STAGING.STG_AR_INVOICE
),
```

### Workaround 2: Add Sources in Finance Core

Create `dbt_finance_core/models/_foundation_sources.yml`:

```yaml
version: 2

sources:
  - name: foundation
    description: "Foundation models as sources"
    database: edw
    schema: dbt_staging
    tables:
      - name: stg_ar_invoice
        identifier: stg_ar_invoice
      
  - name: foundation_shared
    description: "Foundation shared dimensions"
    database: edw
    schema: dbt_shared
    tables:
      - name: dim_customer
      - name: dim_fiscal_calendar
```

Then change refs to sources:
```sql
with ar_invoice as (
    select * from {{ source('foundation', 'stg_ar_invoice') }}
),
customer as (
    select * from {{ source('foundation_shared', 'dim_customer') }}
),
fiscal_cal as (
    select * from {{ source('foundation_shared', 'dim_fiscal_calendar') }}
),
```

---

## Solution C: Hybrid Approach (Recommended)

**Build locally, deploy to Snowflake**

### Step 1: Local Development
```bash
cd /Users/arpanda/Documents/Work/Honeywell/PoC/views/implementation

# Build foundation
cd dbt_foundation
dbt build --profiles-dir ~/.dbt --target dev

# Build finance core
cd ../dbt_finance_core  
dbt build --profiles-dir ~/.dbt --target dev
```

### Step 2: Verify in Snowflake
```sql
-- Check tables were created
SELECT * FROM EDW.DBT_STAGING.STG_AR_INVOICE LIMIT 5;
SELECT * FROM EDW.DBT_SHARED.DIM_CUSTOMER LIMIT 5;
SELECT * FROM EDW.CORP_DM_FIN.DM_FIN_AR_AGING_SIMPLE LIMIT 5;
```

---

## Solution D: Single Project Approach (Nuclear Option)

If cross-project refs continue to fail, merge both projects:

### Create Unified Project
```
dbt_unified/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   └── stg_ar/
│   │       └── stg_ar_invoice.sql
│   ├── marts/
│   │   ├── shared/
│   │   │   ├── dim_customer.sql
│   │   │   └── dim_fiscal_calendar.sql
│   │   └── finance/
│   │       └── dm_fin_ar_aging_simple.sql
│   └── macros/
│       └── aging_bucket.sql
```

Use simple refs without project prefix:
```sql
with ar_invoice as (
    select * from {{ ref('stg_ar_invoice') }}  -- No project prefix
),
```

---

## What I Need From You

**Please tell me:**

1. **How are you running dbt?** (Snowflake UI, local CLI, or Cloud?)

2. **What is the EXACT command that's failing?**
   ```
   Your command: _______
   ```

3. **What's your current working directory?**
   ```bash
   pwd
   ```

4. **Have you built dbt_foundation successfully?**
   ```
   Yes / No
   If yes, when?
   ```

5. **Can you show me the full error message?**
   ```
   [Paste full error here]
   ```

6. **What does this show?**
   ```bash
   ls dbt_foundation/target/manifest.json
   # Does this file exist?
   ```

---

## Quick Debug Commands

Run these and tell me the output:

```bash
# Where are you?
pwd

# What projects exist?
ls -la

# Does foundation manifest exist?
ls -la dbt_foundation/target/manifest.json

# Check dbt version
dbt --version

# Try to compile (not run) finance core
cd dbt_finance_core
dbt compile --select dm_fin_ar_aging_simple
```

---

## Most Likely Issue

Based on the error, the most likely cause is:

**`dbt_foundation` has not been built yet, so its manifest.json doesn't exist or doesn't contain the models.**

### Proof Test:
```bash
cd dbt_foundation
dbt build

# This should succeed and create target/manifest.json
# Then try finance core again
cd ../dbt_finance_core
dbt build
```

If foundation builds successfully but finance_core still fails, then we know it's a different issue (like Snowflake native DBT limitations).

---

## Tell Me What You Get

Please run the commands above and tell me:
1. Which method you're using
2. Output of the debug commands
3. Full error message

Then I can give you the exact fix for your specific situation.

