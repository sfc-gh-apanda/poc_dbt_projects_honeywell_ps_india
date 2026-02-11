# Inside a dbt Build: Production Patterns on Snowflake Native

## **Session: Knowledge Sharing - Snowflake PS India**

**Presenter:** Arpan Dapanda  
**Duration:** ~90 minutes (Demo-heavy)  
**Context:** dbt_o2c_enhanced project (Order-to-Cash Analytics)

---
---

# PART 1: ENGAGEMENT CONTEXT (3 min)

## What Was the Ask?

Client (Fortune 100 manufacturing) has **989 SQL files** executing via a
**config-driven stored procedure framework** on Snowflake. A master stored
procedure reads a configuration table (execution sequence, warehouse,
parameters), then loops through and executes each SQL in order. Control M
orchestrates the overall pipeline.

**They asked us to evaluate dbt as a modernization path** - specifically:
- Can dbt handle their multiple data loading patterns?
- Can it provide testing, lineage, observability?
- How does it compare to their current framework?

**Our deliverable:** A production-ready PoC demonstrating all key features
in a single dbt project (`dbt_o2c_enhanced`) using O2C (Order-to-Cash)
domain data.

**Key takeaway for us:** Everything you see today was built on
**Snowflake Native dbt Projects** (`EXECUTE DBT PROJECT`), which some
of your customers might also be evaluating.

---
---

# PART 2: WHAT IS DBT? (10 min)

## The Core Idea

dbt stands for **data build tool**. It is a **transformation framework**
that sits between your raw data (already loaded into Snowflake) and your
analytics-ready tables.

**What it is:**
- A way to write SELECT statements that dbt turns into tables/views
- A dependency manager (knows what to run in what order)
- A testing framework for data
- A documentation generator

**What it is NOT:**
- An ingestion tool (that's IICS, Fivetran, etc.)
- A scheduler (that's Control M, dbt Cloud, Snowflake Tasks)
- A BI tool (that's PowerBI, Tableau, etc.)

## The Declarative Philosophy

### Old Way (Imperative - Stored Procedures):

```sql
-- You tell Snowflake EXACTLY what to do, step by step:
CREATE OR REPLACE TABLE target_table AS ...;
TRUNCATE TABLE staging_table;
INSERT INTO staging_table SELECT * FROM source;
MERGE INTO target_table USING staging_table ON ...
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...;
-- Manual dependency tracking via config tables
```

### dbt Way (Declarative):

```sql
-- You describe WHAT the result should look like:
-- File: dm_o2c_reconciliation.sql

{{ config(materialized='incremental', incremental_strategy='merge') }}

SELECT
    orders.order_key,
    invoices.invoice_amount,
    payments.payment_amount
FROM {{ ref('stg_enriched_orders') }} orders
LEFT JOIN {{ ref('stg_enriched_invoices') }} invoices ...
```

**You write the SELECT. dbt figures out the rest:**
- Creates the table if it doesn't exist
- Generates the MERGE statement for incremental
- Resolves dependencies (runs staging before marts)
- Adds tests, documentation, lineage

### The ref() Function - The Magic Glue

```sql
-- When you write:
FROM {{ ref('stg_enriched_orders') }}

-- dbt does TWO things:
-- 1. Resolves to: FROM EDW.O2C_ENHANCED_STAGING.STG_ENRICHED_ORDERS
-- 2. Creates a DEPENDENCY: "run stg_enriched_orders BEFORE this model"
```

**This is how dbt builds the DAG (Directed Acyclic Graph) automatically.**
No config tables needed for dependency management.

> **Possible Audience Questions:**
> - "How is this different from just writing views?" →
>   dbt adds testing, documentation, incremental logic, macros, and
>   dependency management on top of plain SQL.
> - "Does it work with Dynamic Tables?" →
>   dbt models can target Dynamic Tables in newer versions, but most
>   patterns use standard tables/views with incremental logic.
> - "Can non-SQL people use it?" →
>   It's SQL-first, but Jinja templating adds logic (loops, conditions).
>   If you know SQL, you can start using dbt immediately.

---
---

# PART 3: PROJECT STRUCTURE & KEY FILES (8 min)

## The Project Layout

```
dbt_o2c_enhanced/
│
├── dbt_project.yml          ← THE CONTROL FILE (project config)
├── packages.yml             ← External package dependencies
├── profiles.yml             ← Snowflake connection details
│
├── macros/                  ← REUSABLE FUNCTIONS (Jinja)
│   ├── audit/
│   │   ├── audit_columns.sql
│   │   └── row_hash.sql
│   ├── logging/
│   │   ├── log_run.sql
│   │   └── log_model.sql
│   ├── warehouse/
│   │   └── get_warehouse.sql
│   └── business/
│       ├── payment_status.sql
│       └── calculate_dso.sql
│
├── models/                  ← YOUR TRANSFORMATIONS
│   ├── sources/
│   │   └── _sources.yml     ← Where raw data lives
│   ├── staging/o2c/
│   │   ├── stg_enriched_orders.sql
│   │   ├── stg_enriched_invoices.sql
│   │   └── stg_enriched_payments.sql
│   └── marts/
│       ├── dimensions/      ← Pattern 1: Truncate & Load
│       ├── core/            ← Pattern 2: Merge (Upsert)
│       ├── events/          ← Pattern 3: Append Only
│       ├── partitioned/     ← Patterns 4 & 5: Delete+Insert
│       └── aggregates/      ← Summary tables
│
└── snapshots/               ← SCD-2 Historical Tracking
    └── snap_customer.sql
```

## Key File 1: profiles.yml (The Connection)

```yaml
snowflake_o2c_enhanced:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: xy12345.us-east-1
      database: EDW
      warehouse: COMPUTE_WH
      schema: O2C_ENHANCED_DEV
      role: DBT_DEV_ROLE
```

**This tells dbt HOW to connect to Snowflake.**
Note: In Snowflake Native dbt, this is configured in the project
definition, not a file.

## Key File 2: packages.yml (External Dependencies)

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
```

**Think of this as pip install for dbt.** Community-maintained packages
that give you pre-built macros and tests. `dbt_utils` gives utilities
like surrogate keys, `dbt_expectations` gives Great Expectations-style
data quality tests.

## Key File 3: dbt_project.yml (The Control File)

This is the most important file. Let me walk through our actual one:

```yaml
name: 'dbt_o2c_enhanced'
version: '2.0.0'

# ═══════════════════════════════════════════════
# HOOKS - Run BEFORE and AFTER the entire build
# ═══════════════════════════════════════════════
on-run-start:
  - "{{ log_run_start() }}"       # Log run to audit table

on-run-end:
  - "{{ log_run_end() }}"         # Update audit with final status

# ═══════════════════════════════════════════════
# VARIABLES
# ═══════════════════════════════════════════════
vars:
  enable_audit_logging: true
  reload_source: 'ALL'
  reload_days: 3

# ═══════════════════════════════════════════════
# MODEL CONFIGURATIONS
# ═══════════════════════════════════════════════
models:
  dbt_o2c_enhanced:
    +post-hook:
      - "{{ log_model_execution() }}"  # Log EVERY model

    staging:
      +materialized: view              # Staging = views
      +schema: staging

    marts:
      dimensions:
        +materialized: table           # Full refresh
      core:
        +materialized: incremental     # Merge pattern
      events:
        +materialized: incremental     # Append pattern
      partitioned:
        +materialized: incremental     # Delete+insert pattern
```

**Key things to notice:**
1. **on-run-start / on-run-end**: These are project-level hooks. They
   fire once at the beginning and end of the ENTIRE dbt run. We use
   them to log audit records.
2. **+post-hook on all models**: Every single model automatically logs
   its execution. You don't need to add anything to individual models.
3. **Different materializations per folder**: Dimensions are tables
   (full refresh), core is incremental (merge), events is incremental
   (append). dbt applies these based on folder structure.

> **Possible Audience Questions:**
> - "What's the difference between on-run-start and pre-hook?" →
>   `on-run-start` fires ONCE before all models. `pre-hook` fires
>   before EACH individual model.
> - "Can you override the folder-level config in individual models?" →
>   Yes, model-level config always takes precedence over folder config.
> - "Where does profiles.yml live in Snowflake Native?" →
>   It doesn't. Connection is configured in the Snowflake project
>   definition object instead.

---
---

# PART 4: THE SEQUENTIAL DEMO - "Inside a dbt Build" (60 min)

## The Setup

**We're about to run ONE command:**

```bash
EXECUTE DBT PROJECT poc_dbt_projects
PROJECT_ROOT = '/O2C/dbt_o2c_enhanced'
ARGS = 'build --target dev';
```

`dbt build` does three things in sequence:
1. **Run** all models (create tables/views)
2. **Test** all tests
3. **Snapshot** all snapshots

**Let's follow what happens, step by step.**

---
---

## ACT 1: THE RUN BEGINS

### Step 1: on-run-start Hook — Logging the Run (5 min)

**What fires first:** The `on-run-start` hook from dbt_project.yml.

**The macro `log_run_start()` does this:**

```sql
-- macros/logging/log_run.sql

{% macro log_run_start() %}
    {% if var('enable_audit_logging', true) %}
        INSERT INTO EDW.O2C_AUDIT.DBT_RUN_LOG (
            run_id,                  -- invocation_id (UUID)
            project_name,            -- 'dbt_o2c_enhanced'
            environment,             -- 'dev' or 'prod'
            run_started_at,          -- timestamp
            run_status,              -- 'RUNNING'
            warehouse_name,          -- CURRENT_WAREHOUSE()
            user_name,               -- CURRENT_USER()
            role_name                -- CURRENT_ROLE()
        )
        SELECT
            '{{ invocation_id }}',
            '{{ project_name }}',
            '{{ target.name }}',
            '{{ run_started_at }}'::TIMESTAMP_NTZ,
            'RUNNING',
            CURRENT_WAREHOUSE(),
            CURRENT_USER(),
            CURRENT_ROLE()
        WHERE NOT EXISTS (
            SELECT 1 FROM EDW.O2C_AUDIT.DBT_RUN_LOG
            WHERE run_id = '{{ invocation_id }}'
        );
    {% endset %}
{% endmacro %}
```

**Key concept - `invocation_id`:**
Every dbt run gets a unique UUID (`invocation_id`). This is the
GOLDEN THREAD that ties together:
- The run log entry
- Every model log entry
- Every audit column in every row of every table

You can trace any row in any table back to the exact dbt run that
created it.

**Key concept - `{{ }}` (Jinja):**
Everything in double curly braces is Jinja templating. It gets
**resolved at compile time** before the SQL is sent to Snowflake.
When Snowflake receives the SQL, it's plain SQL with the UUID string
hardcoded in.

**Demo point:**
```sql
-- BEFORE dbt run:
SELECT * FROM EDW.O2C_AUDIT.DBT_RUN_LOG ORDER BY run_started_at DESC LIMIT 5;

-- RUN dbt build
-- AFTER:
SELECT * FROM EDW.O2C_AUDIT.DBT_RUN_LOG ORDER BY run_started_at DESC LIMIT 5;
-- New row with status = 'RUNNING'
```

> **Possible Audience Questions:**
> - "What if the audit table doesn't exist?" →
>   You run a setup SQL script first that creates the audit schema
>   and tables. It's a one-time setup.
> - "What's the overhead of this logging?" →
>   Minimal. One INSERT per run start, one UPDATE per run end.
>   Each model also gets one INSERT (post-hook).
> - "Can you disable it?" →
>   Yes: `vars: enable_audit_logging: false`

---

### Step 2: Dynamic Warehouse Selection (10 min)

**What happens next:** dbt starts processing models. Each model with
`pre_hook="{{ switch_warehouse() }}"` or
`snowflake_warehouse=get_warehouse()` triggers dynamic warehouse logic.

#### The Problem We Solved

Client's requirement: "Each SQL can have warehouse assignment for better
execution control. This can be changed on the fly in production if a job
fails due to memory/compute issues."

#### Approach 1: snowflake_warehouse config (The Ideal Way)

```sql
-- In the model config:
{{ config(
    snowflake_warehouse=get_warehouse(),   -- Resolved at compile time
    materialized='view'
) }}
```

**The macro `get_warehouse()` queries a config table:**

```sql
{% macro get_warehouse() %}
    {% set query %}
        SELECT warehouse_name
        FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
        WHERE is_active = TRUE
          AND scope_name IN (
              '{{ this.name }}',     -- MODEL level
              '{{ layer_name }}',    -- LAYER level
              '{{ project_name }}',  -- PROJECT level
              'DEFAULT'              -- Global fallback
          )
        ORDER BY priority ASC
        LIMIT 1
    {% endset %}

    {% set results = run_query(query) %}
    -- ... return warehouse name or fallback ...
{% endmacro %}
```

**The `execute` flag problem:**

dbt has TWO phases:
1. **Parse/Compile phase**: Reads all SQL, resolves Jinja, builds the DAG
2. **Execute phase**: Actually runs the SQL against Snowflake

During the **parse phase**, the variable `execute` is `False`. This means
`run_query()` does NOT actually execute. It returns None.

```python
# Internally in dbt:
if execute:
    results = run_query(sql)    # Only during execute phase
else:
    results = None              # During parse/compile phase
```

**Why this matters for `get_warehouse()`:**

When dbt compiles the config block `snowflake_warehouse=get_warehouse()`,
it's in the **parse phase**. The `run_query()` inside `get_warehouse()`
returns None because `execute` is False. So the warehouse lookup FAILS
during compilation.

We logged this to verify:

```
=== GET_WAREHOUSE DEBUG ===
  execute flag: False        ← THIS IS THE PROBLEM
  >>> RESULTS IS NONE/EMPTY
  >>> Returning fallback
```

The `snowflake_warehouse` config requires a VALUE at compile time, but
the config table query can't run until execute time.

#### Approach 2: switch_warehouse() via Stored Procedure (The Working Way)

Since compile-time lookup doesn't work, we moved the logic to a
**pre-hook** instead. Pre-hooks execute at **runtime**, not compile time.

**Step 1:** We created a **stored procedure** in Snowflake:

```sql
-- EDW.CONFIG.SET_DYNAMIC_WAREHOUSE(model, layer, project, env)

CREATE OR REPLACE PROCEDURE EDW.CONFIG.SET_DYNAMIC_WAREHOUSE(
    p_model_name VARCHAR,
    p_layer_name VARCHAR,
    p_project_name VARCHAR,
    p_environment VARCHAR
)
RETURNS VARCHAR
AS
$$
DECLARE
    v_warehouse VARCHAR;
    v_sql VARCHAR;
BEGIN
    -- Query config table for best match
    SELECT warehouse_name INTO v_warehouse
    FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
    WHERE is_active = TRUE
      AND scope_name IN (
          :p_model_name,
          :p_layer_name,
          :p_project_name,
          'DEFAULT'
      )
    ORDER BY priority ASC
    LIMIT 1;

    -- Build USE WAREHOUSE command
    v_sql := 'USE WAREHOUSE ' || COALESCE(v_warehouse, 'COMPUTE_WH');

    -- Execute it
    EXECUTE IMMEDIATE v_sql;

    RETURN v_warehouse;
END;
$$;
```

**Step 2:** The dbt macro calls this stored procedure as a pre-hook:

```sql
{% macro switch_warehouse() %}
    {% set model_name = this.name if this else 'UNKNOWN' %}
    {% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else '' %}

    CALL EDW.CONFIG.SET_DYNAMIC_WAREHOUSE(
        '{{ model_name }}',
        '{{ layer_name }}',
        '{{ project_name }}',
        '{{ target.name }}'
    )
{% endmacro %}
```

**Step 3:** Used as pre-hook in models:

```sql
{{ config(
    materialized='incremental',
    pre_hook="{{ switch_warehouse() }}"    -- Runs at RUNTIME ✅
) }}
```

**The config table (hierarchical priority):**

```
┌──────────────┬─────────────────────────┬────────────────────┬──────────┐
│ config_scope │ scope_name              │ warehouse_name     │ priority │
├──────────────┼─────────────────────────┼────────────────────┼──────────┤
│ MODEL        │ dm_o2c_reconciliation   │ COMPUTE_WH_LARGE   │ 10       │
│ LAYER        │ staging                 │ COMPUTE_WH_SMALL   │ 30       │
│ LAYER        │ marts                   │ COMPUTE_WH_MEDIUM  │ 30       │
│ PROJECT      │ dbt_o2c_enhanced        │ COMPUTE_WH         │ 40       │
│ DEFAULT      │ DEFAULT                 │ COMPUTE_WH_SMALL   │ 100      │
└──────────────┴─────────────────────────┴────────────────────┴──────────┘

Lower priority number = higher precedence
MODEL (10) beats LAYER (30) beats PROJECT (40) beats DEFAULT (100)
```

**Production workflow:**

```sql
-- Model times out at 2 AM on COMPUTE_WH_MEDIUM
-- DBA at 2:05 AM (no code change, no deployment):
UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
SET warehouse_name = 'COMPUTE_WH_XLARGE'
WHERE config_scope = 'MODEL'
  AND scope_name = 'dm_o2c_reconciliation';

-- 4 AM re-run: Uses XLARGE, succeeds ✅
-- Change logged in history table for audit ✅
```

**Demo point:**
```sql
SELECT * FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG ORDER BY priority;
-- Show the hierarchy
-- Update a warehouse
-- Run dbt, show query history to verify it used the new warehouse
```

> **Possible Audience Questions:**
> - "Why not just use `snowflake_warehouse` config directly?" →
>   That requires a compile-time value. The config table query can't
>   run during compile because `execute` is False.
> - "What if the stored procedure fails?" →
>   The stored procedure has a COALESCE fallback to COMPUTE_WH.
>   The model still runs, just on the default warehouse.
> - "Can you use this pattern for other runtime configs?" →
>   Yes! Same pattern works for any runtime configuration:
>   target schemas, query tags, timeout settings, etc.
> - "What about the stg model using get_warehouse() directly?" →
>   We tried both approaches. `snowflake_warehouse=get_warehouse()`
>   in the staging model config was an experiment. It works in some
>   dbt versions where `execute` is True during config resolution.
>   The stored procedure approach (`switch_warehouse()`) is the
>   reliable backup that works everywhere.

---
---

## ACT 2: READING THE DATA

### Step 3: Sources — Where Raw Data Lives (5 min)

**Before dbt can transform data, it needs to know where it comes from.**

```yaml
# models/sources/_sources.yml

sources:
  - name: corp_tran
    database: EDW
    schema: CORP_TRAN
    tables:
      - name: FACT_SALES_ORDERS
        description: "Sales order headers and lines"
      - name: FACT_INVOICES
      - name: FACT_PAYMENTS

  - name: corp_master
    database: EDW
    schema: CORP_MASTER
    tables:
      - name: DIM_CUSTOMER
      - name: DIM_PAYMENT_TERMS
      - name: DIM_BANK_ACCOUNT
```

**How sources are used in SQL:**

```sql
-- This Jinja:
FROM {{ source('corp_tran', 'FACT_SALES_ORDERS') }}

-- Compiles to this SQL:
FROM EDW.CORP_TRAN.FACT_SALES_ORDERS
```

**Why use source() instead of hardcoding?**
1. dbt tracks the lineage (knows your model reads from this table)
2. If database/schema changes, update ONE YAML file (not 50 SQL files)
3. Can add freshness checks, tests, documentation at the source level

**Demo point:** Show _sources.yml, then show compiled SQL to see the
resolution.

> **Possible Audience Questions:**
> - "Can you test source tables?" →
>   Yes! You can add not_null, unique, accepted_values tests
>   directly on source columns in the YAML.
> - "What about source freshness?" →
>   dbt can check if a source was updated recently via
>   `loaded_at_field` and `freshness` config.

---

### Step 4: Staging — Source-Specific Transformations (8 min)

**Staging models clean, standardize, and enrich raw data.**

**Our actual staging model (`stg_enriched_orders.sql`):**

```sql
{{ config(materialized='view', tags=['staging', 'orders']) }}

SELECT
    -- Keys
    orders.source_system,
    orders.company_code,
    orders.order_id,
    orders.order_line,
    orders.source_system || '|' || orders.order_id || '|' || orders.order_line
        AS order_key,

    -- Order details
    orders.order_number,
    orders.order_date,
    orders.order_quantity,
    orders.order_amount_lcl AS order_amount,    -- Rename for clarity
    orders.currency_code AS order_currency,
    orders.order_status,

    -- Customer enrichment (JOIN with dimension)
    orders.customer_id,
    cust.customer_name,
    cust.customer_type,
    cust.customer_country,

    -- Organizational
    orders.sales_org,
    orders.profit_center,

    -- Audit columns (uniform set)
    {{ audit_columns() }}

FROM {{ source('corp_tran', 'FACT_SALES_ORDERS') }} orders

LEFT JOIN {{ source('corp_master', 'DIM_CUSTOMER') }} cust
    ON orders.customer_id = cust.customer_num_sk
    AND orders.source_system = cust.source_system

WHERE orders.order_date >= DATEADD('year', -2, CURRENT_DATE())
```

**Key things to notice:**
1. **Materialized as VIEW**: Staging models are views. No data stored.
   Always current when queried.
2. **Enrichment via JOIN**: Customer dimension is joined here in staging.
   This means ALL downstream models get customer info for free.
3. **Source-specific join**: `AND orders.source_system = cust.source_system`
   — the client has multiple ERP systems (BRP900, CIP900, Microsiga).
   Customer IDs are only unique within a source system.
4. **Composite key**: `source_system || '|' || order_id || '|' || order_line`
   creates a globally unique key across all source systems.
5. **audit_columns()**: Even views get audit columns. More on this next.

**Demo point:** Show the SQL, then show the compiled SQL, then query the
view in Snowflake.

> **Possible Audience Questions:**
> - "Why materialize staging as views and not tables?" →
>   Views are always current. No need to rebuild. Zero storage cost.
>   If staging is slow, you can switch to `ephemeral` (CTE) or `table`.
> - "Why join in staging? Isn't that a mart concern?" →
>   Customer enrichment is needed by ALL downstream models. Joining
>   once in staging avoids repeating the same join in 10 marts.
> - "What about source-specific business rules?" →
>   Add CASE WHEN source_system = 'BRP900' THEN ... for any
>   source-specific logic (currency conversion, date formats, etc.)

---
---

## ACT 3: TRANSFORMING THE DATA

### Step 5: Data Loading Patterns (15 min)

**This is the core of the demo. We implemented 5 loading patterns,
each in a separate model.**

#### Pattern 1: Truncate & Load (dim_o2c_customer)

```sql
{{ config(materialized='table') }}
```

**How it works:** `materialized='table'` means dbt runs
`CREATE OR REPLACE TABLE ... AS SELECT ...` every time. Full refresh.
The old table is dropped and recreated.

**Use case:** Dimensions, reference data, small lookup tables.
Complete refresh is acceptable.

**Compiled SQL:**
```sql
CREATE OR REPLACE TABLE EDW.O2C_ENHANCED_DIMENSIONS.DIM_O2C_CUSTOMER AS (
    SELECT ... FROM ...
);
```

---

#### Pattern 2: Incremental Merge / Upsert (dm_o2c_reconciliation)

```sql
{{ config(
    materialized='incremental',
    unique_key='reconciliation_key',
    incremental_strategy='merge',
    on_schema_change='fail',
    merge_update_columns=[
        'invoice_key', 'payment_key', 'invoice_amount',
        'payment_amount', 'reconciliation_status',
        'dbt_updated_at', 'dbt_run_id', 'dbt_row_hash'
    ]
) }}

SELECT
    ... business columns ...,
    {{ row_hash(['inv.invoice_key', 'pay.payment_key', ...]) }},
    {{ audit_columns_incremental('existing') }}

FROM {{ ref('stg_enriched_orders') }} orders
LEFT JOIN {{ ref('stg_enriched_invoices') }} inv ...
LEFT JOIN {{ ref('stg_enriched_payments') }} pay ...

{% if is_incremental() %}
LEFT JOIN {{ this }} existing
    ON ... = existing.reconciliation_key
{% endif %}
```

**How it works:**
- **First run**: `is_incremental()` is False → full load (CREATE TABLE AS)
- **Subsequent runs**: `is_incremental()` is True → MERGE statement
  - New records: INSERT
  - Existing records: UPDATE (only columns in `merge_update_columns`)
- **`{{ this }}`** refers to the target table itself

**Key detail — `merge_update_columns`:**
Notice `dbt_updated_at` is in the update list but `dbt_created_at` is NOT.
This means on update, the created timestamp is PRESERVED, but the updated
timestamp changes. This is the **watermark behavior**.

**Compiled SQL (second run):**
```sql
MERGE INTO EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION AS DBT_INTERNAL_DEST
USING (...subquery...) AS DBT_INTERNAL_SOURCE
ON DBT_INTERNAL_DEST.reconciliation_key = DBT_INTERNAL_SOURCE.reconciliation_key
WHEN MATCHED THEN UPDATE SET
    invoice_key = DBT_INTERNAL_SOURCE.invoice_key,
    dbt_updated_at = DBT_INTERNAL_SOURCE.dbt_updated_at,
    ...
WHEN NOT MATCHED THEN INSERT (...)
VALUES (...);
```

---

#### Pattern 3: Append Only (fact_o2c_events)

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

SELECT ... FROM ...

{% if is_incremental() %}
WHERE event_timestamp > (
    SELECT COALESCE(MAX(event_timestamp), '1900-01-01'::DATE)
    FROM {{ this }}
)
{% endif %}
```

**How it works:** Only INSERT, never UPDATE. Old records are immutable.
The `WHERE` clause uses the MAX timestamp from the target as a
**high-water mark** to only fetch new events.

**Use case:** Event logs, audit trails, immutable fact streams.

---

#### Pattern 4: Delete + Insert by Partition (fact_o2c_daily)

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['source_system', 'order_date', 'order_key'],
    incremental_predicates=[
        "DBT_INTERNAL_DEST.order_date >= DATEADD('day', -"
        ~ var('reload_days', 3) ~ ", CURRENT_DATE())"
    ]
) }}

SELECT ... FROM ...
WHERE order_date >= DATEADD('day', -{{ var('reload_days', 3) }}, CURRENT_DATE())
```

**How it works:**
1. DELETE all records in target matching the predicate (last 3 days)
2. INSERT fresh data for those 3 days
3. Records older than 3 days are UNTOUCHED

**Use case:** Time-partitioned data where late-arriving records need
correction. Configurable window via `--vars '{"reload_days": 7}'`.

**Compiled SQL (second run):**
```sql
DELETE FROM target WHERE order_date >= DATEADD('day', -3, CURRENT_DATE());
INSERT INTO target SELECT ... WHERE order_date >= DATEADD('day', -3, CURRENT_DATE());
```

---

#### Pattern 5: Source-Specific Reload (fact_o2c_by_source)

```sql
{% set reload_src = var('reload_source', 'ALL') %}

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['order_key', 'source_system']
) }}

SELECT ... FROM {{ ref('stg_enriched_orders') }}

{% if var('reload_source', 'ALL') != 'ALL' %}
WHERE source_system = '{{ var("reload_source") }}'
{% endif %}
```

**How it works:**
```bash
# Reload everything:
dbt run --select fact_o2c_by_source

# Reload ONLY BRP900 (other sources untouched):
dbt run --select fact_o2c_by_source --vars '{"reload_source": "BRP900"}'
```

**Use case:** Multiple ERP source systems feeding one table.
Fix data from one source without affecting others.

---

**Demo point:** Show all 5 models side by side. Run `dbt build`.
Query Snowflake to show the different tables. Run again to show
incremental behavior.

> **Possible Audience Questions:**
> - "Which pattern should I use for my customer?" →
>   Truncate for small dims, Merge for facts with updates,
>   Append for events, Delete+Insert for time-partitioned data,
>   Source reload for multi-ERP scenarios.
> - "What happens if incremental fails midway?" →
>   dbt uses transactions. If the MERGE fails, it rolls back.
>   Re-run and it picks up from where it left off.
> - "Can you mix patterns in one project?" →
>   Yes! That's exactly what we do. Each folder has a different
>   default, and each model can override.
> - "What's on_schema_change?" →
>   Controls what happens if your SELECT returns different columns
>   than the target table. Options: 'fail', 'append_new_columns',
>   'ignore', 'sync_all_columns'.

---

### Step 6: Audit Columns & Watermarking (8 min)

**Every model gets uniform audit columns via macros.**

#### For Tables & Views (Non-Incremental):

```sql
{% macro audit_columns() %}
    '{{ invocation_id }}'::VARCHAR(50)          AS dbt_run_id,
    MD5('{{ invocation_id }}' || '{{ this.name }}')
                                                AS dbt_batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ          AS dbt_loaded_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ          AS dbt_created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ          AS dbt_updated_at,
    '{{ this.name }}'::VARCHAR(100)             AS dbt_source_model,
    '{{ target.name }}'::VARCHAR(20)            AS dbt_environment
{% endmacro %}
```

For full-refresh: `dbt_created_at = dbt_updated_at` (both are NOW)
because the entire table is recreated.

#### For Incremental Models (Watermark Behavior):

```sql
{% macro audit_columns_incremental(existing_alias='existing') %}
    '{{ invocation_id }}'::VARCHAR(50) AS dbt_run_id,
    ...
    {% if is_incremental() %}
    COALESCE({{ existing_alias }}.dbt_created_at,
             CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS dbt_created_at,
    {% else %}
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_created_at,
    {% endif %}

    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_updated_at,
    ...
{% endmacro %}
```

**The Watermark Behavior:**

```
FIRST INSERT (Jan 1):
  dbt_created_at = 2026-01-01 08:00:00
  dbt_updated_at = 2026-01-01 08:00:00

SUBSEQUENT MERGE (Feb 11):
  dbt_created_at = 2026-01-01 08:00:00  ← PRESERVED (original insert)
  dbt_updated_at = 2026-02-11 14:30:00  ← UPDATED (current run)
```

**How it works technically:**
The incremental model LEFT JOINs to `{{ this }}` (itself):

```sql
LEFT JOIN {{ this }} existing
    ON source.key = existing.reconciliation_key
```

For EXISTING records: `existing.dbt_created_at` has the original
timestamp → COALESCE returns that original value.

For NEW records: `existing.dbt_created_at` is NULL (no match) →
COALESCE falls through to CURRENT_TIMESTAMP().

**Why this matters:**
- "When was this record FIRST created?" → `dbt_created_at`
- "When was this record LAST updated?" → `dbt_updated_at`
- "Which dbt run touched this record?" → `dbt_run_id`
- "Which model created this record?" → `dbt_source_model`
- "Is this dev or prod data?" → `dbt_environment`

#### Row Hash for Change Detection:

```sql
{% macro row_hash(columns, alias='dbt_row_hash') %}
    MD5(
        CONCAT_WS('||',
            {% for col in columns %}
            COALESCE(CAST({{ col }} AS VARCHAR), '__NULL__')
            {%- if not loop.last -%},{%- endif %}
            {% endfor %}
        )
    )::VARCHAR(32) AS {{ alias }}
{% endmacro %}
```

**Usage:** `{{ row_hash(['invoice_key', 'payment_amount']) }}`

This creates an MD5 fingerprint of the business columns. If ANY value
changes, the hash changes. Useful for:
- Downstream models can check: "Did this row actually change?"
- Avoids unnecessary updates in MERGE (compare hash, not all columns)

**Demo point:**
```sql
SELECT
    reconciliation_key,
    order_amount,
    dbt_run_id,
    dbt_created_at,
    dbt_updated_at,
    dbt_row_hash
FROM dm_o2c_reconciliation
ORDER BY dbt_updated_at DESC
LIMIT 10;
```

> **Possible Audience Questions:**
> - "Does the LEFT JOIN to self slow things down?" →
>   Slightly, but Snowflake handles self-joins efficiently.
>   The alternative is losing the created timestamp entirely.
> - "Why not use Snowflake's METADATA$ACTION?" →
>   That's for streams. dbt doesn't use streams by default.
>   Our approach works with any materialization strategy.
> - "Can you use this for SCD-2?" →
>   For full SCD-2 with historical versions, use dbt snapshots
>   (we have snap_customer.sql). Audit columns give you
>   lightweight tracking without full versioning.

---
---

## ACT 4: VALIDATING THE DATA

### Step 7: Schema Contracts (5 min)

```yaml
# models/marts/core/_core.yml

models:
  - name: dm_o2c_reconciliation
    config:
      contract:
        enforced: true
    columns:
      - name: reconciliation_key
        data_type: varchar
      - name: order_amount
        data_type: number(18,2)
      - name: dbt_created_at
        data_type: timestamp_ntz
```

**What this does:**
- At **compile time**, dbt checks that your model's SELECT produces
  columns matching the contract (name AND data type)
- If there's a mismatch → **compilation fails, deployment blocked**

**Combined with `on_schema_change: 'fail'`:**
- If you add/remove a column in your SELECT → the model FAILS
- This prevents the classic "wrong data in wrong column" bug

**Demo point:**
```bash
# Intentionally rename a column in the model SQL
# Run dbt compile → Shows contract violation error
# Fix it → compile succeeds
```

> **Possible Audience Questions:**
> - "Is this like a Snowflake table constraint?" →
>   No, it's a compile-time check in dbt. The Snowflake table
>   has no constraints. But dbt won't let you deploy if the
>   columns don't match.
> - "When was this feature added?" →
>   dbt 1.5+ (model contracts). Available in Snowflake Native.

---

### Step 8: Test Cases — Data Quality Gates (10 min)

**Tests run automatically during `dbt build` (after each model).**

#### Built-in Tests:

```yaml
columns:
  - name: reconciliation_key
    tests:
      - unique
      - not_null

  - name: reconciliation_status
    tests:
      - accepted_values:
          values: ['NOT_INVOICED', 'NOT_PAID', 'OPEN', 'CLOSED']
```

**What dbt does with these:** It generates a SELECT query that
returns rows that FAIL the test. If any rows return, the test fails.

```sql
-- Behind the scenes for "unique" test:
SELECT reconciliation_key
FROM dm_o2c_reconciliation
GROUP BY reconciliation_key
HAVING COUNT(*) > 1;
-- If this returns rows → TEST FAILS
```

#### dbt_expectations Tests (Statistical):

```yaml
- name: order_amount
  tests:
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: 0
        max_value: 10000000
        config:
          severity: error

- name: dm_o2c_reconciliation
  tests:
    - dbt_expectations.expect_table_row_count_to_be_between:
        min_value: 100
        max_value: 10000000
        config:
          severity: warn
```

#### Severity Levels:

```yaml
severity: error   # Test fails → dbt build STOPS
severity: warn    # Test fails → Warning logged, build CONTINUES
```

This is powerful: you can have strict tests (unique key violations =
stop everything) alongside advisory tests (row count anomaly = warn
but continue).

**Demo point:**
```bash
# Run dbt build, watch tests execute
# Show: "X tests passed, Y tests warned, Z tests failed"
# Query: DBT_ARTIFACTS.TEST_EXECUTIONS for historical results
```

> **Possible Audience Questions:**
> - "How many tests is too many?" →
>   Rule of thumb: test primary keys (unique, not_null) on every
>   model. Add business-rule tests on critical models. 44 tests
>   across 10 models is reasonable.
> - "Can tests be slow?" →
>   Yes, table-scan tests on large tables can be slow. Use
>   `--exclude tag:slow_test` for quick builds. Run full
>   test suite nightly.
> - "Can you store failed rows?" →
>   Yes: `dbt test --store-failures` stores failing rows in a
>   separate schema for investigation.

---
---

## ACT 5: THE RUN COMPLETES

### Step 9: Post-Hook — Model Logging (5 min)

**After EACH model succeeds, the post-hook fires:**

```yaml
# dbt_project.yml
models:
  dbt_o2c_enhanced:
    +post-hook:
      - "{{ log_model_execution() }}"
```

**The macro:**

```sql
{% macro log_model_execution() %}
    {% if var('enable_audit_logging', true) and execute %}

        INSERT INTO EDW.O2C_AUDIT.DBT_MODEL_LOG (
            log_id,
            run_id,
            model_name,
            materialization,
            status,
            started_at,
            ended_at,
            is_incremental,
            incremental_strategy
        )
        SELECT
            '{{ invocation_id }}_{{ this.name }}',
            '{{ invocation_id }}',
            '{{ this.name }}',
            '{{ config.get("materialized", "view") }}',
            'SUCCESS',
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            {{ 'TRUE' if config.get("materialized") == 'incremental'
               else 'FALSE' }},
            '{{ config.get("incremental_strategy", "default") }}'
        WHERE NOT EXISTS (...);

    {% endif %}
{% endmacro %}
```

**Notice `and execute`:** This check ensures the INSERT only runs
during the execute phase, not during compile. Without this, dbt would
try to run the INSERT during parsing (which would fail or create
duplicate entries).

**Enhanced version with row count:**

```sql
rows_affected = (SELECT COUNT(*) FROM {{ this }})
```

**Demo point:**
```sql
SELECT
    model_name,
    materialization,
    incremental_strategy,
    status,
    rows_affected,
    started_at,
    ended_at
FROM EDW.O2C_AUDIT.DBT_MODEL_LOG
WHERE run_id = '<latest_invocation_id>'
ORDER BY started_at;
```

> **Possible Audience Questions:**
> - "What if the post-hook fails?" →
>   The model itself already succeeded. Post-hook failure is logged
>   but doesn't roll back the model. The data is safe.
> - "Can you log row counts for views?" →
>   Views don't have a persistent row count. You'd need to SELECT
>   COUNT(*) from the view, which runs the full query.

---

### Step 10: on-run-end Hook — Finalizing (3 min)

**After ALL models are done, the on-run-end hook fires:**

```sql
{% macro log_run_end() %}
    UPDATE EDW.O2C_AUDIT.DBT_RUN_LOG
    SET
        run_ended_at = CURRENT_TIMESTAMP(),
        run_duration_seconds = DATEDIFF('second', run_started_at,
                                         CURRENT_TIMESTAMP()),
        run_status = CASE
            WHEN (SELECT COUNT(*) FROM DBT_MODEL_LOG
                  WHERE run_id = '{{ invocation_id }}'
                    AND status = 'FAIL') > 0
            THEN 'FAILED'
            ELSE 'SUCCESS'
        END,
        models_run = (SELECT COUNT(*) ...),
        models_success = (SELECT COUNT(*) ... AND status = 'SUCCESS'),
        models_failed = (SELECT COUNT(*) ... AND status IN ('FAIL','ERROR'))
    WHERE run_id = '{{ invocation_id }}';
{% endmacro %}
```

**What this gives you:**

```sql
SELECT
    run_id,
    run_status,
    run_duration_seconds,
    models_run,
    models_success,
    models_failed
FROM EDW.O2C_AUDIT.DBT_RUN_LOG
ORDER BY run_started_at DESC LIMIT 5;

-- Output:
-- run_id     | status  | duration | models | success | failed
-- abc-123... | SUCCESS | 45       | 10     | 10      | 0
-- def-456... | FAILED  | 32       | 10     | 8       | 2
```

**Also in on-run-end:**
```yaml
on-run-end:
  - "{{ log_run_end() }}"
  - "CALL EDW.O2C_AUDIT.ARCHIVE_DBT_LOG('LATEST')"
```

The second hook archives dbt's internal log to a Snowflake table
for persistent storage.

---

### Step 11: Generated Documentation (5 min)

**After `dbt build` or `dbt docs generate`:**

```bash
dbt docs generate
dbt docs serve    # Opens interactive web UI
```

**What you get:**
- Interactive DAG (dependency graph) of all models
- Click on any model → see description, columns, tests, SQL
- Column-level lineage (which source column flows where)
- Search across all models

**The catalog:**
dbt generates a `catalog.json` that contains table metadata
(column names, types, row counts) by querying Snowflake's
INFORMATION_SCHEMA.

**Demo point:** Open dbt docs, navigate the DAG, click through
models.

> **Possible Audience Questions:**
> - "Can you host dbt docs permanently?" →
>   Yes. Upload to S3/Azure Blob/Snowflake stage. dbt Cloud
>   hosts them automatically.
> - "Is this per-project only?" →
>   Yes, in Snowflake Native. Cross-project lineage requires
>   dbt Cloud with Mesh.

---
---

# PART 5: BONUS FEATURES (8 min)

## SCD-2 Snapshots

```sql
-- snapshots/snap_customer.sql
{% snapshot snap_customer %}
{{
    config(
        target_schema='O2C_ENHANCED_SNAPSHOTS',
        unique_key='customer_num_sk',
        strategy='check',
        check_cols=[
            'customer_name', 'customer_type',
            'customer_country', 'customer_classification'
        ],
        invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ source('corp_master', 'DIM_CUSTOMER') }}
WHERE customer_num_sk IS NOT NULL
{% endsnapshot %}
```

**dbt automatically creates:**
- `dbt_scd_id` — surrogate key for the version
- `dbt_valid_from` — when this version started
- `dbt_valid_to` — when this version ended (NULL = current)
- `dbt_updated_at` — last change timestamp

**Run:** `dbt snapshot` (or included in `dbt build`)

## Business Macros

```sql
-- macros/business/calculate_dso.sql
{% macro calculate_dso(order_date, payment_date) %}
    DATEDIFF('day', {{ order_date }}, {{ payment_date }})
{% endmacro %}

-- Usage in any model:
{{ calculate_dso('orders.order_date', 'payments.payment_date') }} AS dso
```

**Philosophy:** Define business logic ONCE, use it everywhere.
If the DSO calculation changes, update ONE macro, not 50 models.

---
---

# SUMMARY: WHAT WE SAW IN ONE dbt BUILD

```
┌─────────────────────────────────────────────────────────────────┐
│                        dbt build                                 │
│                                                                  │
│  1. on-run-start    → Log run to audit table                    │
│  2. pre-hook        → Dynamic warehouse selection               │
│  3. Sources         → Read from Snowflake raw tables            │
│  4. Staging         → Clean, standardize, enrich (views)        │
│  5. Transformations → 5 loading patterns (table/incremental)    │
│  6. Audit Columns   → Uniform metadata on every row             │
│  7. Row Hash        → Change detection fingerprint              │
│  8. Schema Contract → Column validation at compile time         │
│  9. Tests           → Data quality gates (pass/warn/fail)       │
│ 10. post-hook       → Log each model execution                  │
│ 11. on-run-end      → Finalize run status, archive logs         │
│ 12. Docs/Catalog    → Auto-generated documentation              │
│                                                                  │
│  All from ONE command. All automated. All audited.              │
└─────────────────────────────────────────────────────────────────┘
```

## Key Takeaways

1. **Declarative > Imperative**: Write SELECT, dbt handles the rest
2. **Hooks provide lifecycle control**: on-run-start → pre-hook →
   model → post-hook → on-run-end
3. **Macros = Code reuse**: audit_columns(), switch_warehouse(),
   row_hash() — define once, use everywhere
4. **`execute` flag matters**: Compile time vs runtime is critical
   for macros that query Snowflake
5. **5 patterns cover 95% of use cases**: Know which to recommend
6. **Testing is built-in**: Not an afterthought
7. **Observability requires effort**: On Snowflake Native, you build
   the audit infrastructure yourself (hooks, stored procs, config tables)

---

## Demo Queries (Run These After dbt build)

```sql
-- 1. Run audit log
SELECT * FROM EDW.O2C_AUDIT.DBT_RUN_LOG ORDER BY run_started_at DESC LIMIT 5;

-- 2. Model execution details
SELECT model_name, materialization, status, rows_affected
FROM EDW.O2C_AUDIT.DBT_MODEL_LOG
WHERE run_id = (SELECT MAX(run_id) FROM EDW.O2C_AUDIT.DBT_RUN_LOG)
ORDER BY started_at;

-- 3. Audit columns in action
SELECT reconciliation_key, order_amount, dbt_run_id,
       dbt_created_at, dbt_updated_at, dbt_row_hash, dbt_environment
FROM EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION LIMIT 10;

-- 4. Warehouse config
SELECT * FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG ORDER BY priority;

-- 5. Event table (append-only pattern)
SELECT event_type, COUNT(*) AS cnt, MIN(dbt_loaded_at), MAX(dbt_loaded_at)
FROM EDW.O2C_ENHANCED_EVENTS.FACT_O2C_EVENTS
GROUP BY event_type;

-- 6. Daily fact (partition reload pattern)
SELECT order_date, COUNT(*), MIN(dbt_loaded_at) AS first_loaded
FROM EDW.O2C_ENHANCED_PARTITIONED.FACT_O2C_DAILY
GROUP BY order_date ORDER BY order_date DESC LIMIT 10;
```

---
---

# Q&A PREPARATION

## Expected Questions & Answers

**Q: "How does Snowflake Native dbt differ from dbt Core CLI?"**
A: Same dbt engine, different execution environment. Snowflake Native
runs inside Snowflake (EXECUTE DBT PROJECT). No external server needed.
Limitations: No dbt Cloud features (Mesh, scheduler, IDE). Most dbt
Core features work identically.

**Q: "What about performance at scale?"**
A: dbt adds minimal overhead. The actual SQL execution is 99% of the
time. dbt's contribution is compile time (~5-10 seconds) and hook
execution (~1-2 seconds per model). At 989 models, compile time
might be 30-60 seconds.

**Q: "Can this work with Dynamic Tables?"**
A: dbt doesn't natively manage Dynamic Tables. You can create Dynamic
Tables separately and reference them as sources in dbt. Some community
packages are exploring Dynamic Table materializations.

**Q: "What about CI/CD?"**
A: On Snowflake Native, you'd use GitHub Actions or similar to push
code to the Git repo that Snowflake reads from. dbt Cloud provides
built-in CI/CD with slim CI (only test changed models).

**Q: "How do you handle secrets/credentials?"**
A: In Snowflake Native, connection is handled by the Snowflake project
definition. No profiles.yml with passwords. For dbt Core, use
environment variables, never hardcode credentials.

**Q: "What's the learning curve?"**
A: If you know SQL, you can write dbt models immediately. Jinja
templating (macros, conditionals) takes 1-2 weeks to get comfortable.
Advanced patterns (custom materializations, packages) take 1-2 months.

---

**END OF PRESENTATION**
