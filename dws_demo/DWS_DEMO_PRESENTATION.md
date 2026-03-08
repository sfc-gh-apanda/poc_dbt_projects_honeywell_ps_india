# dbt Projects on Snowflake: Client Reporting for DWS

## **Customer Demo Presentation**

**Purpose:** Show how DWS can use dbt Projects on Snowflake to build reusable, governed client-reporting data models.
**Duration:** ~45 minutes (Presentation + Live Demo)
**Project:** `dbt_dws_client_reporting`

---
---

# PART 1: dbt BASICS (~10 min)

## 1.1 What is dbt?

dbt stands for **data build tool**. It occupies one specific box in the data stack:

```
Data Sources  →  Ingestion  →  ┌─────────────────┐  →  BI / Reporting
(Bloomberg,       (IICS,       │  TRANSFORMATION  │     (PowerBI,
 Custody,          Fivetran,   │     = dbt        │      Tableau,
 OMS, etc.)        Snowpipe)   └─────────────────┘      Snowsight)
```

dbt is **only** the transformation layer. It does not ingest data into Snowflake,
and it does not render dashboards. It takes raw data that's already landed and
turns it into analytics-ready tables.

**What it does:**
- Turns SELECT statements into tables, views, or incremental loads
- Resolves execution order automatically (dependency graph)
- Tests data quality as part of the build
- Generates documentation and lineage

**What it does not do:**
- Load data into Snowflake (that's your ingestion layer)
- Schedule itself (that's Snowflake Tasks, Airflow, Control-M)
- Visualize results (that's your BI tool)

---

## 1.2 The Core Philosophy: Declarative vs. Imperative

This is the single most important concept. It's the difference between telling
the system **what you want** versus telling it **how to get there**.

**Analogy:**

Think of ordering food at a restaurant.

- **Imperative (traditional):** "Go to the kitchen. Take a pan. Heat oil to
  180 degrees. Take the chicken from the fridge, second shelf. Slice it into
  2cm strips. Place strips in oil for 4 minutes each side. Remove. Plate on
  the white dish. Add sauce from the third container..."
- **Declarative (dbt):** "I'd like the grilled chicken with sauce, please."

You describe the **outcome**. The kitchen (dbt + Snowflake) figures out the steps.

---

## 1.3 Side-by-Side: Traditional vs. dbt

**Requirement:** "Build a daily AUM summary by account and fund, with EUR
market values. On first run, create the table. On subsequent runs, update
existing positions and insert new ones."

### Traditional approach (stored procedures + tasks):

```sql
-- Step 1: Create a staging temp table
CREATE OR REPLACE TEMPORARY TABLE stg_aum_temp AS
SELECT
    h.account_id, h.fund_id, h.holding_date,
    SUM(h.quantity * nav.nav_per_unit * fx.exchange_rate) AS total_market_value_eur
FROM DWS_EDW.DWS_TRAN.FACT_PORTFOLIO_HOLDINGS h
JOIN DWS_EDW.DWS_TRAN.FACT_NAV_PRICES nav
    ON h.fund_id = nav.fund_id AND h.holding_date = nav.price_date
JOIN DWS_EDW.DWS_REF.FACT_FX_RATES fx
    ON h.currency = fx.from_currency AND h.holding_date = fx.rate_date
GROUP BY h.account_id, h.fund_id, h.holding_date;

-- Step 2: Check if target table exists
-- Step 3: If not, CREATE TABLE ... AS SELECT * FROM stg_aum_temp
-- Step 4: If yes, run a MERGE
MERGE INTO DWS_EDW.DWS_CORE.DM_AUM_SUMMARY tgt
USING stg_aum_temp src
    ON tgt.account_id = src.account_id
    AND tgt.fund_id = src.fund_id
    AND tgt.holding_date = src.holding_date
WHEN MATCHED THEN UPDATE SET
    tgt.total_market_value_eur = src.total_market_value_eur,
    tgt.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (account_id, fund_id, holding_date,
    total_market_value_eur, created_at, updated_at)
VALUES (src.account_id, src.fund_id, src.holding_date,
    src.total_market_value_eur, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Step 5: Drop temp table
DROP TABLE stg_aum_temp;

-- Step 6: Make sure this runs AFTER the holdings load finishes
--         (manually maintained in a config table or orchestrator)

-- Step 7: Write a separate script to test uniqueness
-- Step 8: Write a separate script to reconcile AUM
-- Step 9: Document the table somewhere (Confluence? Wiki? Nowhere?)
```

You're telling Snowflake **how** to do it. Every DDL statement, every MERGE
clause, every temp table lifecycle, every dependency -- you manage it all.

### dbt approach (what we actually built in `dm_aum_summary.sql`):

```sql
{{ config(
    materialized='incremental',
    unique_key='aum_key',
    incremental_strategy='merge'
) }}

SELECT
    MD5(h.account_id || '|' || h.fund_id || '|' || TO_VARCHAR(h.holding_date, 'YYYYMMDD')) AS aum_key,
    h.holding_date,
    h.account_id,
    a.client_name,
    h.fund_name,
    SUM(h.market_value_eur) AS total_market_value_eur
FROM {{ ref('stg_holdings') }} h
LEFT JOIN {{ ref('dim_account') }} a ON h.account_id = a.account_id
GROUP BY ...
```

You're telling dbt **what** the result should look like. That's it. dbt handles:

| Concern | Traditional (you do it) | dbt (done for you) |
|---------|------------------------|--------------------|
| First run: create table | Write `CREATE TABLE AS` | `materialized='incremental'` -- dbt does it automatically |
| Subsequent runs: merge | Write full `MERGE ... WHEN MATCHED ...` | `incremental_strategy='merge'` + `unique_key` -- dbt generates the MERGE |
| Dependency ordering | Config table or orchestrator sequence numbers | `{{ ref('stg_holdings') }}` -- dbt reads the ref() calls and builds the graph |
| Testing | Separate test scripts, manually scheduled | Declared in YAML, runs automatically with `dbt build` |
| Documentation | Confluence page that goes stale in a week | Auto-generated from YAML descriptions, always in sync |
| Lineage | "Ask someone who wrote the proc" | Visual DAG, auto-generated from ref() and source() calls |

---

## 1.4 The Three Magic Functions

dbt has three functions that replace most of the operational glue code in
traditional pipelines:

**`ref('model_name')`** -- references another dbt model

```sql
FROM {{ ref('stg_holdings') }}
-- Compiles to: FROM DWS_EDWDEV.DWS_CLIENT_REPORTING_STAGING.STG_HOLDINGS
-- AND creates a dependency: "run stg_holdings BEFORE this model"
```

This is how dbt builds the DAG automatically. No config tables, no sequence numbers.

**`source('source_name', 'table_name')`** -- references a raw/source table

```sql
FROM {{ source('dws_tran', 'FACT_PORTFOLIO_HOLDINGS') }}
-- Compiles to: FROM DWS_EDW.DWS_TRAN.FACT_PORTFOLIO_HOLDINGS
```

If the source database changes, you update one YAML file, not 50 SQL files.

**`config(...)`** -- declares *what* the model should do, not *how*

```sql
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='aum_key') }}
```

Three lines. dbt generates the entire MERGE statement, the CREATE TABLE on first
run, the schema handling -- everything.

---

## 1.5 What Does This Buy You?

**A. Governed change management:**
Every model is a file in Git. Changes go through pull requests. Before merging,
you can see what downstream models are affected (because ref() creates the
lineage graph). Same code deploys to dev, test, then prod.

**B. Built-in data quality gates:**
Tests are declared alongside the models. They run automatically as part of the
build. If a test fails with `severity: error`, the pipeline stops.

**C. Self-documenting lineage:**
Because every model uses ref() and source(), dbt knows exactly what feeds what.
Visual dependency graph -- click on any model and see upstream sources,
downstream consumers, tests, and column descriptions.

**D. Environment parity:**
The SQL never mentions a database name. `ref('stg_holdings')` resolves to
different databases depending on the target:

```
--target dev  → DWS_EDWDEV.DWS_CLIENT_REPORTING_STAGING.STG_HOLDINGS
--target test → DWS_EDWTEST.DWS_CLIENT_REPORTING_STAGING.STG_HOLDINGS
--target prod → DWS_EDW.DWS_CLIENT_REPORTING_STAGING.STG_HOLDINGS
```

Same code. Zero environment-specific edits.

---

## 1.6 What dbt is NOT (Set Expectations)

- **"dbt replaces our stored procedures"** -- Not exactly. dbt replaces the
  *transformation* logic. If your procs do ingestion, API calls, or procedural
  control flow, those stay outside dbt.
- **"dbt is a scheduler"** -- No. dbt is invoked *by* a scheduler (Snowflake
  Tasks, Airflow, Control-M). It runs, transforms data, and exits.
- **"We need to learn a new language"** -- No. dbt models are SQL. The Jinja
  templating (`{{ }}`) is the only addition, and you can start without it.
- **"This is another tool to maintain"** -- With Snowflake Native dbt Projects,
  there's no external server. The dbt engine runs inside Snowflake.

---
---

# PART 2: PROJECT STRUCTURE (~8 min)

## 2.1 The Full Layout

A dbt project is a folder with SQL files, YAML files, and one config file that
ties it all together. This is our actual DWS project:

```
dbt_dws_client_reporting/
│
├── dbt_project.yml            ← 1. THE CONTROL FILE
├── profiles.yml               ← 2. THE CONNECTION
├── packages.yml               ← 3. EXTERNAL DEPENDENCIES
│
├── models/                    ← 4. YOUR TRANSFORMATIONS (SQL)
│   ├── sources/
│   │   └── _sources.yml              ← Where raw data lives
│   ├── staging/dws/
│   │   ├── stg_holdings.sql          ← SELECT statement → becomes a VIEW
│   │   ├── stg_transactions.sql
│   │   ├── stg_nav_prices.sql
│   │   └── _stg_dws.yml             ← Tests & docs for staging models
│   └── marts/
│       ├── dimensions/
│       │   ├── dim_client.sql        ← TABLE (full refresh)
│       │   ├── dim_account.sql
│       │   ├── dim_fund.sql
│       │   └── _dimensions.yml
│       ├── core/
│       │   ├── dm_aum_summary.sql    ← INCREMENTAL (merge)
│       │   ├── dm_cashflow_summary.sql
│       │   ├── dm_client_performance.sql
│       │   ├── dm_portfolio_holdings_asof.sql
│       │   └── _core.yml
│       ├── events/
│       │   ├── fact_client_events.sql ← INCREMENTAL (append)
│       │   └── _events.yml
│       └── aggregates/
│           ├── agg_aum_time_series.sql
│           ├── agg_client_overview.sql
│           └── _aggregates.yml
│
├── macros/                    ← 5. REUSABLE FUNCTIONS (Jinja + SQL)
│   ├── audit/
│   │   ├── audit_columns.sql         ← Standardized audit columns
│   │   └── row_hash.sql             ← Change detection hash
│   └── logging/
│       ├── log_run.sql               ← Run-level audit logging
│       └── log_model.sql            ← Model-level audit logging
│
├── snapshots/                 ← 6. HISTORICAL TRACKING (SCD-2)
│   ├── snap_client.sql
│   └── _snapshots.yml
│
├── tests/                     ← 7. CUSTOM DATA TESTS
│   └── test_aum_reconciliation.sql   ← AUM reconciliation check
│
└── .gitignore                 ← 8. WHAT NOT TO COMMIT
```

---

## 2.2 File-by-File Walkthrough

### `dbt_project.yml` -- The Control File

This is the `settings.json` of the project. Every behavior flows from here.

```yaml
name: 'dbt_dws_client_reporting'
version: '1.0.0'
profile: 'snowflake_dws'
```

The `+post-hook` line means: after *every single model* runs, automatically
log its execution to an audit table. Write it once here, apply to all 14 models:

```yaml
models:
  dbt_dws_client_reporting:
    +persist_docs:
      relation: true
      columns: true
    +post-hook:
      - "{{ log_model_execution() }}"
```

Each subfolder gets its own materialization strategy:

```yaml
    staging:
      +materialized: view        # Staging = views (always current)
      +schema: staging
      +access: protected

    marts:
      dimensions:
        +materialized: table     # Dimensions = full refresh
        +on_schema_change: 'fail'
      core:
        +on_schema_change: 'fail'
      events:
        +materialized: incremental  # Events = append only
      aggregates:
        +materialized: table     # Aggregates = full refresh
```

**Key point:** "Drop a SQL file into the `staging/` folder and it automatically
becomes a view. Drop it into `dimensions/` and it becomes a full-refresh table.
The folder determines the behavior."

---

### `profiles.yml` -- The Connection

Tells dbt how to connect to Snowflake. Three targets for three environments:

```yaml
snowflake_dws:
  target: dev
  outputs:
    dev:
      role: DWS_DEVELOPER
      database: DWS_EDWDEV
      warehouse: DWS_WH_XS
      query_tag: dbt_dws_dev

    test:
      role: DWS_TESTER
      database: DWS_EDWTEST
      warehouse: DWS_WH_S
      query_tag: dbt_dws_test

    prod:
      role: DWS_PROD
      database: DWS_EDW
      warehouse: DWS_WH_M
      query_tag: dbt_dws_prod
```

**Snowflake Native note:** In Snowflake Native dbt Projects, this file isn't
read at runtime. The connection is configured in the Snowflake project object.
We keep it in Git as documentation and for local development.

---

### `packages.yml` -- External Dependencies

Like `requirements.txt` (Python) or `package.json` (Node):

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

Install with `dbt deps`. `dbt_utils` gives community-maintained utility macros.

---

### `models/` -- Your Transformations

Every `.sql` file = one table or view in Snowflake. Two types of files:

**SQL files** -- the transformations (SELECT statements):
```sql
SELECT
    h.holding_date,
    h.account_id,
    h.fund_id,
    MD5(h.account_id || '|' || h.fund_id || '|' || ...) AS holding_key,
    ...
FROM {{ source('dws_tran', 'FACT_PORTFOLIO_HOLDINGS') }} h
LEFT JOIN {{ source('dws_master', 'DIM_FUND') }} f ON ...
```

**YAML files** (`_` prefixed) -- metadata, tests, documentation:
```yaml
models:
  - name: stg_holdings
    description: "Portfolio Holdings enriched with Fund & FX data"
    config:
      contract:
        enforced: true
    columns:
      - name: holding_key
        data_type: varchar
        tests: [not_null]
```

**The `_sources.yml`** registers where raw data lives:
```yaml
sources:
  - name: dws_tran
    database: DWS_EDW
    schema: DWS_TRAN
    tables:
      - name: FACT_PORTFOLIO_HOLDINGS
```

**Naming convention:**

| Prefix | Layer | Purpose |
|--------|-------|---------|
| `stg_` | Staging | Clean and standardize raw data. Always views. |
| `dim_` | Dimensions | Reference/master data tables. Full refresh. |
| `dm_` | Data Marts | Business-ready fact tables. Various load patterns. |
| `fact_` | Events/Facts | Transactional event records. Append-only. |
| `agg_` | Aggregates | Pre-calculated summaries for dashboards. |

---

### `macros/` -- Reusable Functions

Like stored procedures for your *dbt code*. Defined once, called from any model:

```sql
{% macro audit_columns() %}
    '{{ invocation_id }}'::VARCHAR(50) AS dbt_run_id,
    MD5('{{ invocation_id }}' || '{{ this.name }}')::VARCHAR(32) AS dbt_batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_loaded_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_updated_at,
    '{{ this.name }}'::VARCHAR(100) AS dbt_source_model,
    '{{ target.name }}'::VARCHAR(20) AS dbt_environment
{% endmacro %}
```

Every model calls `{{ audit_columns() }}` -- seven standardized columns on
every table, defined in one place.

| Macro | What it generates |
|-------|-------------------|
| `audit_columns()` | 7 audit columns for every row |
| `audit_columns_incremental()` | Same, but preserves `dbt_created_at` on updates |
| `row_hash()` | MD5 fingerprint of business columns for change detection |
| `log_run_start()` / `log_run_end()` | INSERT/UPDATE to `DBT_RUN_LOG` audit table |
| `log_model_execution()` | INSERT to `DBT_MODEL_LOG` per model |

---

### `snapshots/` -- Historical Change Tracking (SCD-2)

When a client's risk profile changes, dbt doesn't overwrite. It closes the
previous version and inserts a new one:

```
client_id | risk_profile | dbt_valid_from      | dbt_valid_to
C001      | MODERATE     | 2024-01-01 00:00:00 | 2024-06-15 08:00:00  ← closed
C001      | AGGRESSIVE   | 2024-06-15 08:00:00 | NULL                 ← current
```

Our snapshot watches 7 columns for changes:

```sql
config(
    strategy='check',
    check_cols=['client_name', 'client_type', 'client_segment',
                'domicile_country', 'risk_profile', 'relationship_manager', 'is_active'],
    invalidate_hard_deletes=True
)
```

---

### `tests/` -- Custom Data Quality Tests

A test is a SELECT that returns **rows that fail**. Zero rows = pass:

```sql
-- test_aum_reconciliation.sql
-- Compares staging-level AUM against mart-level AUM
-- FAILS if any account × date has > 1% discrepancy
SELECT *
FROM comparison
WHERE difference_pct > 1.0
```

---

### `.gitignore` -- What Not to Commit

```
target/          ← compiled SQL (generated)
dbt_packages/    ← installed dependencies (reinstalled via dbt deps)
logs/            ← runtime logs
.user.yml        ← local user settings
```

Everything else is committed. The entire project is code.

---

### Summary

| # | What | File Type | Analogy |
|---|------|-----------|---------|
| 1 | Project config | `dbt_project.yml` | `settings.json` |
| 2 | Connection | `profiles.yml` | Database connection string |
| 3 | Dependencies | `packages.yml` | `requirements.txt` |
| 4 | Models | `models/*.sql` + `*.yml` | Business logic + metadata |
| 5 | Macros | `macros/*.sql` | Reusable functions |
| 6 | Snapshots | `snapshots/*.sql` | SCD-2 historical tracking |
| 7 | Tests | `tests/*.sql` | Custom data quality validations |

Everything is a text file. Everything goes in Git. Everything is reviewable,
version-controlled, and deployable across environments.

---
---

# PART 3: EXECUTION MODES (~5 min)

## 3.1 The Command Landscape

**Tier 1 -- The ones you use every day:**

| Command | What it does | Creates objects in Snowflake? |
|---------|-------------|-------------------------------|
| `dbt build` | Runs models + tests + snapshots, in dependency order | Yes |
| `dbt run` | Runs models only (creates tables/views) | Yes |
| `dbt test` | Runs tests only (validates data) | No (queries only) |

**Tier 2 -- Specific tasks:**

| Command | What it does | Creates objects in Snowflake? |
|---------|-------------|-------------------------------|
| `dbt compile` | Resolves all Jinja, outputs raw SQL -- runs nothing | No |
| `dbt show` | Compiles and runs a model, prints results to screen | No |
| `dbt snapshot` | Runs snapshot models only (SCD-2 change capture) | Yes |
| `dbt deps` | Installs packages from `packages.yml` | No |
| `dbt docs generate` | Generates documentation and lineage catalog | No |

---

## 3.2 `dbt build` -- The One Command to Rule Them All

This is your production command. It does **everything** in the correct order:

```
dbt build
│
├── 1. Snapshots     ← Captures SCD-2 changes (snap_client)
│
├── 2. Models        ← Creates/updates tables and views
│   ├── stg_holdings (view)
│   ├── stg_transactions (view)
│   ├── stg_nav_prices (view)
│   ├── dim_client (table)
│   ├── dim_account (table)
│   ├── dim_fund (table)
│   ├── dm_aum_summary (incremental)
│   ├── ...
│   └── agg_client_overview (table)
│
└── 3. Tests         ← After EACH model, runs its tests
    ├── unique(aum_key) on dm_aum_summary
    ├── not_null(client_id) on dim_client
    ├── relationships(client_id) on dim_account → DIM_CLIENT
    └── test_aum_reconciliation (custom)
```

The critical behavior: **tests run immediately after the model they depend on**.
If `dm_aum_summary` fails its uniqueness test, `agg_aum_time_series` is SKIPPED.
Bad data doesn't propagate.

```
dm_aum_summary (build) → unique(aum_key) test → PASS ✓ → agg_aum_time_series (build)
                                                FAIL ✗ → agg_aum_time_series SKIPPED
```

---

## 3.3 `dbt run` + `dbt test` vs. `dbt build`

```
                dbt run + dbt test (separate)
                ─────────────────────────────

    run ALL models first       then test ALL models
    ┌────────────────────┐     ┌────────────────────┐
    │ stg_holdings       │     │ test: unique        │
    │ dim_account        │     │ test: not_null       │
    │ dm_aum_summary     │     │ test: aum_recon      │
    │ agg_client_overview│     │ ... all tests ...    │
    └────────────────────┘     └────────────────────┘

    Problem: agg_client_overview already consumed
    dm_aum_summary data BEFORE the uniqueness test ran.
    If aum_key had duplicates, the aggregate is already wrong.


                dbt build (interleaved)
                ──────────────────────

    ┌ stg_holdings ──→ test(not_null) ✓
    ├ dim_account  ──→ test(unique, relationships) ✓
    ├ dm_aum_summary ──→ test(unique aum_key) ✗ FAIL!
    │
    └ agg_client_overview → SKIPPED (upstream failed)

    Correct: bad data stopped at the gate.
```

`build` is always safer for production. `run` is fine for fast iteration.

---

## 3.4 Other Commands

**`dbt compile`** -- see the SQL without running it:

Resolves all Jinja and writes raw SQL to the `target/` folder. Useful for
debugging ("What SQL did dbt generate?") and contract validation.

**`dbt show`** -- quick preview without materializing:

```sql
EXECUTE DBT PROJECT dbt_dws_client_reporting
    ARGS = 'show --select dm_portfolio_holdings_asof --limit 5';
```

Runs the SELECT, prints rows. Does not create any table.

**`dbt snapshot`** -- SCD-2 only:

Your scheduled task runs this before `dbt build`:

```sql
-- Step 1: Run snapshots (SCD-2 change capture)
EXECUTE DBT PROJECT dbt_dws_client_reporting
    ARGS = 'snapshot --select tag:daily';
-- Step 2: Build all daily-tagged models and run tests
EXECUTE DBT PROJECT dbt_dws_client_reporting
    ARGS = 'build --select tag:daily';
```

---

## 3.5 Selectors -- Running a Subset

```sql
-- Single model
ARGS = 'run --select dm_aum_summary'

-- Model AND everything upstream (the + prefix)
ARGS = 'run --select +dm_aum_summary'

-- Model AND everything downstream (the + suffix)
ARGS = 'run --select dm_aum_summary+'

-- By tag
ARGS = 'build --select tag:critical'

-- By folder
ARGS = 'run --select marts.dimensions'

-- Exclude specific models
ARGS = 'build --exclude dm_client_performance'

-- Full refresh an incremental model
ARGS = 'run --select dm_aum_summary --full-refresh'
```

---

## 3.6 Quick Reference Card

```
┌──────────────────────────────────────────────────────────────────┐
│  COMMAND          │ MODELS │ TESTS │ SNAPSHOTS │ WRITES TO SF?  │
├──────────────────────────────────────────────────────────────────┤
│  dbt build        │   ✓    │   ✓   │     ✓     │      YES       │
│  dbt run          │   ✓    │       │           │      YES       │
│  dbt test         │        │   ✓   │           │      NO        │
│  dbt snapshot     │        │       │     ✓     │      YES       │
│  dbt compile      │        │       │           │      NO        │
│  dbt show         │        │       │           │      NO        │
│  dbt deps         │        │       │           │      NO        │
│  dbt docs generate│        │       │           │      NO        │
├──────────────────────────────────────────────────────────────────┤
│  SELECTORS        │  EXAMPLE                                    │
├──────────────────────────────────────────────────────────────────┤
│  Single model     │  --select dm_aum_summary                    │
│  + upstream       │  --select +dm_aum_summary                   │
│  + downstream     │  --select dm_aum_summary+                   │
│  By tag           │  --select tag:critical                      │
│  By folder        │  --select marts.dimensions                  │
│  Exclude          │  --exclude dm_client_performance            │
│  Full refresh     │  --full-refresh                             │
└──────────────────────────────────────────────────────────────────┘
```

---
---

# PART 4: THE DEMO PROJECT (~12 min)

## 4.1 The Synthetic Data -- What We Loaded

Before dbt can transform anything, there needs to be raw data in Snowflake.
In a real DWS environment, this comes from systems like SIMON (portfolio
management), Bloomberg (market data), and ECB (FX rates). For this demo, we
created representative sample data with a single SQL script.

**8 source tables across 3 schemas:**

```
DWS_EDW (Production Database)
│
├── DWS_MASTER/              ← Master Data
│   ├── DIM_CLIENT           30 clients   (institutional, private bank, retail)
│   ├── DIM_ACCOUNT          40 accounts  (segregated, pooled, advisory, discretionary)
│   └── DIM_FUND             20 funds     (equity, fixed income, ETF, multi-asset, alternatives)
│
├── DWS_TRAN/                ← Transactional Data (from SIMON)
│   ├── FACT_PORTFOLIO_HOLDINGS   ~920 records  (daily positions: account × fund × date)
│   ├── FACT_TRANSACTIONS          50 records   (BUY, SELL, DIVIDEND, FEE, TRANSFER_IN/OUT)
│   └── FACT_NAV_PRICES          ~760 records   (daily NAV per fund from Bloomberg)
│
└── DWS_REF/                 ← Reference Data
    ├── DIM_BENCHMARK          10 benchmarks (MSCI World, Euro Agg, DAX, S&P 500, etc.)
    └── FACT_FX_RATES        ~440 records   (daily FX rates to EUR from ECB)
                             ─────────────
                             ~2,250 total records
```

**What makes the data realistic:**

- **Clients span the full DWS universe:** Sovereign wealth funds (Abu Dhabi
  Investment Authority, Temasek, NBIM), pension funds (CalPERS, NRW Pension,
  Ontario Teachers), insurance (Bayerische Versicherungskammer, Generali,
  Allianz), private banking (Dr. von Harenberg, Chen Family Office), and
  retail (Thomas Braun, Sophie Laurent).

- **Accounts have proper mandate structures:** e.g. ACC001 = "BVK Global
  Equity" (segregated, EUR, 35bps fee), ACC006 = "ADIA Infrastructure
  Allocation" (segregated, USD, 75bps fee).

- **Funds have real ISINs:** FND001 = "DWS Top Dividende" (DE0008490962,
  145bps TER), FND007 = "Xtrackers S&P 500 ETF" (7bps TER). One fund
  (FND020) is intentionally inactive to test soft-delete handling.

- **Holdings simulate daily positions with market noise:** CROSS JOIN
  between 40 business days (Jan-Feb 2024) and 23 account-fund pairs,
  with random quantity variation and market value drift.

- **Transactions cover the full lifecycle:** BUYs, SELLs, DIVIDENDs, FEEs,
  and a TRANSFER_IN. Month-end fees simulate real management fee accruals.

- **Two intentionally inactive records** test edge cases: CLI026 "Defunct
  Holdings GmbH" and CLI027 "Closed Pension Fund UK" (`is_active = FALSE`).

- **FX rates cover 10 currencies** (USD, GBP, CHF, JPY, CAD, SGD, AUD,
  NOK, DKK, KRW) plus EUR-to-EUR at 1.0.

**Environment cloning:**

```sql
CREATE DATABASE IF NOT EXISTS DWS_EDWDEV;
CREATE SCHEMA IF NOT EXISTS DWS_EDWDEV.DWS_TRAN   CLONE DWS_EDW.DWS_TRAN;
CREATE SCHEMA IF NOT EXISTS DWS_EDWDEV.DWS_MASTER CLONE DWS_EDW.DWS_MASTER;
CREATE SCHEMA IF NOT EXISTS DWS_EDWDEV.DWS_REF    CLONE DWS_EDW.DWS_REF;
-- Same for DWS_EDWTEST
```

Snowflake's zero-copy clone gives dev and test identical data instantly,
at zero additional storage cost.

---

## 4.2 The Data Flow -- Source to Client Report

### Full Architecture

```
 ┌────────────────────────────────────────────────────────────────────────┐
 │                        RAW SOURCE TABLES                               │
 │  FACT_PORTFOLIO_HOLDINGS    FACT_NAV_PRICES    FACT_FX_RATES           │
 │  DIM_CLIENT    DIM_ACCOUNT    DIM_FUND    DIM_BENCHMARK               │
 └───────────┬──────────┬──────────┬──────────┬──────────────────────────┘
             │          │          │          │
             ▼          ▼          ▼          ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │  LAYER 1: STAGING (views)                                              │
 │  stg_holdings         stg_transactions        stg_nav_prices           │
 │  (Holdings + Fund     (Transactions + Account  (NAV + Fund             │
 │   + NAV + FX → EUR)    + Fund + FX → EUR)       + Benchmark)           │
 └───────────┬──────────────────┬─────────────────────────────────────────┘
             │                  │
             ▼                  ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │  LAYER 2: DIMENSIONS (tables -- full refresh)                          │
 │  dim_client (SCD-2 via snap_client)                                    │
 │  dim_account (enriched with client + benchmark)                        │
 │  dim_fund (enriched with benchmark)                                    │
 └───────────┬────────────────────────────────────────────────────────────┘
             │
             ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │  LAYER 3: CORE MARTS (mixed patterns)                                  │
 │  dm_aum_summary                 (MERGE upsert)                         │
 │  dm_portfolio_holdings_asof     (TABLE with as-of date filter)         │
 │  dm_cashflow_summary            (DELETE+INSERT last N days)            │
 │  dm_client_performance          (TABLE full refresh)                    │
 │  fact_client_events             (APPEND only)                          │
 └───────────┬────────────────────────────────────────────────────────────┘
             │
             ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │  LAYER 4: AGGREGATES (tables -- dashboard-ready)                       │
 │  agg_aum_time_series            (daily AUM + day-over-day change)      │
 │  agg_client_overview            (one row per client, executive KPIs)   │
 │                                                                        │
 │  ← These are what PowerBI / Tableau / Snowsight dashboards query →    │
 └────────────────────────────────────────────────────────────────────────┘
```

### Tracing One Record Through the Pipeline

Follow a single holding: ACC001 (BVK Global Equity) holds 150,000 units of
FND001 (DWS Top Dividende) on 2024-01-15:

| Layer | What happens | Key transformation |
|-------|-------------|-------------------|
| **Source** | Raw record: qty=150,000, cost=95.50, currency=EUR | Just data |
| **stg_holdings** | JOIN Fund (adds name, ISIN), JOIN NAV (96.20), JOIN FX (1.0). Calculates `market_value_eur` = 150,000 x 96.20 x 1.0 = **14,430,000 EUR** | Enrichment + FX done once |
| **dim_account** | ACC001 enriched with CLI001 (Bayerische Versicherungskammer, INSURANCE, Germany) + BM001 (MSCI World) | Client + benchmark context |
| **dm_aum_summary** | Aggregates to account x fund x date. MERGE upserts. `dbt_created_at` preserved. | Merge with watermark |
| **dm_portfolio_holdings_asof** | Filters to as-of date. Calculates P&L = 14,430,000 - 14,325,000 = **+105,000 EUR** | Point-in-time valuation |
| **agg_client_overview** | CLI001 across ALL accounts → total AUM, flows, client tier = TIER_1 (>100M EUR) | Executive KPI summary |

Enrichment happens once in staging. Every downstream model inherits it. If the
FX logic changes, update one view, and all marts pick it up automatically.

---

## 4.3 Setup Scripts -- Before First Run

Three scripts run once before the first `dbt build`:

```
STEP 1: DWS_LOAD_SAMPLE_DATA.sql
  ├── Creates DWS_EDW database + 3 source schemas
  ├── Loads 8 tables with ~2,250 sample records
  ├── Clones source data to DWS_EDWDEV and DWS_EDWTEST
  ├── Creates 3 roles (DWS_DEVELOPER, DWS_TESTER, DWS_PROD)
  └── Grants permissions per environment

STEP 2: DWS_AUDIT_SETUP.sql
  ├── Creates DWS_AUDIT schema in all 3 databases
  ├── Creates DBT_RUN_LOG table (run-level audit)
  └── Creates DBT_MODEL_LOG table (model-level audit)

STEP 3: Deploy dbt project to Snowflake
  └── Register dbt_dws_client_reporting as a Snowflake dbt Project
```

---

## 4.4 Running the Project

### First Run -- Full Build

```sql
EXECUTE DBT PROJECT dbt_dws_client_reporting
    ARGS = 'build --target dev';
```

What happens in order:

```
 TIME   │ STEP                                    │ SNOWFLAKE ACTION
────────┼─────────────────────────────────────────┼──────────────────────────
 0:00   │ on-run-start: log_run_start()           │ INSERT into DBT_RUN_LOG
────────┼─────────────────────────────────────────┼──────────────────────────
 0:01   │ snap_client (snapshot)                   │ Creates SNAP_CLIENT
────────┼─────────────────────────────────────────┼──────────────────────────
 0:02   │ stg_holdings (view)                      │ CREATE VIEW
 0:03   │ stg_transactions (view)                  │ CREATE VIEW
 0:04   │ stg_nav_prices (view)                    │ CREATE VIEW
────────┼─────────────────────────────────────────┼──────────────────────────
 0:05   │ dim_client (table)                       │ CREATE TABLE AS SELECT
 0:06   │ dim_account (table) → tests              │ unique, relationships
 0:07   │ dim_fund (table) → tests                 │ unique(fund_key)
────────┼─────────────────────────────────────────┼──────────────────────────
 0:08   │ dm_aum_summary (incremental - 1st run)   │ CREATE TABLE AS SELECT
        │   → tests: unique(aum_key)               │
 0:09   │ dm_cashflow_summary                      │ CREATE TABLE AS SELECT
 0:10   │ dm_client_performance                    │ CREATE TABLE AS SELECT
 0:11   │ dm_portfolio_holdings_asof               │ CREATE TABLE AS SELECT
 0:12   │ fact_client_events                       │ CREATE TABLE AS SELECT
────────┼─────────────────────────────────────────┼──────────────────────────
 0:13   │ agg_aum_time_series                      │ CREATE TABLE AS SELECT
 0:14   │ agg_client_overview → tests              │ unique(client_id)
────────┼─────────────────────────────────────────┼──────────────────────────
 0:15   │ test_aum_reconciliation                  │ SELECT ... WHERE diff>1%
────────┼─────────────────────────────────────────┼──────────────────────────
 0:16   │ on-run-end: log_run_end()               │ UPDATE DBT_RUN_LOG
        │                                         │   status = 'SUCCESS'
```

### Second Run -- Incremental Behavior

| Model | First Run | Second Run |
|-------|-----------|------------|
| stg_holdings | CREATE VIEW | No change |
| dim_account | CREATE TABLE | CREATE OR REPLACE TABLE |
| dm_aum_summary | CREATE TABLE (full load) | **MERGE** (upsert) |
| dm_cashflow_summary | CREATE TABLE (full load) | **DELETE last 3 days + INSERT** |
| fact_client_events | CREATE TABLE (full load) | **INSERT only new events** |
| agg_client_overview | CREATE TABLE | CREATE OR REPLACE TABLE |

### Verification Queries

```sql
-- What did dbt create?
SELECT table_schema, table_name, table_type, row_count
FROM DWS_EDWDEV.INFORMATION_SCHEMA.TABLES
WHERE table_schema LIKE 'DWS_CLIENT_REPORTING%'
   OR table_schema = 'DWS_SNAPSHOTS'
ORDER BY table_schema, table_name;

-- Audit watermark (created_at preserved, updated_at changes)
SELECT aum_key, holding_date, client_name, total_market_value_eur,
       dbt_created_at, dbt_updated_at, dbt_run_id
FROM DWS_EDWDEV.DWS_CLIENT_REPORTING_CORE.DM_AUM_SUMMARY
ORDER BY dbt_updated_at DESC LIMIT 5;

-- Run-level audit
SELECT run_id, environment, run_status, run_duration_seconds,
       models_run, models_success, models_failed
FROM DWS_EDWDEV.DWS_AUDIT.DBT_RUN_LOG
ORDER BY run_started_at DESC LIMIT 5;

-- Model-level detail
SELECT model_name, materialization, is_incremental, status
FROM DWS_EDWDEV.DWS_AUDIT.DBT_MODEL_LOG
WHERE run_id = (SELECT MAX(run_id) FROM DWS_EDWDEV.DWS_AUDIT.DBT_RUN_LOG)
ORDER BY started_at;
```

---
---

# PART 5: FEATURES THAT ARE HARD TRADITIONALLY, EASY IN dbt (~10 min)

## 5.1 Data Contracts -- Compile-Time Schema Enforcement

**Traditional pain:** Someone adds a column to a stored procedure's SELECT.
Downstream tables get a new column silently. Or columns shift and values end
up in the wrong column. Discovered when a client report shows NAV in the fees
column.

**dbt solution:** Declare a **contract** in YAML:

```yaml
- name: dm_aum_summary
  config:
    contract:
      enforced: true
  columns:
    - name: aum_key
      data_type: varchar
    - name: holding_date
      data_type: date
    - name: total_market_value_eur
      data_type: number
```

If someone renames `total_market_value_eur` to `market_value_eur` in the SQL:

```
Traditional: Table silently changes. Downstream breaks at runtime.
dbt:         Compilation Error: column "total_market_value_eur" missing
             Build BLOCKED. Nothing deploys.
```

11 out of 14 models in this project have enforced contracts.

---

## 5.2 Schema Change Enforcement -- Per Layer

**Traditional pain:** Source table gains a column. Pipeline silently carries or
drops it. Nobody knows.

**dbt solution:** Different behavior per layer, one line each:

```yaml
staging:
  +on_schema_change: 'append_new_columns'    # tolerant

marts:
  dimensions:
    +on_schema_change: 'fail'                # strict
  core:
    +on_schema_change: 'fail'                # strict
```

| Layer | Policy | Why |
|-------|--------|-----|
| Staging | `append_new_columns` | Sources may evolve. Pick up new columns automatically. |
| All marts | `fail` | Mart schemas are the published API. Changes must be deliberate. |

To implement this traditionally, you'd need a pre-run stored procedure that
queries `INFORMATION_SCHEMA.COLUMNS` and compares against metadata -- 50+ lines
per table. In dbt, it's one line.

---

## 5.3 Audit Columns -- Uniform Metadata on Every Row

**Traditional pain:** Each stored procedure manually adds `created_at`,
`updated_at`. Each developer implements it differently. Some forget.
Incremental procedures need manual COALESCE logic for `created_at`.

**dbt solution:** One macro, called in every model:

```sql
{% macro audit_columns() %}
    '{{ invocation_id }}'::VARCHAR(50) AS dbt_run_id,
    MD5('{{ invocation_id }}' || '{{ this.name }}')::VARCHAR(32) AS dbt_batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_loaded_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_updated_at,
    '{{ this.name }}'::VARCHAR(100) AS dbt_source_model,
    '{{ target.name }}'::VARCHAR(20) AS dbt_environment
{% endmacro %}
```

Seven columns. Identical everywhere. The model SQL just says
`{{ audit_columns() }}` -- one line.

For incremental models, `audit_columns_incremental()` preserves `dbt_created_at`:

```sql
{% if is_incremental() %}
COALESCE(existing.dbt_created_at, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS dbt_created_at,
{% else %}
CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_created_at,
{% endif %}
```

**The watermark behavior:**

```
FIRST INSERT (Jan 1):     dbt_created_at = 2026-01-01    dbt_updated_at = 2026-01-01
SUBSEQUENT MERGE (Feb 11):dbt_created_at = 2026-01-01 ← PRESERVED
                          dbt_updated_at = 2026-02-11 ← UPDATED
```

Add a new audit column? Update the macro, run `dbt build` -- every table
gets it automatically.

---

## 5.4 Row Hash -- Change Detection

**Traditional pain:** MERGE statements update every column on match, whether
data changed or not. Inflates update counts, breaks change-data-capture.

**dbt solution:**

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

Used in models: `{{ row_hash(['SUM(h.market_value_eur)', ...]) }}`

Creates an MD5 fingerprint. If any value changes, the hash changes. Downstream
consumers compare `dbt_row_hash` between runs: "Did this row actually change?"

---

## 5.5 Data Load Patterns -- Four Strategies From Config

**Traditional pain:** Each pattern (full refresh, merge, append, delete+insert)
requires different DDL/DML. Four stored procedure templates x N tables =
dozens of procedural scripts.

**dbt solution:** Same SELECT, different config block:

**Pattern 1: Full Refresh** (dimensions)
```sql
{{ config(materialized='table') }}
SELECT ... FROM ...
-- dbt: CREATE OR REPLACE TABLE ... AS (SELECT ...)
```

**Pattern 2: Incremental Merge** (AUM summary)
```sql
{{ config(
    materialized='incremental',
    unique_key='aum_key',
    incremental_strategy='merge',
    merge_update_columns=['total_market_value_eur', 'dbt_updated_at', ...]
) }}
SELECT ... FROM ...
-- dbt: MERGE INTO ... USING ... ON ... WHEN MATCHED ... WHEN NOT MATCHED ...
```

**Pattern 3: Append Only** (event log)
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}
SELECT ... WHERE event_date > (SELECT MAX(event_date) FROM {{ this }})
-- dbt: INSERT INTO ... SELECT ...
```

**Pattern 4: Delete + Insert** (cashflow summary)
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['account_id', 'transaction_date', 'fund_id'],
    incremental_predicates=["... >= DATEADD('day', -3, CURRENT_DATE())"]
) }}
SELECT ... WHERE transaction_date >= DATEADD('day', -3, CURRENT_DATE())
-- dbt: DELETE FROM ... WHERE predicate; INSERT INTO ... SELECT ...
```

```
Traditional: 4 patterns = 4 stored procedure templates × N tables
             + manual dependency config + manual error handling

dbt:         4 patterns = 4 config() blocks
             Same SELECT syntax. Dependencies auto-resolved.
```

---

## 5.6 Auto-Generated Documentation & Lineage

**Traditional pain:** Documentation in Confluence (stale). Lineage in a Visio
diagram (from 2022). Nobody knows what breaks if you change a source column.

**dbt solution:**

```sql
EXECUTE DBT PROJECT dbt_dws_client_reporting
    ARGS = 'docs generate --target dev';
```

Produces:
- **Interactive lineage graph** -- click any model, see upstream/downstream
- **Column-level documentation** -- from YAML descriptions
- **Test coverage** -- which columns are tested and how
- **SQL source** -- click any model to see actual SQL

Descriptions are pushed to Snowflake via `persist_docs`:

```yaml
+persist_docs:
  relation: true
  columns: true
```

YAML descriptions appear as `COMMENT` on tables and columns in Snowflake's
`INFORMATION_SCHEMA`. Tools like Snowsight, Alation, Collibra can read them.

Color-coded DAG via `node_color`:

| Layer | Color | Hex |
|-------|-------|-----|
| Staging | Green | `#4CAF50` |
| Dimensions | Blue | `#2196F3` |
| Core Marts | Purple | `#9C27B0` |
| Events | Orange | `#FF9800` |
| Aggregates | Pink | `#E91E63` |

---

## 5.7 Access Control at the Model Level

**Traditional pain:** Controlling which teams can read which tables requires
GRANT/REVOKE managed separately from transformation code.

**dbt solution:** Access levels alongside the model:

```yaml
staging:
  +access: protected       # Only this project can ref() staging

dimensions:
  +access: public           # Other projects can ref() these
```

| Access Level | Who Can Use ref() | Use Case |
|:-------------|:------------------|:---------|
| `private` | Same folder only | Internal helpers |
| `protected` | Same project only | Staging views (implementation detail) |
| `public` | Any project (via dbt Mesh) | Published APIs (dimensions, marts) |

---

## 5.8 Summary

```
┌──────────────────────────────────────────────────────────────────────┐
│  FEATURE              │ TRADITIONAL            │ dbt                │
├──────────────────────────────────────────────────────────────────────┤
│  Data contracts       │ Manual column checks   │ YAML declaration,  │
│                       │ or nothing             │ compile-time check │
├──────────────────────────────────────────────────────────────────────┤
│  Schema enforcement   │ Custom INFORMATION_    │ on_schema_change:  │
│                       │ SCHEMA proc per table  │ one line per layer │
├──────────────────────────────────────────────────────────────────────┤
│  Audit columns        │ Copy-paste per proc,   │ One macro, called  │
│                       │ inconsistent           │ in every model     │
├──────────────────────────────────────────────────────────────────────┤
│  Row hash             │ Hand-written MD5 per   │ row_hash() macro,  │
│                       │ proc, each different   │ consistent         │
├──────────────────────────────────────────────────────────────────────┤
│  Load patterns        │ 4 proc templates ×     │ 4 config blocks,   │
│                       │ N tables = many files  │ same SELECT syntax │
├──────────────────────────────────────────────────────────────────────┤
│  Documentation        │ Confluence (stale)     │ Auto-generated,    │
│                       │ + Visio (2022)         │ always in sync     │
├──────────────────────────────────────────────────────────────────────┤
│  Access control       │ GRANT/REVOKE separate  │ access: public/    │
│                       │ from code              │ protected in YAML  │
└──────────────────────────────────────────────────────────────────────┘
```

---
---

# PART 6: SCHEDULING, PROMOTION & OBSERVABILITY (~5 min)

## 6.1 Scheduling via Snowflake Tasks

Three tasks automate the pipeline:

### Task 1: Daily Build (6 AM CET)

```sql
CREATE OR REPLACE TASK DWS_EDW.DWS_AUDIT.DWS_DAILY_DBT_BUILD
    WAREHOUSE = DWS_WH_M
    SCHEDULE = 'USING CRON 0 6 * * * Europe/Berlin'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_TIMEOUT_MS = 3600000
AS
    EXECUTE DBT PROJECT dbt_dws_client_reporting
        ARGS = 'snapshot --select tag:daily';
    EXECUTE DBT PROJECT dbt_dws_client_reporting
        ARGS = 'build --select tag:daily';
```

### Task 2: Weekly Full Refresh (Sunday 2 AM)

```sql
CREATE OR REPLACE TASK DWS_EDW.DWS_AUDIT.DWS_WEEKLY_FULL_REFRESH
    WAREHOUSE = DWS_WH_M
    SCHEDULE = 'USING CRON 0 2 * * 0 Europe/Berlin'
AS
    EXECUTE DBT PROJECT dbt_dws_client_reporting
        ARGS = 'build --full-refresh --select tag:weekly';
```

### Task 3: Reconciliation Check (after daily build)

```sql
CREATE OR REPLACE TASK DWS_EDW.DWS_AUDIT.DWS_RECONCILIATION_CHECK
    WAREHOUSE = DWS_WH_S
    AFTER DWS_EDW.DWS_AUDIT.DWS_DAILY_DBT_BUILD
AS
    EXECUTE DBT PROJECT dbt_dws_client_reporting
        ARGS = 'test --select tag:reconciliation';
```

Error handling: tasks auto-suspend after 3 consecutive failures:
```sql
ALTER TASK DWS_DAILY_DBT_BUILD SET SUSPEND_TASK_AFTER_NUM_FAILURES = 3;
```

---

## 6.2 Promotion: Dev → Test → Prod

Same code, different `--target` flag:

```sql
-- Dev
EXECUTE DBT PROJECT dbt_dws_client_reporting ARGS = 'build --target dev';
-- Test
EXECUTE DBT PROJECT dbt_dws_client_reporting ARGS = 'build --target test';
-- Prod
EXECUTE DBT PROJECT dbt_dws_client_reporting ARGS = 'build --target prod';
```

Each target resolves to a different environment:

| | Dev | Test | Prod |
|---|---|---|---|
| **Database** | DWS_EDWDEV | DWS_EDWTEST | DWS_EDW |
| **Warehouse** | DWS_WH_XS | DWS_WH_S | DWS_WH_M |
| **Role** | DWS_DEVELOPER | DWS_TESTER | DWS_PROD |
| **Threads** | 4 | 4 | 8 |
| **Query Tag** | dbt_dws_dev | dbt_dws_test | dbt_dws_prod |

No find-and-replace. No environment-specific SQL.

---

## 6.3 Observability

### Built-in Audit Tables

**`DBT_RUN_LOG`** -- one row per `dbt build` execution:

```sql
SELECT run_id, project_name, environment, run_status,
       run_duration_seconds, models_run, models_success, models_failed
FROM DWS_EDWDEV.DWS_AUDIT.DBT_RUN_LOG
ORDER BY run_started_at DESC LIMIT 5;
```

**`DBT_MODEL_LOG`** -- one row per model per run:

```sql
SELECT model_name, materialization, incremental_strategy,
       status, rows_affected, started_at, ended_at
FROM DWS_EDWDEV.DWS_AUDIT.DBT_MODEL_LOG
WHERE run_id = '<latest_invocation_id>'
ORDER BY started_at;
```

### Monitoring Dashboard (22 views)

`DWS_MONITORING_DASHBOARD.sql` creates 22 monitoring views using
`SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`:

| Category | Views | Examples |
|----------|-------|---------|
| Execution Tracking | 4 | Model executions, test executions, daily summary, slowest models |
| Freshness | 2 | Source freshness, model freshness |
| Alerts | 4 | Performance degradation, model failures, stale sources, alert summary |
| Test Insights | 3 | Test summary by type, pass rate trend, recurring failures |
| Cost Monitoring | 3 | Daily credits, cost by model, cost anomalies |
| Query Performance | 3 | Long-running queries, queue time analysis, performance trend |
| Data Quality | 3 | Row count tracking, data reconciliation, operational summary |

### Testing Layers

| Layer | Type | Example |
|-------|------|---------|
| Schema tests | `unique`, `not_null` | `aum_key` is unique and not null |
| Referential integrity | `relationships` | Every `client_id` in dim_account exists in DIM_CLIENT |
| Accepted values | `accepted_values` | `transaction_type` must be BUY/SELL/DIVIDEND/FEE/TRANSFER_IN/TRANSFER_OUT |
| Custom reconciliation | SQL test | AUM at staging vs mart level, 1% tolerance |
| Contracts | Compile-time | Column names and types match declaration |

---
---

# PART 7: LIVE DEMO SCRIPT (~15 min)

## Step 1: Show the dbt Project in Snowsight (2 min)

1. Navigate to **Data > Databases > DWS_EDWDEV**
2. Open the dbt Project object
3. **Show the model graph/lineage** -- visual DAG

---

## Step 2: Run Tests and Inspect Failures (5 min)

```sql
-- Run just the tests
EXECUTE DBT PROJECT dbt_dws_client_reporting
    ARGS = 'test --target dev';
```

Narrate: "dbt is executing tests: unique keys on every dimension, not_null
on every business key, accepted_values on transaction types, relationships
for referential integrity, and our custom AUM reconciliation."

After completion, query audit logs:

```sql
SELECT * FROM DWS_EDWDEV.DWS_AUDIT.DBT_RUN_LOG
ORDER BY run_started_at DESC LIMIT 5;

SELECT model_name, materialization, status, rows_affected
FROM DWS_EDWDEV.DWS_AUDIT.DBT_MODEL_LOG
WHERE run_id = (SELECT MAX(run_id) FROM DWS_EDWDEV.DWS_AUDIT.DBT_RUN_LOG)
ORDER BY started_at;
```

---

## Step 3: Trigger a Full Build and Show Run History (5 min)

```sql
EXECUTE DBT PROJECT dbt_dws_client_reporting
    ARGS = 'build --target dev';
```

While it runs, switch to **Activity > Query History** and filter by
`query_tag LIKE 'dbt_%'` to show queries executing in real-time.

After completion:

```sql
-- AUM summary with audit columns
SELECT account_id, client_name, fund_name, holding_date,
       total_market_value_eur, dbt_run_id, dbt_created_at, dbt_updated_at
FROM DWS_EDWDEV.DWS_CLIENT_REPORTING_CORE.DM_AUM_SUMMARY
ORDER BY total_market_value_eur DESC LIMIT 10;

-- As-of holdings
SELECT client_name, fund_name, as_of_date, market_value_eur, unrealized_pnl_eur
FROM DWS_EDWDEV.DWS_CLIENT_REPORTING_CORE.DM_PORTFOLIO_HOLDINGS_ASOF
ORDER BY market_value_eur DESC LIMIT 10;

-- Client overview (executive dashboard table)
SELECT client_name, client_type, client_tier,
       current_aum_eur, total_net_flows_eur, total_dividends_eur
FROM DWS_EDWDEV.DWS_CLIENT_REPORTING_AGGREGATES.AGG_CLIENT_OVERVIEW
ORDER BY current_aum_eur DESC;
```

---

## Step 4: Show Task Scheduling (3 min)

```sql
SHOW TASKS IN SCHEMA DWS_EDW.DWS_AUDIT;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC LIMIT 20;
```

---
---

# PRE-DEMO CHECKLIST

Before the customer session, verify:

- [ ] **Sample data loaded:** Run `DWS_LOAD_SAMPLE_DATA.sql`
- [ ] **Audit tables exist:** Run `DWS_AUDIT_SETUP.sql`
- [ ] **dbt project deployed:** Registered as a Snowflake dbt Project
- [ ] **At least one successful build:** Run `dbt build --target dev`
- [ ] **Run it a second time:** To show incremental behavior
      (merge updates `dbt_updated_at` but preserves `dbt_created_at`)
- [ ] **Monitoring views created:** Run `DWS_MONITORING_DASHBOARD.sql`
- [ ] **Tasks created:** Run `DWS_SCHEDULING.sql`
      (leave tasks suspended -- show but don't enable during demo)

---
---

# ANTICIPATED CUSTOMER QUESTIONS

| Question | Answer |
|----------|--------|
| "How do you handle late-arriving data?" | Delete+insert pattern with configurable reload window (`var('reload_days', 3)`) |
| "Can we query historical client data?" | SCD-2 via snapshots with point-in-time queries (`dbt_valid_from/to`) |
| "What about FX conversion?" | Done once in staging, inherited by all marts |
| "How do we know the data is correct?" | AUM reconciliation test (source vs mart, 1% tolerance) |
| "What's the audit trail?" | Every row has 7 audit columns + run/model logging |
| "How do we promote to production?" | Same code, different `--target` flag |
| "How does Snowflake Native dbt differ from dbt Core?" | Same engine, runs inside Snowflake. No external server needed. |
| "What about performance at scale?" | dbt adds ~5-10s compile time. SQL execution is 99% of runtime. |
| "Can this work with Dynamic Tables?" | dbt doesn't natively manage them. Reference as sources in dbt. |
| "What's the learning curve?" | SQL people can start immediately. Jinja takes 1-2 weeks. Advanced patterns 1-2 months. |

---

**END OF PRESENTATION**
