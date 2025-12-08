{{
    config(
        materialized='incremental',
        unique_key=['source_system', 'order_date', 'order_key'],
        incremental_strategy='delete+insert',
        incremental_predicates=[
            "DBT_INTERNAL_DEST.order_date >= DATEADD('day', -" ~ var('reload_days', 3) ~ ", CURRENT_DATE())"
        ],
        tags=['partitioned', 'delete_insert', 'pattern_example']
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 4: DELETE + INSERT (incremental_strategy='delete+insert')
═══════════════════════════════════════════════════════════════════════════════

Description:
  - Delete records matching predicate, then insert fresh data
  - Ideal for partition-based reloads
  - Handles late-arriving data and corrections

When to Use:
  ✅ Date-partitioned fact tables
  ✅ When you need to reload specific partitions (e.g., last 3 days)
  ✅ Late-arriving data scenarios
  ✅ Data corrections/restatements

How It Works:
  1. DELETE FROM target WHERE order_date >= (TODAY - 3 days)
  2. INSERT INTO target SELECT * FROM source WHERE order_date >= (TODAY - 3 days)

Configuration:
  - var('reload_days'): Number of days to reload (default: 3)
  - Run with: dbt run --select fact_o2c_daily --vars '{"reload_days": 7}'

Testing This Pattern:
  1. First run: dbt run --select fact_o2c_daily (creates table)
  2. Note row counts for last 3 days
  3. Modify source data for 2 days ago
  4. Second run: dbt run --select fact_o2c_daily
  5. Verify:
     - Records older than 3 days: UNCHANGED
     - Records within 3 days: DELETED and RE-INSERTED
     - All records within window have current dbt_loaded_at

═══════════════════════════════════════════════════════════════════════════════
#}

WITH daily_orders AS (
    SELECT
        source_system,
        order_date,
        order_key,
        order_id,
        order_line,
        customer_id,
        customer_name,
        order_amount,
        currency_code,
        order_status
    FROM {{ ref('stg_enriched_orders') }}
    
    -- Reload window: Last N days (configurable via var)
    WHERE order_date >= DATEADD('day', -{{ var('reload_days', 3) }}, CURRENT_DATE())
),

daily_invoices AS (
    SELECT
        order_key,
        invoice_key,
        invoice_date,
        invoice_amount,
        due_date
    FROM {{ ref('stg_enriched_invoices') }}
    WHERE invoice_date >= DATEADD('day', -{{ var('reload_days', 3) }}, CURRENT_DATE())
),

daily_payments AS (
    SELECT
        invoice_key,
        payment_key,
        payment_date,
        payment_amount
    FROM {{ ref('stg_enriched_payments') }}
    WHERE payment_date >= DATEADD('day', -{{ var('reload_days', 3) }}, CURRENT_DATE())
)

SELECT
    -- Partition key
    o.order_date,
    
    -- Keys
    o.source_system,
    o.order_key,
    COALESCE(i.invoice_key, 'NOT_INVOICED') AS invoice_key,
    COALESCE(p.payment_key, 'NOT_PAID') AS payment_key,
    
    -- Order details
    o.order_id,
    o.order_line,
    o.customer_id,
    o.customer_name,
    o.order_amount,
    o.currency_code,
    o.order_status,
    
    -- Invoice details
    i.invoice_date,
    i.invoice_amount,
    i.due_date,
    
    -- Payment details
    p.payment_date,
    p.payment_amount,
    
    -- Daily aggregates
    CASE
        WHEN i.invoice_key IS NOT NULL THEN 'INVOICED'
        ELSE 'NOT_INVOICED'
    END AS invoice_status,
    
    CASE
        WHEN p.payment_key IS NOT NULL THEN 'PAID'
        ELSE 'UNPAID'
    END AS payment_status,
    
    -- Full audit columns (since we're always inserting fresh)
    {{ audit_columns() }}

FROM daily_orders o
LEFT JOIN daily_invoices i ON o.order_key = i.order_key
LEFT JOIN daily_payments p ON i.invoice_key = p.invoice_key


