{{
    config(
        materialized='incremental',
        unique_key='reconciliation_key',
        incremental_strategy='merge',
        merge_update_columns=[
            'invoice_key', 'payment_key', 'invoice_date', 'invoice_amount',
            'payment_date', 'payment_amount', 'days_order_to_invoice',
            'days_invoice_to_payment', 'days_order_to_cash', 'days_past_due',
            'unbilled_amount', 'outstanding_amount', 'reconciliation_status',
            'payment_timing', 'dbt_updated_at', 'dbt_run_id', 'dbt_row_hash'
        ],
        tags=['core', 'merge', 'upsert', 'pattern_example']
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 2: MERGE / UPSERT (incremental_strategy='merge')
═══════════════════════════════════════════════════════════════════════════════

Description:
  - Insert new records, Update existing records
  - Uses MERGE INTO statement in Snowflake
  - Preserves create timestamp, updates last modified

When to Use:
  ✅ Fact tables with updates (status changes, corrections)
  ✅ When you need both INSERT and UPDATE
  ✅ SCD Type 1 updates (overwrite with latest)
  ✅ When source has reliable change tracking

How It Works:
  MERGE INTO target
  USING source ON key_match
  WHEN MATCHED THEN UPDATE SET columns...
  WHEN NOT MATCHED THEN INSERT (columns...) VALUES (...)

Testing This Pattern:
  1. First run: dbt run --select dm_o2c_reconciliation (creates table)
  2. Modify source data or wait for new data
  3. Second run: dbt run --select dm_o2c_reconciliation
  4. Verify:
     - New records have current dbt_created_at
     - Updated records have original dbt_created_at, new dbt_updated_at
     - dbt_row_hash changes for modified records

═══════════════════════════════════════════════════════════════════════════════
#}

WITH orders AS (
    SELECT * FROM {{ ref('stg_enriched_orders') }}
),

invoices AS (
    SELECT * FROM {{ ref('stg_enriched_invoices') }}
),

payments AS (
    SELECT * FROM {{ ref('stg_enriched_payments') }}
),

-- Join all three staging models
reconciliation AS (
    SELECT
        -- Keys
        o.source_system,
        o.order_key,
        COALESCE(i.invoice_key, 'NOT_INVOICED') AS invoice_key,
        COALESCE(p.payment_key, 'NOT_PAID') AS payment_key,
        
        -- Generate composite key for merge
        {{ hash_key(['o.order_key', "COALESCE(i.invoice_key, 'NOT_INVOICED')", "COALESCE(p.payment_key, 'NOT_PAID')"], 'reconciliation_key') }},
        
        -- Order data
        o.order_id,
        o.order_line,
        o.order_date,
        o.order_amount,
        o.currency_code,
        
        -- Customer data (from staging enrichment)
        o.customer_id,
        o.customer_name,
        o.customer_type,
        o.customer_country,
        
        -- Invoice data
        i.invoice_id,
        i.invoice_line,
        i.invoice_date,
        i.invoice_amount,
        i.payment_terms_code,
        i.payment_terms_name,
        i.payment_terms_days,
        i.calculated_due_date AS due_date,
        
        -- Payment data
        p.payment_id,
        p.payment_date,
        p.payment_amount,
        p.bank_name,
        p.bank_country,
        
        -- Calculated metrics
        DATEDIFF('day', o.order_date, i.invoice_date) AS days_order_to_invoice,
        DATEDIFF('day', i.invoice_date, p.payment_date) AS days_invoice_to_payment,
        DATEDIFF('day', o.order_date, p.payment_date) AS days_order_to_cash,
        DATEDIFF('day', i.calculated_due_date, CURRENT_DATE()) AS days_past_due,
        
        -- Amounts
        CASE WHEN i.invoice_key IS NULL THEN o.order_amount ELSE 0 END AS unbilled_amount,
        CASE WHEN p.payment_key IS NULL AND i.invoice_key IS NOT NULL 
             THEN i.invoice_amount ELSE 0 END AS outstanding_amount,
        
        -- Status
        CASE
            WHEN i.invoice_key IS NULL THEN 'NOT_INVOICED'
            WHEN p.payment_key IS NULL THEN 'NOT_PAID'
            WHEN p.payment_amount < i.invoice_amount THEN 'OPEN'
            ELSE 'CLOSED'
        END AS reconciliation_status,
        
        -- Payment timing
        CASE
            WHEN p.payment_date IS NULL AND i.calculated_due_date < CURRENT_DATE() THEN 'OVERDUE'
            WHEN p.payment_date IS NULL THEN 'CURRENT'
            WHEN p.payment_date <= i.calculated_due_date THEN 'ON_TIME'
            ELSE 'LATE'
        END AS payment_timing
        
    FROM orders o
    LEFT JOIN invoices i
        ON o.order_key = i.order_key
    LEFT JOIN payments p
        ON i.invoice_key = p.invoice_key
)

SELECT
    r.*,
    
    -- Row hash for change detection
    {{ row_hash([
        'r.invoice_key', 'r.payment_key', 'r.invoice_amount', 'r.payment_amount',
        'r.reconciliation_status', 'r.payment_timing'
    ]) }},
    
    -- Incremental audit columns (preserve create_ts)
    {{ audit_columns_incremental() }}

FROM reconciliation r

{% if is_incremental() %}
LEFT JOIN {{ this }} t ON r.reconciliation_key = t.reconciliation_key
WHERE 
    -- New records
    t.reconciliation_key IS NULL
    -- Or changed records (compare row hash)
    OR t.dbt_row_hash != {{ row_hash([
        'r.invoice_key', 'r.payment_key', 'r.invoice_amount', 'r.payment_amount',
        'r.reconciliation_status', 'r.payment_timing'
    ], 'new_hash') }}
{% endif %}


