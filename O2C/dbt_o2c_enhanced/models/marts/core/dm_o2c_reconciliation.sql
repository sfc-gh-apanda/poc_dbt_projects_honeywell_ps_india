{{
    config(
        materialized='incremental',
        snowflake_warehouse=get_warehouse(),
        unique_key='reconciliation_key',
        incremental_strategy='merge',
        merge_update_columns=[
            'invoice_key', 'payment_key', 'invoice_date', 'invoice_amount',
            'payment_date', 'payment_amount', 'days_order_to_invoice',
            'days_invoice_to_payment', 'days_order_to_cash', 'days_past_due',
            'unbilled_amount', 'outstanding_amount', 'reconciliation_status',
            'payment_timing', 'dbt_updated_at', 'dbt_run_id', 'dbt_batch_id',
            'dbt_loaded_at', 'dbt_source_model', 'dbt_environment', 'dbt_row_hash'
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

Testing This Pattern:
  1. First run: dbt run --select dm_o2c_reconciliation (creates table)
  2. Modify source data or wait for new data
  3. Second run: dbt run --select dm_o2c_reconciliation
  4. Verify:
     - New records have current dbt_created_at
     - Updated records have original dbt_created_at, new dbt_updated_at

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Keys
    orders.source_system,
    orders.order_key,
    COALESCE(inv.invoice_key, 'NOT_INVOICED') AS invoice_key,
    COALESCE(pay.payment_key, 'NOT_PAID') AS payment_key,
    
    -- Composite key for merge
    MD5(orders.order_key || '|' || COALESCE(inv.invoice_key, 'NOT_INVOICED') || '|' || COALESCE(pay.payment_key, 'NOT_PAID')) AS reconciliation_key,
    
    -- Order Information
    orders.order_id,
    orders.order_line,
    orders.order_number,
    orders.order_date,
    orders.order_quantity,
    orders.order_amount,
    orders.order_currency,
    orders.order_status,
    
    -- Customer (from staging enrichment)
    orders.customer_id,
    orders.customer_name,
    orders.customer_type,
    orders.customer_country,
    
    -- Invoice Information
    inv.invoice_id,
    inv.invoice_number,
    inv.invoice_date,
    inv.invoice_amount,
    inv.invoice_status,
    
    -- Payment terms (from staging enrichment)
    inv.payment_terms_code,
    inv.payment_terms_name,
    inv.payment_terms_days,
    inv.calculated_due_date AS due_date,
    
    -- Payment Information
    pay.payment_id,
    pay.payment_reference,
    pay.payment_date,
    pay.payment_amount,
    pay.payment_status,
    pay.cleared_flag,
    
    -- Bank (from staging enrichment)
    pay.bank_name,
    pay.bank_country,
    
    -- Calculated Metrics
    DATEDIFF('day', orders.order_date, inv.invoice_date) AS days_order_to_invoice,
    DATEDIFF('day', inv.invoice_date, pay.payment_date) AS days_invoice_to_payment,
    DATEDIFF('day', orders.order_date, pay.payment_date) AS days_order_to_cash,
    DATEDIFF('day', inv.calculated_due_date, COALESCE(pay.payment_date, CURRENT_DATE())) AS days_past_due,
    
    -- Reconciliation amounts
    (orders.order_amount - COALESCE(inv.invoice_amount, 0)) AS unbilled_amount,
    (COALESCE(inv.invoice_amount, 0) - COALESCE(pay.payment_amount, 0)) AS outstanding_amount,
    
    -- Status
    CASE
        WHEN inv.invoice_id IS NULL THEN 'NOT_INVOICED'
        WHEN pay.payment_id IS NULL THEN 'NOT_PAID'
        WHEN pay.cleared_flag = TRUE THEN 'CLOSED'
        ELSE 'OPEN'
    END AS reconciliation_status,
    
    CASE
        WHEN pay.payment_date IS NULL AND inv.calculated_due_date < CURRENT_DATE() THEN 'OVERDUE'
        WHEN pay.payment_date IS NULL THEN 'CURRENT'
        WHEN pay.payment_date <= inv.calculated_due_date THEN 'ON_TIME'
        ELSE 'LATE'
    END AS payment_timing,
    
    -- Row hash for change detection
    {{ row_hash([
        'COALESCE(inv.invoice_key, \'NOT_INVOICED\')',
        'COALESCE(pay.payment_key, \'NOT_PAID\')',
        'COALESCE(CAST(inv.invoice_amount AS VARCHAR), \'\')',
        'COALESCE(CAST(pay.payment_amount AS VARCHAR), \'\')'
    ]) }},
    
    -- Audit columns (incremental - preserves dbt_created_at)
    {{ audit_columns_incremental('existing') }}

FROM {{ ref('stg_enriched_orders') }} orders

LEFT JOIN {{ ref('stg_enriched_invoices') }} inv
    ON orders.order_key = inv.order_key

LEFT JOIN {{ ref('stg_enriched_payments') }} pay
    ON inv.invoice_key = pay.invoice_key

{% if is_incremental() %}
LEFT JOIN {{ this }} existing 
    ON MD5(orders.order_key || '|' || COALESCE(inv.invoice_key, 'NOT_INVOICED') || '|' || COALESCE(pay.payment_key, 'NOT_PAID')) = existing.reconciliation_key
{% endif %}
