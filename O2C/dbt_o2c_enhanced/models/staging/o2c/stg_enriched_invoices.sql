{{
    config(
        materialized='view',
        pre_hook="{{ switch_warehouse() }}",
        tags=['staging', 'invoices'],
        query_tag='dbt_stg_enriched_invoices'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
STG_ENRICHED_INVOICES - Invoices with Payment Terms Enrichment
═══════════════════════════════════════════════════════════════════════════════

Purpose: Join fact_invoices with dim_payment_terms for payment details
Pattern: VIEW (always current, no materialization)
Audit: Full audit columns (uniform across all models)

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Invoice keys
    inv.source_system,
    inv.company_code,
    inv.invoice_id,
    inv.invoice_line,
    inv.source_system || '|' || inv.invoice_id || '|' || inv.invoice_line AS invoice_key,
    inv.order_id,
    inv.order_line,
    inv.source_system || '|' || inv.order_id || '|' || inv.order_line AS order_key,
    
    -- Invoice details
    inv.invoice_number,
    inv.invoice_date,
    inv.posting_date,
    inv.invoice_quantity,
    inv.invoice_amount_lcl AS invoice_amount,
    inv.currency_code AS invoice_currency,
    inv.invoice_status,
    
    -- Payment terms (from JOIN)
    inv.payment_terms_code,
    pt.payment_terms_name,
    pt.payment_terms_days,
    
    -- Calculated due date
    DATEADD('day', COALESCE(pt.payment_terms_days, 30), inv.invoice_date) AS calculated_due_date,
    
    -- Audit columns (uniform set)
    {{ audit_columns() }}

FROM {{ source('corp_tran', 'FACT_INVOICES') }} inv

LEFT JOIN {{ source('corp_master', 'DIM_PAYMENT_TERMS') }} pt
    ON inv.payment_terms_code = pt.payment_terms_code
    AND inv.source_system = pt.source_system

WHERE inv.invoice_date >= DATEADD('year', -2, CURRENT_DATE())
