{{
    config(
        materialized='view',
        tags=['staging', 'invoices']
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
STG_ENRICHED_INVOICES - Invoices with Payment Terms Enrichment
═══════════════════════════════════════════════════════════════════════════════
#}

WITH invoices AS (
    SELECT
        source_system,
        company_code,
        invoice_id,
        invoice_line,
        order_id,
        order_line,
        customer_id,
        invoice_date,
        due_date,
        payment_terms_code,
        invoice_type,
        invoice_status,
        invoice_amount_lcl,
        invoice_amount_usd,
        tax_amount_lcl,
        currency_code,
        gl_account,
        profit_center,
        cost_center,
        created_by,
        created_date
    FROM {{ source('corp_tran', 'FACT_INVOICES') }}
),

payment_terms AS (
    SELECT
        source_system,
        payment_terms_code,
        payment_terms_name,
        payment_terms_days,
        discount_percent,
        discount_days
    FROM {{ source('corp_master', 'DIM_PAYMENT_TERMS') }}
)

SELECT
    -- Invoice keys
    i.source_system,
    i.company_code,
    i.invoice_id,
    i.invoice_line,
    
    -- Generate surrogate key
    {{ hash_key(['i.source_system', 'i.invoice_id', 'i.invoice_line'], 'invoice_key') }},
    
    -- Related order
    i.order_id,
    i.order_line,
    {{ hash_key(['i.source_system', 'i.order_id', 'i.order_line'], 'order_key') }},
    
    -- Invoice details
    i.customer_id,
    i.invoice_date,
    i.due_date,
    i.invoice_type,
    i.invoice_status,
    i.invoice_amount_lcl,
    i.invoice_amount_usd AS invoice_amount,
    i.tax_amount_lcl,
    i.currency_code,
    
    -- Payment terms (from enrichment)
    i.payment_terms_code,
    pt.payment_terms_name,
    pt.payment_terms_days,
    pt.discount_percent,
    pt.discount_days,
    
    -- Calculated: Due date from terms if not provided
    COALESCE(i.due_date, DATEADD(day, COALESCE(pt.payment_terms_days, 30), i.invoice_date)) AS calculated_due_date,
    
    -- Organizational
    i.gl_account,
    i.profit_center,
    i.cost_center,
    i.created_by,
    i.created_date,
    
    -- Audit columns
    {{ audit_columns_minimal() }}

FROM invoices i
LEFT JOIN payment_terms pt
    ON i.payment_terms_code = pt.payment_terms_code
    AND i.source_system = pt.source_system


