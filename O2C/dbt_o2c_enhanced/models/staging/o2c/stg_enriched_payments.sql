{{
    config(
        materialized='view',
        snowflake_warehouse=get_warehouse(),
        tags=['staging', 'payments']
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
STG_ENRICHED_PAYMENTS - Payments with Bank Account Enrichment
═══════════════════════════════════════════════════════════════════════════════

Purpose: Join fact_payments with dim_bank_account for bank details
Pattern: VIEW (always current, no materialization)
Audit: Full audit columns (uniform across all models)

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Payment keys
    pay.source_system,
    pay.company_code,
    pay.payment_id,
    pay.source_system || '|' || pay.payment_id AS payment_key,
    pay.invoice_id,
    pay.invoice_line,
    pay.source_system || '|' || pay.invoice_id || '|' || COALESCE(pay.invoice_line, 0) AS invoice_key,
    
    -- Payment details
    pay.payment_reference,
    pay.payment_date,
    pay.clearing_date,
    pay.payment_amount_lcl AS payment_amount,
    pay.currency_code AS payment_currency,
    pay.payment_status,
    pay.cleared_flag,
    
    -- Bank info (from JOIN)
    pay.bank_account_id,
    bank.bank_name,
    bank.bank_country,
    bank.bank_account_type,
    
    -- Audit columns (uniform set)
    {{ audit_columns() }}

FROM {{ source('corp_tran', 'FACT_PAYMENTS') }} pay

LEFT JOIN {{ source('corp_master', 'DIM_BANK_ACCOUNT') }} bank
    ON pay.bank_account_id = bank.bank_account_id
    AND pay.source_system = bank.source_system

WHERE pay.payment_date >= DATEADD('year', -2, CURRENT_DATE())
