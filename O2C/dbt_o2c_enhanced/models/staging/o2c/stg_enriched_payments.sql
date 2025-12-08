{{
    config(
        materialized='view',
        tags=['staging', 'payments']
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
STG_ENRICHED_PAYMENTS - Payments with Bank Account Enrichment
═══════════════════════════════════════════════════════════════════════════════
#}

WITH payments AS (
    SELECT
        source_system,
        company_code,
        payment_id,
        invoice_id,
        invoice_line,
        payment_reference,
        payment_date,
        clearing_date,
        value_date,
        payment_method_code,
        payment_type,
        payment_amount_lcl,
        payment_amount_usd,
        discount_amount_lcl,
        currency_code,
        bank_account_id,
        payment_status,
        cleared_flag,
        reconciled_flag,
        reversed_flag,
        gl_account,
        profit_center,
        cost_center,
        created_by,
        created_date
    FROM {{ source('corp_tran', 'FACT_PAYMENTS') }}
),

bank_accounts AS (
    SELECT
        source_system,
        bank_account_id,
        bank_name,
        bank_country,
        account_type,
        currency_code AS bank_currency
    FROM {{ source('corp_master', 'DIM_BANK_ACCOUNT') }}
)

SELECT
    -- Payment keys
    p.source_system,
    p.company_code,
    p.payment_id,
    
    -- Generate surrogate key
    {{ hash_key(['p.source_system', 'p.payment_id'], 'payment_key') }},
    
    -- Related invoice
    p.invoice_id,
    p.invoice_line,
    {{ hash_key(['p.source_system', 'p.invoice_id', 'p.invoice_line'], 'invoice_key') }},
    
    -- Payment details
    p.payment_reference,
    p.payment_date,
    p.clearing_date,
    p.value_date,
    p.payment_method_code,
    p.payment_type,
    p.payment_amount_lcl,
    p.payment_amount_usd AS payment_amount,
    p.discount_amount_lcl,
    p.currency_code,
    p.payment_status,
    p.cleared_flag,
    p.reconciled_flag,
    p.reversed_flag,
    
    -- Bank info (from enrichment)
    p.bank_account_id,
    ba.bank_name,
    ba.bank_country,
    ba.account_type AS bank_account_type,
    
    -- Organizational
    p.gl_account,
    p.profit_center,
    p.cost_center,
    p.created_by,
    p.created_date,
    
    -- Audit columns
    {{ audit_columns_minimal() }}

FROM payments p
LEFT JOIN bank_accounts ba
    ON p.bank_account_id = ba.bank_account_id
    AND p.source_system = ba.source_system


