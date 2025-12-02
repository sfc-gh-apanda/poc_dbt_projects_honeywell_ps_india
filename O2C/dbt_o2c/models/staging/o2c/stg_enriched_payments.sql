{{
    config(
        materialized='view',
        tags=['o2c', 'staging', 'payments']
    )
}}

/*
    O2C Enriched Payments Staging
    
    Joins payments with bank account master.
    Provides bank details for payment analysis.
*/

select
    -- Keys
    pay.source_system,
    pay.company_code,
    pay.payment_id,
    pay.source_system || '|' || pay.payment_id as payment_key,
    pay.invoice_id,
    pay.invoice_line,
    pay.source_system || '|' || pay.invoice_id || '|' || coalesce(pay.invoice_line, 0) as invoice_key,
    
    -- Payment details
    pay.payment_reference,
    pay.payment_date,
    pay.clearing_date,
    pay.payment_amount_lcl as payment_amount,
    pay.currency_code as payment_currency,
    pay.payment_status,
    pay.cleared_flag,
    
    -- Bank info (from JOIN)
    pay.bank_account_id,
    bank.bank_name,
    bank.bank_country,
    bank.bank_account_type,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as _dbt_loaded_at

from {{ source('o2c_transactions', 'fact_payments') }} pay

left join {{ source('o2c_master_data', 'dim_bank_account') }} bank
    on pay.bank_account_id = bank.bank_account_id
    and pay.source_system = bank.source_system

where pay.payment_date >= dateadd('year', -2, current_date())

