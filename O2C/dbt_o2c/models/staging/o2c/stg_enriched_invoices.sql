{{
    config(
        materialized='view',
        tags=['o2c', 'staging', 'invoices']
    )
}}

/*
    O2C Enriched Invoices Staging
    
    Joins invoices with payment terms master.
    Calculates due dates based on payment terms.
*/

select
    -- Keys
    inv.source_system,
    inv.company_code,
    inv.invoice_id,
    inv.invoice_line,
    inv.source_system || '|' || inv.invoice_id || '|' || inv.invoice_line as invoice_key,
    inv.order_id,
    inv.order_line,
    inv.source_system || '|' || inv.order_id || '|' || inv.order_line as order_key,
    
    -- Invoice details
    inv.invoice_number,
    inv.invoice_date,
    inv.posting_date,
    inv.invoice_quantity,
    inv.invoice_amount_lcl as invoice_amount,
    inv.currency_code as invoice_currency,
    inv.invoice_status,
    
    -- Payment terms (from JOIN)
    inv.payment_terms_code,
    pt.payment_terms_name,
    pt.payment_terms_days,
    
    -- Calculated due date
    dateadd('day', coalesce(pt.payment_terms_days, 30), inv.invoice_date) as calculated_due_date,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as _dbt_loaded_at

from {{ source('o2c_transactions', 'fact_invoices') }} inv

left join {{ source('o2c_master_data', 'dim_payment_terms') }} pt
    on inv.payment_terms_code = pt.payment_terms_code
    and inv.source_system = pt.source_system

where inv.invoice_date >= dateadd('year', -2, current_date())

