{{
    config(
        materialized='table',
        schema='o2c_core',
        tags=['o2c', 'mart', 'core', 'reconciliation'],
        access='public'
    )
}}

/*
    Order-to-Cash Reconciliation Mart
    
    Complete O2C reconciliation joining all enriched staging models.
    Main mart for O2C analytics and semantic layer.
    
    Data Flow:
    1. stg_enriched_orders (orders + customer)
    2. stg_enriched_invoices (invoices + payment terms)
    3. stg_enriched_payments (payments + bank)
    4. JOIN all three together here
    
    Grain: One row per order-invoice-payment combination
*/

select
    -- Primary Keys
    orders.source_system,
    orders.company_code,
    orders.order_key,
    coalesce(inv.invoice_key, 'NOT_INVOICED') as invoice_key,
    coalesce(pay.payment_key, 'NOT_PAID') as payment_key,
    
    -- Order Information (from staging with customer)
    orders.order_id,
    orders.order_line,
    orders.order_number,
    orders.order_date,
    orders.order_quantity,
    orders.order_amount,
    orders.order_currency,
    orders.order_status,
    
    -- Customer (already enriched in staging)
    orders.customer_id,
    orders.customer_name,
    orders.customer_type,
    orders.customer_country,
    
    -- Invoice Information (from staging with payment terms)
    inv.invoice_id,
    inv.invoice_number,
    inv.invoice_date,
    inv.invoice_amount,
    inv.invoice_status,
    
    -- Payment terms (already enriched in staging)
    inv.payment_terms_code,
    inv.payment_terms_name,
    inv.payment_terms_days,
    inv.calculated_due_date as due_date,
    
    -- Payment Information (from staging with bank)
    pay.payment_id,
    pay.payment_reference,
    pay.payment_date,
    pay.payment_amount,
    pay.payment_status,
    pay.cleared_flag,
    
    -- Bank (already enriched in staging)
    pay.bank_name,
    pay.bank_country,
    
    -- Calculated Metrics
    datediff('day', orders.order_date, inv.invoice_date) as days_order_to_invoice,
    datediff('day', inv.invoice_date, pay.payment_date) as days_invoice_to_payment,
    datediff('day', orders.order_date, pay.payment_date) as days_order_to_cash,
    datediff('day', inv.calculated_due_date, coalesce(pay.payment_date, current_date())) as days_past_due,
    
    -- Reconciliation amounts
    (orders.order_amount - coalesce(inv.invoice_amount, 0)) as unbilled_amount,
    (inv.invoice_amount - coalesce(pay.payment_amount, 0)) as outstanding_amount,
    
    -- Status
    case
        when inv.invoice_id is null then 'NOT_INVOICED'
        when pay.payment_id is null then 'NOT_PAID'
        when pay.cleared_flag = true then 'CLOSED'
        else 'OPEN'
    end as reconciliation_status,
    
    case
        when pay.payment_date <= inv.calculated_due_date then 'ON_TIME'
        when pay.payment_date > inv.calculated_due_date then 'LATE'
        when current_date() > inv.calculated_due_date then 'OVERDUE'
        else 'CURRENT'
    end as payment_timing,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as loaded_at

from {{ ref('stg_enriched_orders') }} orders

left join {{ ref('stg_enriched_invoices') }} inv
    on orders.order_key = inv.order_key

left join {{ ref('stg_enriched_payments') }} pay
    on inv.invoice_key = pay.invoice_key

