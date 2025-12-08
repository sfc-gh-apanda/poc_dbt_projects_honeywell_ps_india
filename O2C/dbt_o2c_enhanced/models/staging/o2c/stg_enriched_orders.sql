{{
    config(
        materialized='view',
        tags=['staging', 'orders']
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
STG_ENRICHED_ORDERS - Orders with Customer Enrichment
═══════════════════════════════════════════════════════════════════════════════

Purpose: Join fact_sales_orders with dim_customer for customer details
Pattern: VIEW (always current, no materialization)
Audit: Minimal audit columns (run_id, loaded_at)

═══════════════════════════════════════════════════════════════════════════════
#}

WITH orders AS (
    SELECT
        source_system,
        company_code,
        order_id,
        order_line,
        customer_id,
        order_date,
        requested_delivery_date,
        order_type,
        order_status,
        product_id,
        quantity,
        unit_price,
        order_amount_lcl,
        order_amount_usd,
        currency_code,
        sales_org,
        distribution_channel,
        profit_center,
        cost_center,
        created_by,
        created_date
    FROM {{ source('corp_tran', 'FACT_SALES_ORDERS') }}
),

customers AS (
    SELECT
        source_system,
        customer_num_sk,
        customer_name,
        customer_type,
        customer_country,
        customer_region,
        credit_limit,
        payment_terms_code AS customer_payment_terms
    FROM {{ source('corp_master', 'DIM_CUSTOMER') }}
)

SELECT
    -- Order keys
    o.source_system,
    o.company_code,
    o.order_id,
    o.order_line,
    
    -- Generate surrogate key
    {{ hash_key(['o.source_system', 'o.order_id', 'o.order_line'], 'order_key') }},
    
    -- Order details
    o.order_date,
    o.requested_delivery_date,
    o.order_type,
    o.order_status,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.order_amount_lcl,
    o.order_amount_usd AS order_amount,
    o.currency_code,
    
    -- Customer info (from enrichment)
    o.customer_id,
    c.customer_name,
    c.customer_type,
    c.customer_country,
    c.customer_region,
    c.credit_limit,
    c.customer_payment_terms,
    
    -- Organizational
    o.sales_org,
    o.distribution_channel,
    o.profit_center,
    o.cost_center,
    o.created_by,
    o.created_date,
    
    -- Audit columns (minimal for views)
    {{ audit_columns_minimal() }}

FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_num_sk
    AND o.source_system = c.source_system


