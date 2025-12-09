{{
    config(
        materialized='table',
        tags=['aggregate', 'truncate_load']
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
CUSTOMER AGGREGATE - Truncate & Load Pattern
═══════════════════════════════════════════════════════════════════════════════

Audit: Full audit columns (uniform set - dbt_created_at = dbt_updated_at for full refresh)
#}

WITH customer_metrics AS (
    SELECT
        source_system,
        customer_id,
        customer_name,
        customer_type,
        customer_country,
        
        -- Volume metrics
        COUNT(DISTINCT order_key) AS total_orders,
        COUNT(DISTINCT CASE WHEN invoice_key != 'NOT_INVOICED' THEN invoice_key END) AS total_invoices,
        COUNT(DISTINCT CASE WHEN payment_key != 'NOT_PAID' THEN payment_key END) AS total_payments,
        
        -- Value metrics
        SUM(order_amount) AS total_order_value,
        SUM(invoice_amount) AS total_invoice_value,
        SUM(payment_amount) AS total_payment_value,
        SUM(outstanding_amount) AS total_outstanding,
        
        -- Performance metrics
        AVG(days_order_to_cash) AS avg_dso,
        MEDIAN(days_order_to_cash) AS median_dso,
        MAX(days_order_to_cash) AS max_dso,
        
        -- Payment behavior
        SUM(CASE WHEN payment_timing = 'ON_TIME' THEN 1 ELSE 0 END) AS on_time_payments,
        SUM(CASE WHEN payment_timing = 'LATE' THEN 1 ELSE 0 END) AS late_payments,
        SUM(CASE WHEN payment_timing = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_count,
        
        -- Date range
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
        
    FROM {{ ref('dm_o2c_reconciliation') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    cm.*,
    
    -- Calculated rates
    ROUND(cm.total_invoices * 100.0 / NULLIF(cm.total_orders, 0), 2) AS billing_rate_pct,
    ROUND(cm.total_payments * 100.0 / NULLIF(cm.total_invoices, 0), 2) AS collection_rate_pct,
    ROUND(cm.on_time_payments * 100.0 / NULLIF(cm.total_payments, 0), 2) AS on_time_payment_pct,
    
    -- Customer classification
    CASE
        WHEN cm.avg_dso <= 30 THEN 'EXCELLENT'
        WHEN cm.avg_dso <= 60 THEN 'GOOD'
        WHEN cm.avg_dso <= 90 THEN 'FAIR'
        ELSE 'POOR'
    END AS payment_behavior,
    
    -- Audit columns (uniform set - full refresh so created = updated)
    {{ audit_columns() }}

FROM customer_metrics cm
