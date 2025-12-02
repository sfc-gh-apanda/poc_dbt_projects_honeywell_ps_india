{{
    config(
        materialized='dynamic_table',
        target_lag='1 hour',
        snowflake_warehouse='COMPUTE_WH',
        schema='o2c_semantic_views',
        tags=['o2c', 'semantic_view', 'ar_aging']
    )
}}

/*
    O2C AR Aging Semantic View
    
    AR aging analysis with standard aging buckets.
    Materialized as Snowflake DYNAMIC TABLE.
    
    Source: dm_o2c_reconciliation
    Refresh: Every 1 hour
*/

select
    -- Dimensions
    source_system,
    customer_id,
    customer_name,
    customer_type,
    customer_country,
    
    -- Current date for aging calculation
    current_date() as aging_as_of_date,
    
    -- Aging buckets (based on days past due)
    sum(case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') and days_past_due <= 0 
        then outstanding_amount 
        else 0 
    end) as current_amount,
    
    sum(case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') and days_past_due between 1 and 30 
        then outstanding_amount 
        else 0 
    end) as past_due_1_30_days,
    
    sum(case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') and days_past_due between 31 and 60 
        then outstanding_amount 
        else 0 
    end) as past_due_31_60_days,
    
    sum(case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') and days_past_due between 61 and 90 
        then outstanding_amount 
        else 0 
    end) as past_due_61_90_days,
    
    sum(case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') and days_past_due > 90 
        then outstanding_amount 
        else 0 
    end) as past_due_over_90_days,
    
    -- Total outstanding
    sum(case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') 
        then outstanding_amount 
        else 0 
    end) as total_ar_outstanding,
    
    -- Invoice counts by bucket
    count(distinct case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') and days_past_due <= 0 
        then invoice_key 
    end) as current_invoices,
    
    count(distinct case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') and days_past_due between 1 and 30 
        then invoice_key 
    end) as past_due_1_30_invoices,
    
    count(distinct case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') and days_past_due between 31 and 60 
        then invoice_key 
    end) as past_due_31_60_invoices,
    
    count(distinct case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') and days_past_due between 61 and 90 
        then invoice_key 
    end) as past_due_61_90_invoices,
    
    count(distinct case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') and days_past_due > 90 
        then invoice_key 
    end) as past_due_over_90_invoices,
    
    -- Average days past due
    round(avg(case 
        when reconciliation_status in ('NOT_PAID', 'OPEN') 
        then days_past_due 
    end), 1) as weighted_avg_days_past_due,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as view_refreshed_at

from {{ ref('dm_o2c_reconciliation') }}

group by
    source_system,
    customer_id,
    customer_name,
    customer_type,
    customer_country
    
having total_ar_outstanding > 0  -- Only customers with outstanding AR

