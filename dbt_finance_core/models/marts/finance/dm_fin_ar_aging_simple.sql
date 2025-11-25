{{
    config(
        materialized='table',
        tags=['finance', 'ar', 'aging', 'data_mart']
    )
}}

/*
    Simplified AR Aging Data Mart
    
    Purpose: Smallest isolated branch implementation demonstrating:
    - Foundation dependency only (no lateral dependencies)
    - Use of shared dimensions from foundation
    - Use of shared macros from foundation
    - Complete data lineage from source to mart
    
    Dependencies:
    - dbt_foundation.stg_ar_invoice (staging)
    - dbt_foundation.dim_customer (shared dimension)
    - dbt_foundation.dim_fiscal_calendar (shared dimension)
    - dbt_foundation.aging_bucket() (shared macro)
*/

with ar_invoice as (

    -- Reference foundation staging model
    select * from {{ ref('dbt_foundation', 'stg_ar_invoice') }}

),

customer as (

    -- Reference foundation shared dimension
    select * from {{ ref('dbt_foundation', 'dim_customer') }}

),

fiscal_cal as (

    -- Reference foundation shared dimension
    select * from {{ ref('dbt_foundation', 'dim_fiscal_calendar') }}

),

-- First CTE: Calculate base data with days_late
base_data as (

    select
        -- Snapshot information
        {{ var('snapshot_date') }} as snapshot_date,
        
        -- Invoice keys
        ar.source_system,
        ar.company_code,
        ar.document_number,
        ar.document_line,
        ar.document_year,
        
        -- Customer information
        ar.customer_number,
        ar.customer_sk,
        cust.customer_name,
        cust.customer_type,
        cust.is_internal,
        
        -- Amounts
        ar.amt_usd_me,
        ar.amt_doc,
        ar.amt_lcl,
        ar.doc_currency,
        ar.local_currency,
        
        -- Dates
        ar.document_date,
        ar.posting_date,
        ar.net_due_date as due_date,
        
        -- Aging calculation (calculate here so it can be reused)
        datediff('day', ar.net_due_date, {{ var('snapshot_date') }}) as days_late,
        
        -- GL and organizational
        ar.gl_account,
        ar.profit_center,
        ar.sales_organization,
        
        -- Payment terms
        ar.payment_terms,
        ar.payment_terms_name,
        
        -- Analyst
        ar.credit_analyst_name,
        ar.credit_analyst_id,
        
        -- Fiscal period (from posting date)
        fc.fiscal_year_period_str as fiscal_year_period,
        fc.fiscal_year_int as fiscal_year,
        fc.fiscal_period_int as fiscal_period
        
    from ar_invoice ar
    
    left join customer cust
        on ar.customer_sk = cust.customer_num_sk
        and ar.source_system = cust.source_system
    
    left join fiscal_cal fc
        on to_char(ar.posting_date, 'YYYYMMDD') = fc.fiscal_day_key_str

),

-- Second CTE: Apply derived calculations using days_late
aging_calc as (

    select
        -- All base columns
        snapshot_date,
        source_system,
        company_code,
        document_number,
        document_line,
        document_year,
        customer_number,
        customer_sk,
        customer_name,
        customer_type,
        
        -- Customer type flag (derived from is_internal)
        case
            when is_internal then 'INTERNAL'
            else 'EXTERNAL'
        end as customer_type_flag,
        
        -- Amounts
        amt_usd_me,
        amt_doc,
        amt_lcl,
        doc_currency,
        local_currency,
        
        -- Dates
        document_date,
        posting_date,
        due_date,
        
        -- Days late (now available from base_data CTE)
        days_late,
        
        -- Aging bucket using foundation macro (now days_late is accessible)
        {{ aging_bucket('days_late') }} as aging_bucket,
        
        -- Past due flag
        case
            when days_late > 0 then 'YES'
            else 'NO'
        end as past_due_flag,
        
        -- Amount buckets
        case
            when days_late <= 0 then round(amt_usd_me, 2)
            else 0
        end as current_amt,
        
        case
            when days_late > 0 then round(amt_usd_me, 2)
            else 0
        end as past_due_amt,
        
        -- GL and organizational
        gl_account,
        profit_center,
        sales_organization,
        
        -- Payment terms
        payment_terms,
        payment_terms_name,
        
        -- Analyst
        credit_analyst_name,
        credit_analyst_id,
        
        -- Fiscal period
        fiscal_year_period,
        fiscal_year,
        fiscal_period,
        
        -- Metadata
        current_timestamp()::timestamp_ntz as loaded_at
        
    from base_data

)

select * from aging_calc

/*
    Data Quality Notes:
    - All amounts are in USD (month-end rate)
    - current_amt + past_due_amt should equal amt_usd_me
    - aging_bucket categories follow standard HON buckets
    - Only open invoices (clearing_date IS NULL in staging)
    - Only customer debits (account_type = 'D' in staging)
*/
