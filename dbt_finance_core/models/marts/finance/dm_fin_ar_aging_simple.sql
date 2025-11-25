{{
    config(
        materialized='table',
        tags=['finance', 'ar', 'aging', 'data_mart']
    )
}}

/*
    Simplified AR Aging Data Mart
    
    Fully inlined version for Snowflake native DBT compatibility
    No macros, no complex CTEs, no variables
*/

select
    -- Snapshot information
    current_date() as snapshot_date,
    
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
    case
        when cust.is_internal then 'INTERNAL'
        else 'EXTERNAL'
    end as customer_type_flag,
    
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
    
    -- Aging calculation (inline)
    datediff('day', ar.net_due_date, current_date()) as days_late,
    
    -- Aging bucket (inline CASE - no macro)
    case
        when datediff('day', ar.net_due_date, current_date()) <= 0 then 'CURRENT'
        when datediff('day', ar.net_due_date, current_date()) between 1 and 30 then '1-30'
        when datediff('day', ar.net_due_date, current_date()) between 31 and 60 then '31-60'
        when datediff('day', ar.net_due_date, current_date()) between 61 and 90 then '61-90'
        when datediff('day', ar.net_due_date, current_date()) between 91 and 120 then '91-120'
        when datediff('day', ar.net_due_date, current_date()) between 121 and 150 then '121-150'
        when datediff('day', ar.net_due_date, current_date()) between 151 and 180 then '151-180'
        when datediff('day', ar.net_due_date, current_date()) between 181 and 360 then '181-360'
        else '361+'
    end as aging_bucket,
    
    -- Past due flag
    case
        when datediff('day', ar.net_due_date, current_date()) > 0 then 'YES'
        else 'NO'
    end as past_due_flag,
    
    -- Amount buckets
    case
        when datediff('day', ar.net_due_date, current_date()) <= 0 then round(ar.amt_usd_me, 2)
        else 0
    end as current_amt,
    
    case
        when datediff('day', ar.net_due_date, current_date()) > 0 then round(ar.amt_usd_me, 2)
        else 0
    end as past_due_amt,
    
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
    
    -- Fiscal period
    fc.fiscal_year_period_str as fiscal_year_period,
    fc.fiscal_year_int as fiscal_year,
    fc.fiscal_period_int as fiscal_period,
    
    -- Metadata
    current_timestamp()::timestamp_ntz as loaded_at
    
from {{ source('foundation_staging', 'stg_ar_invoice') }} ar

left join {{ source('foundation_shared', 'dim_customer') }} cust
    on ar.customer_sk = cust.customer_num_sk
    and ar.source_system = cust.source_system

left join {{ source('foundation_shared', 'dim_fiscal_calendar') }} fc
    on to_char(ar.posting_date, 'YYYYMMDD') = fc.fiscal_day_key_str
