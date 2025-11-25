{{
    config(
        materialized='view',
        tags=['staging', 'ar', 'invoice']
    )
}}

/*
    Staging model for AR invoices
    - Selects only open receivables (clearing_date IS NULL)
    - Filters to debits only (account_type = 'D')
    - Excludes special GL indicators
    - Light transformations and renaming for consistency
*/

with source as (

    select * from {{ source('corp_tran', 'fact_account_receivable_gbl') }}
    
    where 1=1
        -- Only source systems we support
        and source_system in ({{ "'" + var('source_systems') | join("','") + "'" }})
        -- Only open items (unpaid invoices)
        and clearing_date is null
        -- Only debits (receivables, not credits)
        and account_type = 'D'
        -- Exclude special GL indicators (down payments, guarantees, etc.)
        and (special_gl_indicator is null or special_gl_indicator not in ('A', 'F', 'G'))

),

renamed as (

    select
        -- Primary keys
        source_system,
        company_code,
        accounting_doc as document_number,
        account_doc_line_item as document_line,
        fiscal_year as document_year,
        
        -- Amounts
        amt_doc,
        amt_usd,
        amt_usd_me,
        amt_lcl,
        
        -- Currency
        doc_curr as doc_currency,
        lcl_curr as local_currency,
        
        -- Dates
        doc_date as document_date,
        posting_date,
        net_due_date,
        baseline_date,
        clearing_date,
        
        -- Customer
        sold_to as customer_number,
        customer_num_sk as customer_sk,
        
        -- GL and organizational
        gl_account,
        sub_gl_account,
        profit_center,
        sales_org as sales_organization,
        
        -- Payment terms
        payment_terms,
        payment_terms_name,
        payment_transaction as payment_index,
        
        -- Document information
        accounting_doc_type as document_type_sk,
        accounting_doc_type_name as doc_type_desc,
        posting_key,
        posting_key_name,
        
        -- References
        ref_doc_num,
        ref_transaction as reference_transaction,
        residual_doc as invoice_ref,
        
        -- Other attributes
        account_type,
        special_gl_indicator,
        balance_type,
        assignment,
        reason_code,
        
        -- Analyst
        credit_analyst_name,
        credit_analyst_id,
        
        -- Source organization
        source_org,
        
        -- Metadata
        load_ts,
        update_ts,
        current_timestamp()::timestamp_ntz as _stg_loaded_at
        
    from source

)

select * from renamed

