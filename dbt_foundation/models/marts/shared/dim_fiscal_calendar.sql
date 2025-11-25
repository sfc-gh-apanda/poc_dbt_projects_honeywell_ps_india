{{
    config(
        materialized='table',
        tags=['shared', 'dimension', 'fiscal_calendar']
    )
}}

/*
    Shared fiscal calendar dimension
    - Published to all domain projects (access: public)
    - Enforces schema contract
    - Provides fiscal period calculations
*/

with source as (

    select * from {{ source('corp_ref', 'time_fiscal_day') }}

),

transformed as (

    select
        -- Primary key
        fiscal_day_key_str,
        
        -- Date
        fiscal_date_key_date as fiscal_date,
        
        -- Fiscal year
        fiscal_year_str,
        fiscal_year_int,
        
        -- Fiscal period
        fiscal_period_str,
        fiscal_period_int,
        
        -- Fiscal year-period
        fiscal_year_period_str,
        fiscal_year_period_int,
        
        -- Fiscal quarter
        fiscal_year_quarter_str,
        
        -- Calendar attributes (for reference)
        calendar_year,
        calendar_month,
        calendar_day,
        day_of_week,
        
        -- Metadata
        load_ts,
        current_timestamp()::timestamp_ntz as _loaded_at
        
    from source

)

select * from transformed

