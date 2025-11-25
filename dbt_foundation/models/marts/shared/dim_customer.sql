{{
    config(
        materialized='table',
        tags=['shared', 'dimension', 'customer']
    )
}}

/*
    Shared customer dimension
    - Published to all domain projects (access: public)
    - Enforces schema contract
    - Adds business logic flags (is_internal)
*/

with source as (

    select * from {{ source('corp_master', 'dim_customer') }}

),

transformed as (

    select
        -- Composite primary key
        customer_num_sk || '|' || source_system as customer_id,
        
        -- Natural keys
        customer_num_sk,
        source_system,
        
        -- Customer attributes
        customer_name,
        customer_type,
        customer_type_name,
        customer_classification_name as customer_classification,
        customer_account_group,
        
        -- Location
        customer_country,
        customer_country_name,
        
        -- MDM attributes
        mdm_customer_duns_num as duns_number,
        mdm_customer_full_name,
        mdm_customer_global_ultimate_duns as global_ultimate_duns,
        mdm_customer_global_ultimate_name as global_ultimate_name,
        mdm_customer_global_ultimate_parent_name as global_ultimate_parent_name,
        ultimate_parent_source,
        
        -- Business logic flags
        case
            when customer_type = 'I' then true
            when upper(customer_name) like '%HONEYWELL%' then true
            when upper(customer_name) like '%ECLIPSE%' then true
            when upper(customer_name) like '%ELSTER%' then true
            else false
        end as is_internal,
        
        -- Metadata
        load_ts,
        update_ts,
        current_timestamp() as _loaded_at
        
    from source

)

select * from transformed

