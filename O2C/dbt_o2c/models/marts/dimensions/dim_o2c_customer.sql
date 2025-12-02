{{
    config(
        materialized='table',
        schema='o2c_dimensions',
        tags=['o2c', 'dimension', 'customer'],
        access='public',
        contract={
            'enforced': true
        }
    )
}}

/*
    O2C Customer Dimension
    
    Published customer dimension for O2C domain.
    Enforces schema contract for downstream consumers.
    
    Source: DIM_CUSTOMER (master data)
    Grain: One row per customer per source system
*/

select
    -- Primary key
    customer_num_sk || '|' || source_system as customer_id,
    
    -- Natural keys
    customer_num_sk,
    source_system,
    
    -- Attributes
    customer_name,
    customer_type,
    customer_country,
    customer_country_name,
    customer_classification,
    customer_account_group,
    
    -- MDM attributes
    duns_number,
    mdm_customer_global_ultimate_duns as global_ultimate_duns,
    mdm_customer_global_ultimate_name as global_ultimate_name,
    mdm_customer_full_name as full_name,
    
    -- Derived flags
    case when customer_type = 'I' then true else false end as is_internal,
    
    -- Metadata
    load_ts as source_load_ts,
    update_ts as source_update_ts,
    current_timestamp()::timestamp_ntz as _loaded_at

from {{ source('o2c_master_data', 'dim_customer') }}

where customer_num_sk is not null

