{#
═══════════════════════════════════════════════════════════════════════════════
SNAPSHOT: CUSTOMER (SCD-2 with Soft Delete)
═══════════════════════════════════════════════════════════════════════════════

Purpose:
  Captures historical changes to customer dimension using SCD-2 pattern.
  When a record is deleted from source, it's marked as ended (soft delete),
  not physically removed.

Key Features:
  • strategy='check' - Compares multiple columns for changes
  • invalidate_hard_deletes=True - Soft delete when record disappears
  • Automatic columns: dbt_scd_id, dbt_valid_from, dbt_valid_to, dbt_updated_at

Execution:
  dbt snapshot --select snap_customer
  
  Or in Snowflake Native dbt:
  EXECUTE DBT PROJECT dbt_o2c_enhanced ARGS = 'snapshot --select snap_customer';

═══════════════════════════════════════════════════════════════════════════════
#}

{% snapshot snap_customer %}

{{
    config(
        target_database='EDW',
        target_schema='O2C_ENHANCED_SNAPSHOTS',
        unique_key='customer_num_sk',
        strategy='check',
        check_cols=[
            'customer_name',
            'customer_type',
            'customer_country',
            'customer_country_name',
            'customer_classification',
            'customer_account_group',
            'duns_number',
            'mdm_customer_global_ultimate_duns',
            'mdm_customer_global_ultimate_name',
            'mdm_customer_full_name'
        ],
        invalidate_hard_deletes=True
    )
}}

SELECT
    -- Primary Key
    customer_num_sk,
    source_system,
    
    -- Business Attributes (tracked for changes)
    customer_name,
    customer_type,
    customer_country,
    customer_country_name,
    customer_classification,
    customer_account_group,
    
    -- MDM Attributes
    duns_number,
    mdm_customer_global_ultimate_duns,
    mdm_customer_global_ultimate_name,
    mdm_customer_full_name,
    
    -- Source system active flag
    TRUE AS source_is_active,
    
    -- Source timestamps
    load_ts AS source_load_ts,
    update_ts AS source_update_ts,
    
    -- Snapshot metadata
    '{{ invocation_id }}' AS dbt_snapshot_run_id,
    CURRENT_TIMESTAMP() AS dbt_snapshot_at

FROM {{ source('corp_master', 'DIM_CUSTOMER') }}

WHERE customer_num_sk IS NOT NULL

{% endsnapshot %}
