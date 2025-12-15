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
        
        -- Strategy: Compare columns to detect changes
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
        
        -- CRITICAL: Soft delete - when record disappears, close it, don't delete
        invalidate_hard_deletes=True
    )
}}

SELECT
    -- ═══════════════════════════════════════════════════════════════
    -- PRIMARY KEY
    -- ═══════════════════════════════════════════════════════════════
    customer_num_sk,
    source_system,
    
    -- ═══════════════════════════════════════════════════════════════
    -- BUSINESS ATTRIBUTES (tracked for changes)
    -- ═══════════════════════════════════════════════════════════════
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
    
    -- ═══════════════════════════════════════════════════════════════
    -- SOURCE SYSTEM ACTIVE FLAG (if available)
    -- ═══════════════════════════════════════════════════════════════
    -- If your source has an active/inactive flag, include it here
    -- COALESCE(is_active, TRUE) AS source_is_active,
    TRUE AS source_is_active,  -- Default to active if no flag in source
    
    -- ═══════════════════════════════════════════════════════════════
    -- SOURCE TIMESTAMPS
    -- ═══════════════════════════════════════════════════════════════
    load_ts AS source_load_ts,
    update_ts AS source_update_ts,
    
    -- ═══════════════════════════════════════════════════════════════
    -- SNAPSHOT METADATA
    -- ═══════════════════════════════════════════════════════════════
    '{{ invocation_id }}' AS dbt_snapshot_run_id,
    CURRENT_TIMESTAMP() AS dbt_snapshot_at

FROM {{ source('corp_master', 'DIM_CUSTOMER') }}

WHERE customer_num_sk IS NOT NULL

{% endsnapshot %}

