{{
    config(
        materialized='table',
        tags=['dimension', 'truncate_load', 'pattern_example'],
        contract={'enforced': true}
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 1: TRUNCATE & LOAD (materialized='table')
═══════════════════════════════════════════════════════════════════════════════

Description:
  - Full table replacement on every dbt run
  - Entire table is dropped and recreated
  - Simplest pattern, ideal for dimension tables

When to Use:
  ✅ Small to medium tables (< 1M rows)
  ✅ Dimension tables that need full refresh
  ✅ When source doesn't have reliable change tracking
  ✅ When data integrity is paramount

How It Works:
  1. dbt creates: dim_o2c_customer__dbt_tmp
  2. dbt drops: dim_o2c_customer
  3. dbt renames: dim_o2c_customer__dbt_tmp → dim_o2c_customer

Testing This Pattern:
  1. Run: dbt run --select dim_o2c_customer
  2. Check row count before/after
  3. Verify dbt_loaded_at is current timestamp
  4. All records have same dbt_run_id

═══════════════════════════════════════════════════════════════════════════════
#}

SELECT
    -- Surrogate key
    customer_num_sk || '|' || source_system AS customer_key,
    
    -- Natural key
    source_system,
    customer_num_sk AS customer_id,
    
    -- Attributes
    customer_name,
    customer_type,
    customer_country,
    customer_country_name,
    customer_classification,
    customer_account_group,
    
    -- MDM attributes
    duns_number,
    mdm_customer_global_ultimate_duns AS global_ultimate_duns,
    mdm_customer_global_ultimate_name AS global_ultimate_name,
    mdm_customer_full_name AS full_name,
    
    -- Derived flags
    CASE WHEN customer_type = 'I' THEN TRUE ELSE FALSE END AS is_internal,
    
    -- Source tracking
    load_ts AS source_load_ts,
    update_ts AS source_update_ts,
    
    -- Row hash for downstream change detection
    MD5(COALESCE(customer_name, '') || '|' || COALESCE(customer_type, '') || '|' || COALESCE(customer_country, '')) AS dbt_row_hash,
    
    -- Audit columns
    '{{ invocation_id }}' AS dbt_run_id,
    MD5('{{ invocation_id }}' || '|' || customer_num_sk) AS dbt_batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_loaded_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS dbt_updated_at

FROM {{ source('corp_master', 'DIM_CUSTOMER') }}

WHERE customer_num_sk IS NOT NULL
