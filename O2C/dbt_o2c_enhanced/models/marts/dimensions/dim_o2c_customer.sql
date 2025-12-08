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

WITH source_customers AS (
    SELECT
        source_system,
        customer_num_sk,
        customer_name,
        customer_type,
        customer_country,
        customer_region,
        credit_limit,
        payment_terms_code,
        load_ts AS source_load_ts,
        update_ts AS source_update_ts
    FROM {{ source('corp_master', 'DIM_CUSTOMER') }}
)

SELECT
    -- Surrogate key
    {{ hash_key(['source_system', 'customer_num_sk'], 'customer_key') }},
    
    -- Natural key
    source_system,
    customer_num_sk AS customer_id,
    
    -- Attributes
    customer_name,
    customer_type,
    customer_country,
    customer_region,
    credit_limit,
    payment_terms_code,
    
    -- Source tracking
    source_load_ts,
    source_update_ts,
    
    -- Row hash for downstream change detection
    {{ row_hash(['customer_name', 'customer_type', 'customer_country', 'credit_limit']) }},
    
    -- Full audit columns
    {{ audit_columns() }}

FROM source_customers


