{% set reload_src = var('reload_source', 'ALL') %}

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['order_key', 'source_system'],
        on_schema_change='append_new_columns',
        tags=['partitioned', 'pre_hook_delete', 'source_reload', 'pattern_example'],
        query_tag='dbt_fact_o2c_by_source'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 5: DELETE+INSERT BY SOURCE (Source-specific reload)
═══════════════════════════════════════════════════════════════════════════════

Description:
  - Use delete+insert strategy with source system filter
  - Only rows matching the source in the new data get deleted
  - Then fresh data for that source is inserted
  - Ideal for source-system-specific reloads

Configuration:
  - var('reload_source'): Source system to reload (default: 'ALL')
  - Run with: dbt run --select fact_o2c_by_source --vars '{"reload_source": "BRP900"}'

Testing This Pattern:
  1. First run: dbt run --select fact_o2c_by_source (loads all sources)
  2. Run for specific source: dbt run --select fact_o2c_by_source --vars '{"reload_source": "BRP900"}'
  3. Verify:
     - BRP900 records: Deleted and re-inserted (new dbt_loaded_at)
     - Other sources: UNCHANGED

Audit: Full audit columns (uniform set - delete+insert so created = updated)

═══════════════════════════════════════════════════════════════════════════════
#}

WITH source_data AS (
    SELECT
        source_system,
        company_code,
        order_key,
        order_id,
        order_date,
        customer_id,
        customer_name,
        order_amount,
        order_currency,
        order_status,
        sales_org,
        profit_center
    FROM {{ ref('stg_enriched_orders') }}
    
    -- Filter to specific source if specified
    {% if var('reload_source', 'ALL') != 'ALL' %}
    WHERE source_system = '{{ var("reload_source") }}'
    {% endif %}
)

SELECT
    s.source_system,
    s.company_code,
    s.order_key,
    s.order_id,
    s.order_date,
    s.customer_id,
    s.customer_name,
    s.order_amount,
    s.order_currency,
    s.order_status,
    s.sales_org,
    s.profit_center,
    
    -- Audit columns (uniform set - pre-hook delete so created = updated)
    {{ audit_columns() }}

FROM source_data s

{% if is_incremental() and var('reload_source', 'ALL') == 'ALL' %}
-- If reloading ALL, only get new records
WHERE NOT EXISTS (
    SELECT 1 FROM {{ this }} t
    WHERE t.order_key = s.order_key
      AND t.source_system = s.source_system
)
{% endif %}
