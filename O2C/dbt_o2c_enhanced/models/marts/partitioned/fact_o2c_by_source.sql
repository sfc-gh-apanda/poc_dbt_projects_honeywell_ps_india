{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        
        -- Pre-hook: Delete specific source before insert
        pre_hook=[
            "DELETE FROM {{ this }} WHERE source_system = '{{ var('reload_source', 'ALL') }}' AND dbt_loaded_at < CURRENT_DATE()"
            if var('reload_source', 'ALL') != 'ALL'
            else "-- No pre-delete when reload_source = ALL"
        ],
        
        tags=['partitioned', 'pre_hook_delete', 'source_reload', 'pattern_example']
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN 5: PRE-HOOK DELETE (Custom delete + append)
═══════════════════════════════════════════════════════════════════════════════

Description:
  - Delete specific subset of data via pre-hook
  - Insert fresh data via append strategy
  - Ideal for source-system-specific reloads

When to Use:
  ✅ Reloading data from a specific source system
  ✅ Selective partition/subset refresh
  ✅ When delete criteria is complex
  ✅ Multi-source consolidation with source-specific refresh

How It Works:
  1. PRE-HOOK: DELETE FROM target WHERE source_system = 'BRP'
  2. INSERT: Append data filtered to source_system = 'BRP'

Configuration:
  - var('reload_source'): Source system to reload (default: 'ALL')
  - Run with: dbt run --select fact_o2c_by_source --vars '{"reload_source": "BRP"}'

Testing This Pattern:
  1. First run: dbt run --select fact_o2c_by_source (loads all sources)
  2. Run for specific source: dbt run --select fact_o2c_by_source --vars '{"reload_source": "BRP"}'
  3. Verify:
     - BRP records: Deleted and re-inserted (new dbt_loaded_at)
     - Other sources: UNCHANGED
  4. Run again with different source: --vars '{"reload_source": "CIP"}'

═══════════════════════════════════════════════════════════════════════════════
#}

WITH source_data AS (
    SELECT
        source_system,
        order_key,
        order_id,
        order_date,
        customer_id,
        customer_name,
        order_amount,
        currency_code,
        order_status
    FROM {{ ref('stg_enriched_orders') }}
    
    -- Filter to specific source if specified
    {% if var('reload_source', 'ALL') != 'ALL' %}
    WHERE source_system = '{{ var("reload_source") }}'
    {% endif %}
)

SELECT
    s.source_system,
    s.order_key,
    s.order_id,
    s.order_date,
    s.customer_id,
    s.customer_name,
    s.order_amount,
    s.currency_code,
    s.order_status,
    
    -- Full audit columns
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


