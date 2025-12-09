{#-
================================================================================
    DYNAMIC WAREHOUSE MACROS
================================================================================

    Two approaches available:
    
    1. get_warehouse() - For snowflake_warehouse config
       - Uses run_query() during compile phase
       - Snowflake Native dbt may support compile-time queries
       
    2. switch_warehouse() - For pre_hook (backup approach)
       - Uses stored procedure at runtime
       - Guaranteed to work
    
================================================================================
-#}


{% macro get_warehouse() %}
{#-
================================================================================
    GET_WAREHOUSE - Dynamic warehouse lookup via run_query
================================================================================
-#}

{#- Build scope values for lookup -#}
{% set model_name = this.name if this else 'UNKNOWN' %}
{% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else '' %}
{% set project = project_name %}
{% set environment = target.name %}
{% set fallback = target.warehouse %}

{#- DEBUG: Log what we're working with -#}
{{ log("=== GET_WAREHOUSE DEBUG ===", info=True) }}
{{ log("  this available: " ~ (this is not none), info=True) }}
{{ log("  model_name: " ~ model_name, info=True) }}
{{ log("  layer_name: " ~ layer_name, info=True) }}
{{ log("  project: " ~ project, info=True) }}
{{ log("  environment: " ~ environment, info=True) }}
{{ log("  fallback: " ~ fallback, info=True) }}
{{ log("  execute flag: " ~ execute, info=True) }}

{#- Query the config table -#}
{% set query %}
    SELECT warehouse_name 
    FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
    WHERE is_active = TRUE
      AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
      AND scope_name IN ('{{ model_name }}', '{{ layer_name }}', '{{ project }}', '{{ environment }}', 'DEFAULT')
    ORDER BY priority ASC
    LIMIT 1
{% endset %}

{{ log("  Query: " ~ query | replace('\n', ' '), info=True) }}

{#- Execute query and check results -#}
{% set results = run_query(query) %}

{{ log("  run_query executed", info=True) }}
{{ log("  results is none: " ~ (results is none), info=True) }}

{% if results %}
    {{ log("  results.rows length: " ~ results.rows | length, info=True) }}
    {% if results.rows | length > 0 %}
        {% set warehouse_value = results.columns[0].values()[0] %}
        {{ log("  >>> FOUND warehouse: " ~ warehouse_value, info=True) }}
        {{ log("=== END DEBUG (returning: " ~ warehouse_value ~ ") ===", info=True) }}
        {{ return(warehouse_value) }}
    {% else %}
        {{ log("  >>> NO ROWS returned", info=True) }}
    {% endif %}
{% else %}
    {{ log("  >>> RESULTS IS NONE/EMPTY", info=True) }}
{% endif %}

{{ log("=== END DEBUG (returning fallback: " ~ fallback ~ ") ===", info=True) }}
{{ return(fallback) }}

{% endmacro %}


{% macro get_warehouse_hardcoded() %}
{#-
================================================================================
    GET_WAREHOUSE_HARDCODED - Test with hardcoded scope (bypass 'this' issue)
================================================================================
-#}

{% set fallback = target.warehouse %}

{{ log("=== GET_WAREHOUSE_HARDCODED DEBUG ===", info=True) }}
{{ log("  execute flag: " ~ execute, info=True) }}

{#- Hardcoded query - no dependency on 'this' -#}
{% set query %}
    SELECT warehouse_name 
    FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG 
    WHERE is_active = TRUE
      AND scope_name = 'stg_enriched_orders'
    LIMIT 1
{% endset %}

{{ log("  Query: " ~ query | replace('\n', ' '), info=True) }}

{% set results = run_query(query) %}

{{ log("  run_query executed", info=True) }}

{% if results %}
    {{ log("  results.rows length: " ~ results.rows | length, info=True) }}
    {% if results.rows | length > 0 %}
        {% set warehouse_value = results.columns[0].values()[0] %}
        {{ log("  >>> FOUND warehouse: " ~ warehouse_value, info=True) }}
        {{ return(warehouse_value) }}
    {% endif %}
{% endif %}

{{ log("  >>> Returning fallback: " ~ fallback, info=True) }}
{{ return(fallback) }}

{% endmacro %}


{% macro switch_warehouse() %}
{#-
================================================================================
    SWITCH_WAREHOUSE - Runtime warehouse switching via stored procedure
================================================================================

    BACKUP APPROACH: Use this if get_warehouse() doesn't work.
    
    This macro calls a stored procedure that:
    1. Queries the config table at RUNTIME
    2. Executes USE WAREHOUSE internally via EXECUTE IMMEDIATE
    3. Falls back to COMPUTE_WH if not found
    
    Usage in model:
        {{
            config(
                materialized='table',
                pre_hook="{{ switch_warehouse() }}"
            )
        }}

================================================================================
-#}

{#- Build parameters for the stored procedure -#}
{% set model_name = this.name if this else 'UNKNOWN' %}
{% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else '' %}
{% set project = project_name %}
{% set environment = target.name %}

{#- Call the stored procedure - it executes USE WAREHOUSE internally -#}
CALL EDW.CONFIG.SET_DYNAMIC_WAREHOUSE(
    '{{ model_name }}',
    '{{ layer_name }}',
    '{{ project }}',
    '{{ environment }}'
)
{% endmacro %}
