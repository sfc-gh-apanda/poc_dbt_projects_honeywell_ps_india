{#-
================================================================================
    DYNAMIC WAREHOUSE MACROS
================================================================================

    Two approaches:
    1. get_warehouse() - For snowflake_warehouse config (compile-time)
    2. switch_warehouse() - For pre_hook (runtime) - RECOMMENDED
    
================================================================================
-#}


{% macro get_warehouse() %}
{#- 
    Returns warehouse name. Used in snowflake_warehouse config.
    NOTE: Config is evaluated at compile-time when execute=False,
    so this will return profiles.yml fallback during compilation.
-#}
    {% set fallback = target.warehouse %}
    
    {% if execute %}
        {% set model_name = this.name if this else 'UNKNOWN' %}
        {% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else 'unknown' %}
        {% set project_name = this.package_name if this else 'unknown' %}
        {% set environment = target.name %}
        
        {% set query %}
            SELECT warehouse_name
            FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
            WHERE is_active = TRUE
              AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
              AND scope_name IN ('{{ model_name }}', '{{ layer_name }}', '{{ project_name }}', '{{ environment }}', 'DEFAULT')
            ORDER BY priority ASC
            LIMIT 1
        {% endset %}
        
        {% set results = run_query(query) %}
        {% if results and results.rows | length > 0 %}
            {{ return(results.rows[0][0]) }}
        {% endif %}
    {% endif %}
    
    {{ return(fallback) }}
{% endmacro %}


{% macro switch_warehouse() %}
{#-
================================================================================
    SWITCH_WAREHOUSE - Runtime warehouse switching via pre_hook
================================================================================

    This macro generates a USE WAREHOUSE statement that:
    1. Queries the config table at RUNTIME
    2. Switches to the warehouse found
    3. Falls back to profiles.yml if not found
    
    Usage in model:
        {{
            config(
                materialized='table',
                pre_hook="{{ switch_warehouse() }}"
            )
        }}

================================================================================
-#}

{% set model_name = this.name if this else 'UNKNOWN' %}
{% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else 'unknown' %}
{% set project_name = this.package_name if this else 'unknown' %}
{% set environment = target.name %}
{% set fallback = target.warehouse %}

{#- Generate SQL that runs at execution time -#}
USE WAREHOUSE IDENTIFIER(
    COALESCE(
        (SELECT warehouse_name 
         FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
         WHERE is_active = TRUE
           AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
           AND scope_name IN ('{{ model_name }}', '{{ layer_name }}', '{{ project_name }}', '{{ environment }}', 'DEFAULT')
         ORDER BY priority ASC
         LIMIT 1),
        '{{ fallback }}'
    )
)
{% endmacro %}
