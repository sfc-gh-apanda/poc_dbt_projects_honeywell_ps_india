{% macro get_warehouse() %}
{#-
================================================================================
    DYNAMIC WAREHOUSE LOOKUP MACRO
================================================================================

    IMPORTANT LIMITATION:
    ---------------------
    The `snowflake_warehouse` config is resolved at COMPILE time.
    Database queries only work at EXECUTION time (when execute=True).
    
    This means: get_warehouse() in config() ALWAYS returns profiles.yml fallback.
    
    SOLUTION: Use pre_hook to switch warehouse at RUNTIME.
    See use_dynamic_warehouse() macro below.

================================================================================
-#}

    {#- This only works during execution, not compilation -#}
    {#- For config(), it will always return the fallback -#}
    
    {% set fallback_warehouse = target.warehouse %}
    
    {% if execute %}
        {% set model_name = this.name if this else 'UNKNOWN' %}
        {% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else 'unknown' %}
        {% set project_name = this.package_name if this else 'unknown' %}
        {% set environment = target.name %}
        
        {% set lookup_query %}
            SELECT warehouse_name
            FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
            WHERE is_active = TRUE
              AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
              AND scope_name IN (
                  '{{ model_name }}',
                  '{{ layer_name }}',
                  '{{ project_name }}',
                  '{{ environment }}',
                  'DEFAULT'
              )
            ORDER BY priority ASC
            LIMIT 1
        {% endset %}
        
        {% set results = run_query(lookup_query) %}
        
        {% if results and results.rows | length > 0 %}
            {{ return(results.rows[0][0]) }}
        {% endif %}
    {% endif %}
    
    {{ return(fallback_warehouse) }}
{% endmacro %}


{# ============================================================================
   USE_DYNAMIC_WAREHOUSE - Runtime warehouse switching via pre_hook
   ============================================================================
   
   This is the WORKING solution for dynamic warehouse configuration.
   Add this to your model's pre_hook to switch warehouse at runtime.
   
   Usage in model config:
       {{
           config(
               materialized='table',
               pre_hook="{{ use_dynamic_warehouse() }}"
           )
       }}
   
   ============================================================================ #}

{% macro use_dynamic_warehouse() %}
    {#- This runs at EXECUTION time, so database queries work! -#}
    
    {% set model_name = this.name if this else 'UNKNOWN' %}
    {% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else 'unknown' %}
    {% set project_name = this.package_name if this else 'unknown' %}
    {% set environment = target.name %}
    {% set fallback_warehouse = target.warehouse %}
    
    {% set lookup_query %}
        SELECT warehouse_name
        FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
        WHERE is_active = TRUE
          AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
          AND scope_name IN (
              '{{ model_name }}',
              '{{ layer_name }}',
              '{{ project_name }}',
              '{{ environment }}',
              'DEFAULT'
          )
        ORDER BY priority ASC
        LIMIT 1
    {% endset %}
    
    {#- Execute during run phase -#}
    {% if execute %}
        {% set results = run_query(lookup_query) %}
        {% if results and results.rows | length > 0 %}
            {% set target_warehouse = results.rows[0][0] %}
            {{ log("🏭 [" ~ model_name ~ "] Switching to warehouse: " ~ target_warehouse, info=True) }}
            USE WAREHOUSE {{ target_warehouse }}
        {% else %}
            {{ log("🏭 [" ~ model_name ~ "] Using default warehouse: " ~ fallback_warehouse, info=True) }}
            USE WAREHOUSE {{ fallback_warehouse }}
        {% endif %}
    {% else %}
        {#- During compilation, output the USE WAREHOUSE statement with lookup -#}
        USE WAREHOUSE IDENTIFIER(
            COALESCE(
                (SELECT warehouse_name FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
                 WHERE is_active = TRUE
                   AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
                   AND scope_name IN ('{{ model_name }}', '{{ layer_name }}', '{{ project_name }}', '{{ environment }}', 'DEFAULT')
                 ORDER BY priority ASC LIMIT 1),
                '{{ fallback_warehouse }}'
            )
        )
    {% endif %}
{% endmacro %}


{% macro log_warehouse_resolution() %}
{#- Debug macro to see warehouse resolution. -#}
    {% set model_name = this.name if this else 'UNKNOWN' %}
    {% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else 'unknown' %}
    {% set project_name = this.package_name if this else 'unknown' %}
    {% set environment = target.name %}
    
    {{ log("═══════════════════════════════════════════════════════════", info=True) }}
    {{ log("WAREHOUSE RESOLUTION DEBUG", info=True) }}
    {{ log("Model:       " ~ model_name, info=True) }}
    {{ log("Layer:       " ~ layer_name, info=True) }}
    {{ log("Project:     " ~ project_name, info=True) }}
    {{ log("Environment: " ~ environment, info=True) }}
    {{ log("Resolved:    " ~ get_warehouse(), info=True) }}
    {{ log("═══════════════════════════════════════════════════════════", info=True) }}
{% endmacro %}
