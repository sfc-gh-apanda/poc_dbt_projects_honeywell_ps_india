{% macro get_warehouse() %}
{#-
================================================================================
    DYNAMIC WAREHOUSE LOOKUP MACRO
================================================================================

    Purpose:
        Retrieves warehouse configuration from Snowflake metadata table
        with hierarchical fallback for maximum flexibility.

    Resolution Order (Most Specific → Least Specific):
        1. MODEL level      → Exact model name (e.g., 'dm_o2c_reconciliation')
        2. LAYER level      → Layer name (e.g., 'staging', 'marts')
        3. PROJECT level    → Project name (e.g., 'dbt_o2c_enhanced')
        4. ENVIRONMENT level→ Target name (e.g., 'dev', 'prod')
        5. DEFAULT level    → Global default row
        6. profiles.yml     → Ultimate fallback (target.warehouse)

    Configuration Table:
        EDW.CONFIG.DBT_WAREHOUSE_CONFIG

    IMPORTANT: This macro only works during EXECUTION phase (dbt run/build).
    During parsing, it returns the profiles.yml fallback warehouse.

================================================================================
-#}

    {#- Get context about the current model -#}
    {% set model_name = this.name if this else 'UNKNOWN' %}
    {% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else 'unknown' %}
    {% set project_name = this.package_name if this else 'unknown' %}
    {% set environment = target.name %}
    {% set fallback_warehouse = target.warehouse %}
    
    {#- During execution phase, query the config table -#}
    {% if execute %}
        
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
            {% set warehouse_from_config = results.rows[0][0] %}
            {{ log("🏭 [" ~ model_name ~ "] Using warehouse: " ~ warehouse_from_config ~ " (from config table)", info=True) }}
            {{ return(warehouse_from_config) }}
        {% else %}
            {{ log("🏭 [" ~ model_name ~ "] No config found, using: " ~ fallback_warehouse ~ " (from profiles.yml)", info=True) }}
        {% endif %}
        
    {% endif %}
    
    {#- Fallback to profiles.yml -#}
    {{ return(fallback_warehouse) }}
    
{% endmacro %}


{% macro get_warehouse_for_model(model_name) %}
{#- Alternative macro that accepts model name as parameter. -#}
    {% if execute %}
        {% set lookup_query %}
            SELECT warehouse_name
            FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
            WHERE is_active = TRUE
              AND scope_name = '{{ model_name }}'
            LIMIT 1
        {% endset %}
        
        {% set results = run_query(lookup_query) %}
        
        {% if results and results.rows | length > 0 %}
            {{ return(results.rows[0][0]) }}
        {% endif %}
    {% endif %}
    
    {{ return(target.warehouse) }}
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
