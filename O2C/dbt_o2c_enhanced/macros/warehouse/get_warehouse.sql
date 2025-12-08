{% macro get_warehouse() %}
{#- 
Returns warehouse from config table or fallback.
NOTE: Only works at EXECUTION time, not useful for config().
-#}
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
              AND scope_name IN ('{{ model_name }}', '{{ layer_name }}', '{{ project_name }}', '{{ environment }}', 'DEFAULT')
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


{% macro use_dynamic_warehouse() %}
{#- 
Placeholder macro - dynamic warehouse via pre_hook is complex in Snowflake Native dbt.
For now, returns empty string (no-op).
Use hardcoded pre_hook="USE WAREHOUSE <name>" if needed.
-#}
{# No-op - returns nothing #}
{% endmacro %}
