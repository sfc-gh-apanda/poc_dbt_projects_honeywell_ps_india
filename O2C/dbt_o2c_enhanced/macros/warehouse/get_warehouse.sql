{% macro get_warehouse() %}
{#- Returns warehouse from config table or fallback. Works at EXECUTION time only. -#}
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
================================================================================
DYNAMIC WAREHOUSE SWITCH - Executed as pre_hook
================================================================================

This generates a USE WAREHOUSE statement that queries the config table
directly in SQL, bypassing the Jinja execute flag limitation.

The SQL runs as a pre_hook before the model executes.
================================================================================
-#}

{% set model_name = this.name if this else 'UNKNOWN' %}
{% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else 'unknown' %}
{% set project_name = this.package_name if this else 'unknown' %}
{% set environment = target.name %}
{% set fallback = target.warehouse %}

{#- Generate pure SQL that will run as pre_hook -#}
CALL SYSTEM$SET_RETURN_VALUE(NULL);
DECLARE
    v_warehouse VARCHAR;
BEGIN
    SELECT warehouse_name INTO v_warehouse
    FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
    WHERE is_active = TRUE
      AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
      AND scope_name IN ('{{ model_name }}', '{{ layer_name }}', '{{ project_name }}', '{{ environment }}', 'DEFAULT')
    ORDER BY priority ASC
    LIMIT 1;
    
    IF (v_warehouse IS NULL) THEN
        v_warehouse := '{{ fallback }}';
    END IF;
    
    EXECUTE IMMEDIATE 'USE WAREHOUSE ' || v_warehouse;
END;

{% endmacro %}


{% macro use_warehouse_simple() %}
{#- Simple version: Just outputs USE WAREHOUSE with inline query -#}
{% set model_name = this.name if this else 'UNKNOWN' %}
{% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else 'unknown' %}
{% set project_name = this.package_name if this else 'unknown' %}
{% set environment = target.name %}
{% set fallback = target.warehouse %}

USE WAREHOUSE IDENTIFIER(
    (SELECT COALESCE(
        (SELECT warehouse_name 
         FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
         WHERE is_active = TRUE
           AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
           AND scope_name IN ('{{ model_name }}', '{{ layer_name }}', '{{ project_name }}', '{{ environment }}', 'DEFAULT')
         ORDER BY priority ASC
         LIMIT 1),
        '{{ fallback }}'
    ))
)
{% endmacro %}
