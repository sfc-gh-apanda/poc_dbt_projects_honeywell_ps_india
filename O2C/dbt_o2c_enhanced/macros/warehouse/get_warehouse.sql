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

    This macro queries the config table to get the warehouse name.
    
    In Snowflake Native dbt, run_query() may execute during compile phase
    because the dbt runtime is already inside a Snowflake session.
    
    Usage:
        {{
            config(
                materialized='table',
                snowflake_warehouse=get_warehouse()
            )
        }}
        
    To change warehouse (NO CI/CD needed):
        UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
        SET warehouse_name = 'NEW_WAREHOUSE'
        WHERE scope_name = 'model_name';

================================================================================
-#}

{#- Build scope values for lookup -#}
{% set model_name = this.name if this else 'UNKNOWN' %}
{% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else '' %}
{% set project = project_name %}
{% set environment = target.name %}
{% set fallback = target.warehouse %}

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

{#- 
    Execute query and return result
    NOTE: In Snowflake Native dbt, run_query() may work at compile time
    because the runtime is already inside Snowflake.
    If results are empty, fallback to profiles.yml warehouse.
-#}
{% set results = run_query(query) %}

{% if results and results.rows | length > 0 %}
    {{ return(results.columns[0].values()[0]) }}
{% else %}
    {{ return(fallback) }}
{% endif %}

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
        
    Prerequisites:
        Run O2C_ENHANCED_DYNAMIC_WAREHOUSE_SETUP.sql to create the stored procedure.

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
