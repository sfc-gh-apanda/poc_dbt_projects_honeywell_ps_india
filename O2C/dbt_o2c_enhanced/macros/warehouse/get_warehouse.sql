{#-
================================================================================
    DYNAMIC WAREHOUSE MACROS
================================================================================

    Approach: Stored Procedure + pre_hook
    
    The stored procedure executes USE WAREHOUSE internally via EXECUTE IMMEDIATE,
    bypassing Snowflake's limitation that USE WAREHOUSE doesn't accept dynamic expressions.
    
    Prerequisites:
    - Run O2C_ENHANCED_DYNAMIC_WAREHOUSE_SETUP.sql to create:
      1. EDW.CONFIG.DBT_WAREHOUSE_CONFIG table
      2. EDW.CONFIG.SET_DYNAMIC_WAREHOUSE procedure
    
================================================================================
-#}


{% macro switch_warehouse() %}
{#-
================================================================================
    SWITCH_WAREHOUSE - Runtime warehouse switching via stored procedure
================================================================================

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
        
    To change warehouse (NO CI/CD needed):
        UPDATE EDW.CONFIG.DBT_WAREHOUSE_CONFIG
        SET warehouse_name = 'NEW_WAREHOUSE'
        WHERE scope_name = 'model_name';

================================================================================
-#}

{#- Build parameters for the stored procedure -#}
{% set model_name = this.name if this else 'UNKNOWN' %}
{% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else '' %}
{% set project_name = project_name %}
{% set environment = target.name %}

{#- Call the stored procedure - it executes USE WAREHOUSE internally -#}
CALL EDW.CONFIG.SET_DYNAMIC_WAREHOUSE(
    '{{ model_name }}',
    '{{ layer_name }}',
    '{{ project_name }}',
    '{{ environment }}'
)
{% endmacro %}


{% macro get_warehouse() %}
{#- 
================================================================================
    GET_WAREHOUSE - Fallback macro (for reference)
================================================================================

    NOTE: This is kept for backwards compatibility and documentation.
    Config values are evaluated at compile-time when execute=False,
    so this will always return profiles.yml fallback during compilation.
    
    Use switch_warehouse() with pre_hook instead for dynamic switching.
================================================================================
-#}
    {{ return(target.warehouse) }}
{% endmacro %}
