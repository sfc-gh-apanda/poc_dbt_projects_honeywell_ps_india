{#-
================================================================================
    DYNAMIC WAREHOUSE - REFERENCE ONLY
================================================================================

    NOTE: Due to Snowflake Native dbt limitations, dynamic warehouse cannot be
    set within dbt model configs (config() is evaluated at compile time when
    database queries cannot run).

    SOLUTION: Use the stored procedure wrapper instead:
    
        CALL EDW.CONFIG.RUN_DBT_WITH_DYNAMIC_WAREHOUSE(
            'dbt_o2c_enhanced',
            'build --target dev'
        );
    
    This procedure:
    1. Reads warehouse from EDW.CONFIG.DBT_WAREHOUSE_CONFIG table
    2. Switches to that warehouse using USE WAREHOUSE
    3. Then executes dbt build
    
    To change warehouse without CI/CD:
    
        CALL EDW.CONFIG.SET_PROJECT_WAREHOUSE(
            'dbt_o2c_enhanced', 
            'COMPUTE_WH_LARGE'
        );
    
    See: O2C/docs_o2c_enhanced/O2C_ENHANCED_DYNAMIC_WAREHOUSE.sql

================================================================================
-#}

{% macro get_warehouse() %}
{#- 
    This macro is kept for reference but is NOT used in model configs.
    The stored procedure wrapper handles dynamic warehouse selection.
    
    Returns: target.warehouse from profiles.yml (always the fallback)
-#}
    {{ return(target.warehouse) }}
{% endmacro %}
