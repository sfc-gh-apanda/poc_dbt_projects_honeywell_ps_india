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

    Usage:
        In model config:
            {{ config(snowflake_warehouse=get_warehouse()) }}

    Configuration Table:
        EDW.CONFIG.DBT_WAREHOUSE_CONFIG
        - config_scope: 'MODEL', 'LAYER', 'PROJECT', 'ENVIRONMENT', 'DEFAULT'
        - scope_name: The identifier
        - warehouse_name: Target warehouse
        - priority: Lower = higher priority
        - is_active: Boolean flag

    Benefits:
        - No code changes to modify warehouse
        - Just UPDATE the config table
        - Built-in audit trail
        - Always has fallback (never fails)

================================================================================
-#}

    {#- Get context about the current model -#}
    {% set model_name = this.name if this else 'UNKNOWN' %}
    {% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else 'unknown' %}
    {% set project_name = this.package_name if this else 'unknown' %}
    {% set environment = target.name %}
    
    {#- Only execute queries during run phase, not parse phase -#}
    {% if execute %}
        
        {#- Build query to get warehouse from config table with priority ordering -#}
        {% set lookup_query %}
            SELECT warehouse_name
            FROM EDW.CONFIG.DBT_WAREHOUSE_CONFIG
            WHERE is_active = TRUE
              AND (effective_to IS NULL OR effective_to >= CURRENT_DATE())
              AND scope_name IN (
                  '{{ model_name }}',       -- Level 1: Exact model
                  '{{ layer_name }}',       -- Level 2: Layer (staging, marts)
                  '{{ project_name }}',     -- Level 3: Project (dbt_o2c_enhanced)
                  '{{ environment }}',      -- Level 4: Environment (dev, prod)
                  'DEFAULT'                 -- Level 5: Global default
              )
            ORDER BY priority ASC           -- Lower priority number = higher precedence
            LIMIT 1
        {% endset %}
        
        {#- Execute the lookup query -#}
        {% set results = run_query(lookup_query) %}
        
        {#- If found in config table, return that warehouse -#}
        {% if results and results.rows | length > 0 %}
            {% set warehouse_from_config = results.rows[0][0] %}
            {{ log("🏭 Warehouse for '" ~ model_name ~ "': " ~ warehouse_from_config ~ " (from config table)", info=False) }}
            {{ return(warehouse_from_config) }}
        {% endif %}
        
    {% endif %}
    
    {#- ═══════════════════════════════════════════════════════════════════ -#}
    {#- ULTIMATE FALLBACK: profiles.yml warehouse                           -#}
    {#- This ALWAYS exists, so dbt will NEVER fail due to missing config   -#}
    {#- ═══════════════════════════════════════════════════════════════════ -#}
    
    {% set fallback_warehouse = target.warehouse %}
    {{ log("🏭 Warehouse for '" ~ model_name ~ "': " ~ fallback_warehouse ~ " (fallback to profiles.yml)", info=False) }}
    {{ return(fallback_warehouse) }}
    
{% endmacro %}


{% macro get_warehouse_for_model(model_name) %}
{#-
    Alternative macro that accepts model name as parameter.
    Useful when you need to look up warehouse for a specific model.
    
    Usage:
        {{ get_warehouse_for_model('dm_o2c_reconciliation') }}
-#}

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
    
    {#- Fallback to profiles.yml -#}
    {{ return(target.warehouse) }}
    
{% endmacro %}


{% macro log_warehouse_resolution() %}
{#-
    Debug macro to see how warehouse would be resolved for current model.
    
    Usage in a model:
        {{ log_warehouse_resolution() }}
-#}

    {% set model_name = this.name if this else 'UNKNOWN' %}
    {% set layer_name = this.fqn[1] if (this and this.fqn | length > 1) else 'unknown' %}
    {% set project_name = this.package_name if this else 'unknown' %}
    {% set environment = target.name %}
    
    {{ log("═══════════════════════════════════════════════════════════", info=True) }}
    {{ log("WAREHOUSE RESOLUTION DEBUG", info=True) }}
    {{ log("═══════════════════════════════════════════════════════════", info=True) }}
    {{ log("Model:       " ~ model_name, info=True) }}
    {{ log("Layer:       " ~ layer_name, info=True) }}
    {{ log("Project:     " ~ project_name, info=True) }}
    {{ log("Environment: " ~ environment, info=True) }}
    {{ log("Resolved:    " ~ get_warehouse(), info=True) }}
    {{ log("═══════════════════════════════════════════════════════════", info=True) }}
    
{% endmacro %}

