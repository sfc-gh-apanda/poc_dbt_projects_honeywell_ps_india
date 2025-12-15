{{
    config(
        materialized='table',
        pre_hook="{{ switch_warehouse() }}",
        tags=['dimension', 'scd2', 'soft_delete', 'pattern_example'],
        query_tag='dbt_dim_o2c_customer'
    )
}}

{#
═══════════════════════════════════════════════════════════════════════════════
PATTERN: SCD-2 DIMENSION with SOFT DELETE
═══════════════════════════════════════════════════════════════════════════════

Description:
  Customer dimension with full historical tracking (SCD-2) and soft delete.
  Built on top of dbt snapshot for change data capture.

Key Features:
  • Full history preservation - all changes tracked
  • Soft delete - deleted records marked, never physically removed
  • Point-in-time queries - query customer as of any date
  • Industry standard columns: is_current, is_active, is_deleted, record_status

Column Definitions:
  ┌─────────────────┬─────────────────────────────────────────────────────┐
  │ Column          │ Description                                         │
  ├─────────────────┼─────────────────────────────────────────────────────┤
  │ is_current      │ TRUE = Latest version of this customer              │
  │ is_active       │ TRUE = Active in current business operations        │
  │ is_deleted      │ TRUE = Removed from source (soft delete)            │
  │ record_status   │ ACTIVE, INACTIVE, DELETED, HISTORICAL               │
  │ deleted_at      │ Timestamp when soft-deleted                         │
  │ dbt_valid_from  │ SCD-2: When this version became effective           │
  │ dbt_valid_to    │ SCD-2: When this version ended (NULL = current)     │
  └─────────────────┴─────────────────────────────────────────────────────┘

Query Patterns:
  -- Current active customers only
  SELECT * FROM dim_o2c_customer WHERE is_current AND is_active;
  
  -- All current customers (including inactive/deleted)
  SELECT * FROM dim_o2c_customer WHERE is_current;
  
  -- Customer as of specific date
  SELECT * FROM dim_o2c_customer 
  WHERE '2024-06-15' BETWEEN dbt_valid_from 
    AND COALESCE(dbt_valid_to, '9999-12-31');
  
  -- Deleted customers audit
  SELECT * FROM dim_o2c_customer WHERE is_deleted ORDER BY deleted_at DESC;

Prerequisites:
  Run snapshot first: dbt snapshot --select snap_customer

═══════════════════════════════════════════════════════════════════════════════
#}

WITH snapshot_data AS (
    -- Source from dbt snapshot (SCD-2 data)
    SELECT * FROM {{ ref('snap_customer') }}
),

-- Identify the latest version for each customer (for soft delete detection)
latest_versions AS (
    SELECT
        customer_num_sk,
        source_system,
        MAX(dbt_valid_from) AS max_valid_from,
        MAX(CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END) AS has_current_version
    FROM snapshot_data
    GROUP BY customer_num_sk, source_system
),

enriched AS (
    SELECT
        -- ═══════════════════════════════════════════════════════════════
        -- SURROGATE KEY
        -- ═══════════════════════════════════════════════════════════════
        snap.customer_num_sk || '|' || snap.source_system || '|' || 
            TO_VARCHAR(snap.dbt_valid_from, 'YYYYMMDDHH24MISS') AS customer_key,
        
        -- Business key for joining (use with is_current = TRUE)
        snap.customer_num_sk || '|' || snap.source_system AS customer_business_key,
        
        -- ═══════════════════════════════════════════════════════════════
        -- NATURAL KEYS
        -- ═══════════════════════════════════════════════════════════════
        snap.source_system,
        snap.customer_num_sk AS customer_id,
        
        -- ═══════════════════════════════════════════════════════════════
        -- BUSINESS ATTRIBUTES
        -- ═══════════════════════════════════════════════════════════════
        snap.customer_name,
        snap.customer_type,
        snap.customer_country,
        snap.customer_country_name,
        snap.customer_classification,
        snap.customer_account_group,
        
        -- MDM Attributes
        snap.duns_number,
        snap.mdm_customer_global_ultimate_duns AS global_ultimate_duns,
        snap.mdm_customer_global_ultimate_name AS global_ultimate_name,
        snap.mdm_customer_full_name AS full_name,
        
        -- Derived flags
        CASE WHEN snap.customer_type = 'I' THEN TRUE ELSE FALSE END AS is_internal,
        
        -- Source timestamps
        snap.source_load_ts,
        snap.source_update_ts,
        
        -- ═══════════════════════════════════════════════════════════════
        -- SCD-2 VALIDITY COLUMNS (from dbt snapshot)
        -- ═══════════════════════════════════════════════════════════════
        snap.dbt_valid_from,
        snap.dbt_valid_to,
        snap.dbt_scd_id,
        snap.dbt_updated_at AS dbt_snapshot_updated_at,
        
        -- ═══════════════════════════════════════════════════════════════
        -- IS_CURRENT: Is this the latest version of this customer?
        -- ═══════════════════════════════════════════════════════════════
        CASE 
            WHEN snap.dbt_valid_to IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_current,
        
        -- ═══════════════════════════════════════════════════════════════
        -- IS_DELETED: Record was removed from source (soft delete)
        -- Detected when: record has dbt_valid_to set AND no current version exists
        -- ═══════════════════════════════════════════════════════════════
        CASE 
            WHEN snap.dbt_valid_to IS NOT NULL 
             AND lv.has_current_version = 0
             AND snap.dbt_valid_from = lv.max_valid_from
            THEN TRUE
            ELSE FALSE 
        END AS is_deleted,
        
        -- ═══════════════════════════════════════════════════════════════
        -- DELETED_AT: When was it soft-deleted?
        -- ═══════════════════════════════════════════════════════════════
        CASE 
            WHEN snap.dbt_valid_to IS NOT NULL 
             AND lv.has_current_version = 0
             AND snap.dbt_valid_from = lv.max_valid_from
            THEN snap.dbt_valid_to
            ELSE NULL 
        END AS deleted_at,
        
        -- ═══════════════════════════════════════════════════════════════
        -- IS_ACTIVE: Business-level active status
        -- Active = Current version AND source says active AND not deleted
        -- ═══════════════════════════════════════════════════════════════
        CASE
            -- Deleted from source = Not active
            WHEN snap.dbt_valid_to IS NOT NULL 
             AND lv.has_current_version = 0
            THEN FALSE
            -- Source marked inactive
            WHEN snap.source_is_active = FALSE 
            THEN FALSE
            -- Current version and source is active
            WHEN snap.dbt_valid_to IS NULL AND snap.source_is_active = TRUE 
            THEN TRUE
            -- Historical version = Not active
            ELSE FALSE
        END AS is_active,
        
        -- ═══════════════════════════════════════════════════════════════
        -- RECORD_STATUS: Comprehensive lifecycle status
        -- ═══════════════════════════════════════════════════════════════
        CASE
            -- Current and source active
            WHEN snap.dbt_valid_to IS NULL AND snap.source_is_active = TRUE 
                THEN 'ACTIVE'
            -- Current but source marked inactive
            WHEN snap.dbt_valid_to IS NULL AND snap.source_is_active = FALSE 
                THEN 'INACTIVE'
            -- Deleted from source (last version, closed, no current exists)
            WHEN snap.dbt_valid_to IS NOT NULL 
             AND lv.has_current_version = 0
             AND snap.dbt_valid_from = lv.max_valid_from
                THEN 'DELETED'
            -- Historical version (superseded by newer version)
            ELSE 'HISTORICAL'
        END AS record_status,
        
        -- ═══════════════════════════════════════════════════════════════
        -- CHANGE DETECTION
        -- ═══════════════════════════════════════════════════════════════
        {{ row_hash([
            'snap.customer_name', 
            'snap.customer_type', 
            'snap.customer_country', 
            'snap.customer_classification'
        ]) }},
        
        -- ═══════════════════════════════════════════════════════════════
        -- AUDIT COLUMNS
        -- ═══════════════════════════════════════════════════════════════
        {{ audit_columns() }}

    FROM snapshot_data snap
    
    LEFT JOIN latest_versions lv
        ON snap.customer_num_sk = lv.customer_num_sk
       AND snap.source_system = lv.source_system
)

SELECT * FROM enriched
