/*
    Deploy O2C Semantic Views for Cortex Analyst
    
    This script manually creates Snowflake SEMANTIC VIEW objects
    that enable Cortex Analyst to answer natural language questions.
    
    Run this in Snowflake after successful dbt_o2c build.
    
    Reference: https://docs.snowflake.com/en/user-guide/semantic-views
*/

USE ROLE DBT_O2C_DEVELOPER;
USE DATABASE EDW;
USE WAREHOUSE COMPUTE_WH;

-- Create schema for semantic views
CREATE SCHEMA IF NOT EXISTS EDW.O2C_SEMANTIC_VIEWS;

-- ============================================================================
-- SEMANTIC VIEW 1: O2C Reconciliation
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW EDW.O2C_SEMANTIC_VIEWS.O2C_RECONCILIATION_SEMANTIC
AS
  SELECT
    -- Keys
    source_system,
    order_key,
    invoice_key,
    payment_key,
    
    -- Dates (time dimensions)
    order_date,
    invoice_date,
    payment_date,
    due_date,
    
    -- Customer dimensions
    customer_id,
    customer_name,
    customer_type,
    customer_country,
    
    -- Status dimensions
    reconciliation_status,
    payment_timing,
    
    -- Order metrics
    order_amount,
    order_quantity,
    
    -- Invoice metrics
    invoice_amount,
    
    -- Payment metrics
    payment_amount,
    
    -- Calculated metrics
    days_order_to_invoice,
    days_invoice_to_payment,
    days_order_to_cash,
    days_past_due,
    unbilled_amount,
    outstanding_amount
    
  FROM EDW.O2C_CORE.DM_O2C_RECONCILIATION

-- Semantic annotations
WITH SEMANTIC
  -- Time dimension
  TIME DIMENSION order_date
  
  -- Dimension annotations with synonyms
  DIMENSION customer_name SYNONYMS ('customer', 'company', 'account')
  DIMENSION customer_type SYNONYMS ('account type', 'customer category')
  DIMENSION customer_country SYNONYMS ('country', 'location', 'geography')
  DIMENSION reconciliation_status SYNONYMS ('status', 'state')
  DIMENSION payment_timing SYNONYMS ('payment status', 'timeliness')
  
  -- Measure annotations with synonyms and aggregations
  MEASURE order_amount 
    AGGREGATION SUM
    SYNONYMS ('revenue', 'sales', 'order value', 'total order value')
    
  MEASURE outstanding_amount 
    AGGREGATION SUM
    SYNONYMS ('AR outstanding', 'receivables', 'unpaid invoices', 'AR')
    
  MEASURE days_order_to_cash 
    AGGREGATION AVERAGE
    SYNONYMS ('DSO', 'days sales outstanding', 'collection period', 'days to collect')
    
  MEASURE days_order_to_invoice
    AGGREGATION AVERAGE
    SYNONYMS ('billing time', 'time to invoice')
    
  MEASURE days_invoice_to_payment
    AGGREGATION AVERAGE
    SYNONYMS ('collection time', 'payment time')
    
  MEASURE days_past_due
    AGGREGATION AVERAGE
    SYNONYMS ('overdue days', 'late days')
;

-- ============================================================================
-- SEMANTIC VIEW 2: O2C Customer Metrics (Aggregated)
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW EDW.O2C_SEMANTIC_VIEWS.O2C_CUSTOMER_METRICS_SEMANTIC
AS
  SELECT
    -- Customer dimensions
    source_system,
    customer_id,
    customer_name,
    customer_type,
    customer_country,
    
    -- Aggregated metrics
    total_orders,
    total_invoices,
    total_payments,
    total_order_amount,
    total_invoice_amount,
    total_payment_amount,
    total_outstanding,
    avg_days_to_cash,
    avg_days_past_due
    
  FROM EDW.O2C_AGGREGATES.AGG_O2C_BY_CUSTOMER

-- Semantic annotations
WITH SEMANTIC
  -- Dimension annotations
  DIMENSION customer_name SYNONYMS ('customer', 'company', 'account')
  DIMENSION customer_type SYNONYMS ('account type')
  DIMENSION customer_country SYNONYMS ('country', 'location')
  
  -- Measure annotations
  MEASURE total_order_amount
    AGGREGATION SUM
    SYNONYMS ('customer revenue', 'customer sales', 'customer order value')
    
  MEASURE total_outstanding
    AGGREGATION SUM
    SYNONYMS ('customer AR', 'customer receivables', 'unpaid amount')
    
  MEASURE avg_days_to_cash
    AGGREGATION AVERAGE
    SYNONYMS ('customer DSO', 'customer collection time')
;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

SELECT '✅ Semantic views created successfully!' as STATUS;

-- Show semantic views
SHOW SEMANTIC VIEWS IN SCHEMA EDW.O2C_SEMANTIC_VIEWS;

-- Describe semantic views
DESCRIBE SEMANTIC VIEW EDW.O2C_SEMANTIC_VIEWS.O2C_RECONCILIATION_SEMANTIC;
DESCRIBE SEMANTIC VIEW EDW.O2C_SEMANTIC_VIEWS.O2C_CUSTOMER_METRICS_SEMANTIC;

-- Test query
SELECT 
    customer_name,
    COUNT(*) as orders,
    SUM(order_amount) as revenue,
    AVG(days_order_to_cash) as avg_dso
FROM EDW.O2C_SEMANTIC_VIEWS.O2C_RECONCILIATION_SEMANTIC
WHERE customer_type = 'E'
GROUP BY customer_name
ORDER BY revenue DESC
LIMIT 5;

SELECT '✅ Semantic views ready for Cortex Analyst!' as STATUS;

