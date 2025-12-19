{{
    config(
        materialized='semantic_view',
        persist_docs={'relation': true, 'columns': true}
    )
}}

/*
    O2C Enhanced Reconciliation Semantic View
    
    Purpose: Enable Cortex Analyst to answer natural language questions about:
    - Order to cash metrics (DSO, cycle times)
    - Customer performance and AR outstanding
    - Invoice and payment status
    - Data quality and audit trail
    
    Source: dm_o2c_reconciliation (Enhanced with audit columns)
    
    Materialization: Snowflake SEMANTIC VIEW (using dbt_semantic_view package)
    
    Example Questions:
    - "Show me customers with outstanding AR over $50,000"
    - "What's the average DSO for external customers in Germany?"
    - "Which records were updated in the last 24 hours?"
    - "Show me overdue invoices by customer type"
*/

TABLES(
    {{ ref('dbt_o2c_enhanced', 'dm_o2c_reconciliation') }}
)

DIMENSIONS(
    -- ═══════════════════════════════════════════════════════════════
    -- BUSINESS DIMENSIONS
    -- ═══════════════════════════════════════════════════════════════
    
    customer_name 
        COMMENT = 'Customer company name' 
        SYNONYMS ('customer', 'company', 'account', 'client', 'organization'),
    
    customer_type 
        COMMENT = 'Customer type: E=External, I=Internal' 
        SYNONYMS ('account type', 'customer category', 'customer segment', 'customer class'),
    
    customer_country 
        COMMENT = 'Customer country location' 
        SYNONYMS ('country', 'location', 'geography', 'nation', 'region'),
    
    source_system 
        COMMENT = 'Source ERP system (BRP900, CIP900, EEP300, etc.)' 
        SYNONYMS ('system', 'ERP', 'source', 'source ERP', 'ERP system'),
    
    reconciliation_status 
        COMMENT = 'Order-to-cash status: NOT_INVOICED, NOT_PAID, OPEN, CLOSED' 
        SYNONYMS ('status', 'state', 'reconciliation state', 'O2C status', 'order status'),
    
    payment_timing 
        COMMENT = 'Payment timeliness: ON_TIME, LATE, OVERDUE, CURRENT' 
        SYNONYMS ('payment status', 'timeliness', 'on time status', 'payment performance'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- TIME DIMENSIONS
    -- ═══════════════════════════════════════════════════════════════
    
    order_date 
        COMMENT = 'Order placement date'
        SYNONYMS ('order day', 'order timestamp'),
    
    invoice_date 
        COMMENT = 'Invoice generation date' 
        SYNONYMS ('billing date', 'invoice day', 'billed date'),
    
    payment_date 
        COMMENT = 'Payment received date' 
        SYNONYMS ('collection date', 'cash date', 'payment day', 'received date'),
    
    due_date 
        COMMENT = 'Invoice payment due date' 
        SYNONYMS ('payment due date', 'due day', 'maturity date'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- AUDIT & DATA QUALITY DIMENSIONS (Enhanced Feature)
    -- ═══════════════════════════════════════════════════════════════
    
    dbt_environment 
        COMMENT = 'Data environment: dev or prod'
        SYNONYMS ('environment', 'env', 'data environment', 'deployment environment'),
    
    dbt_source_model 
        COMMENT = 'Source dbt model that created this record'
        SYNONYMS ('source model', 'dbt model', 'model name', 'origin model')
)

FACTS(
    -- ═══════════════════════════════════════════════════════════════
    -- FINANCIAL FACTS (Amounts)
    -- ═══════════════════════════════════════════════════════════════
    
    order_amount 
        COMMENT = 'Order amount in USD' 
        SYNONYMS ('revenue', 'sales', 'order value', 'sales value', 'order total'),
    
    invoice_amount 
        COMMENT = 'Invoice amount in USD' 
        SYNONYMS ('billed amount', 'invoice value', 'billing value', 'invoice total'),
    
    payment_amount 
        COMMENT = 'Payment amount received in USD' 
        SYNONYMS ('cash collected', 'collection amount', 'cash received', 'payment total', 'collected amount'),
    
    outstanding_amount 
        COMMENT = 'Outstanding AR amount in USD (invoice - payment)' 
        SYNONYMS ('AR outstanding', 'receivables', 'unpaid amount', 'AR', 'open AR', 'open receivables', 'balance due'),
    
    unbilled_amount 
        COMMENT = 'Unbilled order amount in USD (order - invoice)' 
        SYNONYMS ('not invoiced', 'pending billing', 'unbilled revenue', 'backlog'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- CYCLE TIME FACTS (Days)
    -- ═══════════════════════════════════════════════════════════════
    
    days_order_to_invoice 
        COMMENT = 'Days from order to invoice' 
        SYNONYMS ('billing time', 'time to invoice', 'invoice lag', 'order to invoice days', 'billing lag'),
    
    days_invoice_to_payment 
        COMMENT = 'Days from invoice to payment' 
        SYNONYMS ('collection time', 'payment time', 'time to collect', 'collection period', 'invoice to cash days'),
    
    days_order_to_cash 
        COMMENT = 'Days from order to cash (DSO)' 
        SYNONYMS ('DSO', 'days sales outstanding', 'collection period', 'cycle time', 'cash conversion', 'O2C cycle time', 'order to cash days'),
    
    days_past_due 
        COMMENT = 'Days past invoice due date' 
        SYNONYMS ('overdue days', 'late days', 'delinquent days', 'past due days', 'aging days'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- VOLUME FACTS
    -- ═══════════════════════════════════════════════════════════════
    
    order_quantity 
        COMMENT = 'Order line quantity' 
        SYNONYMS ('quantity', 'units', 'qty', 'order qty', 'volume'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- AUDIT & TRACKING FACTS (Enhanced Feature)
    -- ═══════════════════════════════════════════════════════════════
    
    dbt_created_at 
        COMMENT = 'Record creation timestamp (preserved on updates)'
        SYNONYMS ('created date', 'creation time', 'created timestamp', 'record created'),
    
    dbt_updated_at 
        COMMENT = 'Record last update timestamp (always current)'
        SYNONYMS ('updated date', 'update time', 'last modified', 'modified timestamp', 'last updated'),
    
    dbt_loaded_at 
        COMMENT = 'Load timestamp for this record'
        SYNONYMS ('loaded date', 'load time', 'loaded timestamp', 'ingestion time'),
    
    dbt_row_hash 
        COMMENT = 'MD5 hash for change detection and data quality'
        SYNONYMS ('row hash', 'data hash', 'record hash', 'change hash')
)

METRICS(
    -- ═══════════════════════════════════════════════════════════════
    -- REVENUE & VOLUME METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    total_revenue 
        AS SUM(order_amount) 
        COMMENT = 'Total order revenue across all records' 
        SYNONYMS ('total sales', 'total order value', 'gross revenue', 'total orders'),
    
    avg_order_value 
        AS AVG(order_amount) 
        COMMENT = 'Average order amount' 
        SYNONYMS ('average order', 'mean order value', 'average sale'),
    
    total_orders 
        AS COUNT(DISTINCT order_key) 
        COMMENT = 'Total number of unique orders' 
        SYNONYMS ('order count', 'number of orders', 'order volume'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- AR & COLLECTIONS METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    total_ar_outstanding 
        AS SUM(outstanding_amount) 
        COMMENT = 'Total accounts receivable outstanding' 
        SYNONYMS ('total receivables', 'total AR', 'unpaid invoices total', 'open AR total'),
    
    total_unbilled 
        AS SUM(unbilled_amount) 
        COMMENT = 'Total unbilled revenue' 
        SYNONYMS ('total backlog', 'pending billing total', 'unbilled revenue'),
    
    total_invoices 
        AS COUNT(DISTINCT invoice_key) 
        COMMENT = 'Total number of unique invoices' 
        SYNONYMS ('invoice count', 'number of invoices', 'invoice volume'),
    
    total_payments 
        AS COUNT(DISTINCT payment_key) 
        COMMENT = 'Total number of unique payments' 
        SYNONYMS ('payment count', 'number of payments', 'payment volume'),
    
    billing_rate 
        AS SUM(invoice_amount) / NULLIF(SUM(order_amount), 0) 
        COMMENT = 'Percentage of orders that have been invoiced' 
        SYNONYMS ('invoice rate', 'billed rate', 'billing completion'),
    
    collection_rate 
        AS SUM(payment_amount) / NULLIF(SUM(invoice_amount), 0) 
        COMMENT = 'Percentage of invoices that have been collected' 
        SYNONYMS ('payment rate', 'cash collection rate', 'collection completion'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- PERFORMANCE METRICS (DSO & Cycle Times)
    -- ═══════════════════════════════════════════════════════════════
    
    avg_dso 
        AS AVG(days_order_to_cash) 
        COMMENT = 'Average days sales outstanding (order to cash)' 
        SYNONYMS ('average DSO', 'mean DSO', 'average collection period', 'avg days sales outstanding'),
    
    median_dso 
        AS MEDIAN(days_order_to_cash) 
        COMMENT = 'Median days sales outstanding' 
        SYNONYMS ('median collection period', '50th percentile DSO'),
    
    avg_days_to_invoice 
        AS AVG(days_order_to_invoice) 
        COMMENT = 'Average days from order to invoice' 
        SYNONYMS ('average billing time', 'mean time to invoice', 'avg invoice lag'),
    
    avg_days_to_payment 
        AS AVG(days_invoice_to_payment) 
        COMMENT = 'Average days from invoice to payment' 
        SYNONYMS ('average collection time', 'mean payment time', 'avg time to collect'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- DATA QUALITY METRICS (Enhanced Feature)
    -- ═══════════════════════════════════════════════════════════════
    
    record_count 
        AS COUNT(*) 
        COMMENT = 'Total number of records in the view' 
        SYNONYMS ('row count', 'total records', 'record total'),
    
    distinct_customers 
        AS COUNT(DISTINCT customer_name) 
        COMMENT = 'Number of unique customers' 
        SYNONYMS ('unique customers', 'customer count', 'distinct customer count'),
    
    distinct_countries 
        AS COUNT(DISTINCT customer_country) 
        COMMENT = 'Number of unique countries' 
        SYNONYMS ('unique countries', 'country count'),
    
    records_updated_today 
        AS COUNT_IF(DATE(dbt_updated_at) = CURRENT_DATE()) 
        COMMENT = 'Number of records updated today' 
        SYNONYMS ('today updates', 'records updated today', 'daily updates'),
    
    -- ═══════════════════════════════════════════════════════════════
    -- RISK & AGING METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    overdue_ar_amount 
        AS SUM(CASE WHEN days_past_due > 0 THEN outstanding_amount ELSE 0 END) 
        COMMENT = 'Total AR amount that is past due' 
        SYNONYMS ('overdue receivables', 'late AR', 'past due amount'),
    
    overdue_ar_count 
        AS COUNT_IF(days_past_due > 0 AND outstanding_amount > 0) 
        COMMENT = 'Number of invoices that are past due' 
        SYNONYMS ('overdue count', 'late invoice count', 'delinquent count'),
    
    high_risk_ar_amount 
        AS SUM(CASE WHEN days_past_due > 60 THEN outstanding_amount ELSE 0 END) 
        COMMENT = 'AR amount more than 60 days past due (high risk)' 
        SYNONYMS ('high risk receivables', 'severely overdue AR', '60+ days AR')
)

COMMENT = 'Enhanced O2C Reconciliation semantic view with audit trail and data quality metrics for Cortex Analyst'

