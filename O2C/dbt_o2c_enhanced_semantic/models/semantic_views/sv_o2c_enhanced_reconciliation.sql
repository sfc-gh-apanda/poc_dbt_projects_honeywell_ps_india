{{
    config(
        materialized='semantic_view',
        persist_docs={'relation': true, 'columns': true}
    )
}}

/*
    O2C Enhanced Reconciliation Semantic View
    
    Enables Cortex Analyst natural language queries about:
    - Order to cash metrics, customer performance, AR outstanding
    - Includes audit columns for data quality queries
    
    Source: EDW.O2C_ENHANCED_CORE.DM_O2C_RECONCILIATION
*/

TABLES(
    {{ source('o2c_enhanced_core', 'dm_o2c_reconciliation') }}
)
DIMENSIONS(
    customer_name COMMENT = 'Customer company name' SYNONYMS ('customer', 'company', 'account', 'client'),
    customer_type COMMENT = 'Customer type: E=External, I=Internal' SYNONYMS ('account type', 'customer category'),
    customer_country COMMENT = 'Customer country' SYNONYMS ('country', 'location', 'geography'),
    source_system COMMENT = 'Source ERP system' SYNONYMS ('system', 'ERP', 'source'),
    reconciliation_status COMMENT = 'Status: NOT_INVOICED, NOT_PAID, OPEN, CLOSED' SYNONYMS ('status', 'state'),
    payment_timing COMMENT = 'Payment timing: ON_TIME, LATE, OVERDUE, CURRENT' SYNONYMS ('payment status', 'timeliness'),
    order_date COMMENT = 'Order date',
    invoice_date COMMENT = 'Invoice date' SYNONYMS ('billing date'),
    payment_date COMMENT = 'Payment received date' SYNONYMS ('collection date', 'cash date'),
    due_date COMMENT = 'Invoice due date' SYNONYMS ('payment due date'),
    dbt_environment COMMENT = 'Environment: dev or prod' SYNONYMS ('environment', 'env'),
    dbt_source_model COMMENT = 'Source dbt model' SYNONYMS ('source model', 'model name')
)
FACTS(
    order_amount COMMENT = 'Order amount in USD' SYNONYMS ('revenue', 'sales', 'order value'),
    invoice_amount COMMENT = 'Invoice amount in USD' SYNONYMS ('billed amount', 'invoice value'),
    payment_amount COMMENT = 'Payment amount in USD' SYNONYMS ('cash collected', 'collection amount'),
    outstanding_amount COMMENT = 'Outstanding AR in USD' SYNONYMS ('AR outstanding', 'receivables', 'AR', 'open AR'),
    unbilled_amount COMMENT = 'Unbilled amount in USD' SYNONYMS ('not invoiced', 'pending billing'),
    days_order_to_invoice COMMENT = 'Days from order to invoice' SYNONYMS ('billing time', 'time to invoice'),
    days_invoice_to_payment COMMENT = 'Days from invoice to payment' SYNONYMS ('collection time', 'payment time'),
    days_order_to_cash COMMENT = 'Days from order to cash (DSO)' SYNONYMS ('DSO', 'days sales outstanding', 'collection period'),
    days_past_due COMMENT = 'Days past due date' SYNONYMS ('overdue days', 'late days'),
    order_quantity COMMENT = 'Order quantity' SYNONYMS ('quantity', 'units'),
    dbt_created_at COMMENT = 'Record creation timestamp' SYNONYMS ('created date', 'creation time'),
    dbt_updated_at COMMENT = 'Last update timestamp' SYNONYMS ('updated date', 'last modified'),
    dbt_loaded_at COMMENT = 'Load timestamp' SYNONYMS ('loaded date', 'load time'),
    dbt_row_hash COMMENT = 'Data hash for change detection' SYNONYMS ('row hash', 'data hash')
)
METRICS(
    total_revenue AS SUM(order_amount) COMMENT = 'Total order revenue' SYNONYMS ('total sales', 'total order value'),
    avg_order_value AS AVG(order_amount) COMMENT = 'Average order amount' SYNONYMS ('average order', 'mean order value'),
    total_orders AS COUNT(DISTINCT order_key) COMMENT = 'Total number of orders' SYNONYMS ('order count', 'number of orders'),
    total_ar_outstanding AS SUM(outstanding_amount) COMMENT = 'Total AR outstanding' SYNONYMS ('total receivables', 'total AR'),
    total_unbilled AS SUM(unbilled_amount) COMMENT = 'Total unbilled revenue' SYNONYMS ('total backlog', 'pending billing'),
    total_invoices AS COUNT(DISTINCT invoice_key) COMMENT = 'Total invoice count' SYNONYMS ('invoice count', 'number of invoices'),
    total_payments AS COUNT(DISTINCT payment_key) COMMENT = 'Total payment count' SYNONYMS ('payment count', 'number of payments'),
    billing_rate AS SUM(invoice_amount) / NULLIF(SUM(order_amount), 0) COMMENT = 'Percentage invoiced' SYNONYMS ('invoice rate', 'billed rate'),
    collection_rate AS SUM(payment_amount) / NULLIF(SUM(invoice_amount), 0) COMMENT = 'Percentage collected' SYNONYMS ('payment rate', 'collection completion'),
    avg_dso AS AVG(days_order_to_cash) COMMENT = 'Average DSO' SYNONYMS ('average days sales outstanding', 'mean DSO'),
    median_dso AS MEDIAN(days_order_to_cash) COMMENT = 'Median DSO' SYNONYMS ('median collection period'),
    avg_days_to_invoice AS AVG(days_order_to_invoice) COMMENT = 'Average days to invoice' SYNONYMS ('average billing time'),
    avg_days_to_payment AS AVG(days_invoice_to_payment) COMMENT = 'Average days to payment' SYNONYMS ('average collection time'),
    record_count AS COUNT(*) COMMENT = 'Total records' SYNONYMS ('row count', 'total records'),
    distinct_customers AS COUNT(DISTINCT customer_name) COMMENT = 'Unique customers' SYNONYMS ('customer count', 'unique customers'),
    distinct_countries AS COUNT(DISTINCT customer_country) COMMENT = 'Unique countries' SYNONYMS ('country count'),
    records_updated_today AS COUNT_IF(DATE(dbt_updated_at) = CURRENT_DATE()) COMMENT = 'Records updated today' SYNONYMS ('today updates', 'daily updates'),
    overdue_ar_amount AS SUM(CASE WHEN days_past_due > 0 THEN outstanding_amount ELSE 0 END) COMMENT = 'Overdue AR amount' SYNONYMS ('overdue receivables', 'late AR'),
    overdue_ar_count AS COUNT_IF(days_past_due > 0 AND outstanding_amount > 0) COMMENT = 'Overdue invoice count' SYNONYMS ('overdue count', 'late invoices'),
    high_risk_ar_amount AS SUM(CASE WHEN days_past_due > 60 THEN outstanding_amount ELSE 0 END) COMMENT = 'AR over 60 days past due' SYNONYMS ('high risk receivables', '60+ days AR')
)
COMMENT = 'Enhanced O2C Reconciliation semantic view with audit trail for Cortex Analyst'
