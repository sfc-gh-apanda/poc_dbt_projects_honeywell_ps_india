{{
    config(
        materialized='semantic_view'
    )
}}

/*
    O2C Reconciliation Semantic View
    
    Enables Cortex Analyst to answer natural language questions about:
    - Order to cash metrics
    - Customer performance
    - AR outstanding and collections
    - DSO and cycle times
    
    Materialization: Snowflake SEMANTIC VIEW (using dbt_semantic_view package)
*/

TABLES(
    {{ ref('dbt_o2c', 'dm_o2c_reconciliation') }}
)
DIMENSIONS(
    customer_name COMMENT = 'Customer company name' SYNONYMS ('customer', 'company', 'account', 'client'),
    customer_type COMMENT = 'Customer type: E=External, I=Internal' SYNONYMS ('account type', 'customer category'),
    customer_country COMMENT = 'Customer country' SYNONYMS ('country', 'location', 'geography', 'nation'),
    source_system COMMENT = 'Source ERP system (BRP900, CIP900, EEP300)' SYNONYMS ('system', 'ERP', 'source'),
    reconciliation_status COMMENT = 'Status: NOT_INVOICED, NOT_PAID, OPEN, CLOSED' SYNONYMS ('status', 'state', 'reconciliation state'),
    payment_timing COMMENT = 'Payment timing: ON_TIME, LATE, OVERDUE, CURRENT' SYNONYMS ('payment status', 'timeliness', 'on time status'),
    order_date COMMENT = 'Order date',
    invoice_date COMMENT = 'Invoice date' SYNONYMS ('billing date'),
    payment_date COMMENT = 'Payment received date' SYNONYMS ('collection date', 'cash date'),
    due_date COMMENT = 'Invoice due date' SYNONYMS ('payment due date')
)
FACTS(
    order_amount COMMENT = 'Order amount in USD' SYNONYMS ('revenue', 'sales', 'order value', 'sales value'),
    invoice_amount COMMENT = 'Invoice amount in USD' SYNONYMS ('billed amount', 'invoice value'),
    payment_amount COMMENT = 'Payment amount in USD' SYNONYMS ('cash collected', 'collection amount', 'cash received'),
    outstanding_amount COMMENT = 'Outstanding AR amount in USD' SYNONYMS ('AR outstanding', 'receivables', 'unpaid amount', 'AR', 'open AR'),
    unbilled_amount COMMENT = 'Unbilled order amount' SYNONYMS ('not invoiced', 'pending billing'),
    days_order_to_invoice COMMENT = 'Days from order to invoice' SYNONYMS ('billing time', 'time to invoice', 'invoice lag'),
    days_invoice_to_payment COMMENT = 'Days from invoice to payment' SYNONYMS ('collection time', 'payment time', 'time to collect'),
    days_order_to_cash COMMENT = 'Days from order to cash (DSO)' SYNONYMS ('DSO', 'days sales outstanding', 'collection period', 'cycle time', 'cash conversion'),
    days_past_due COMMENT = 'Days past due date' SYNONYMS ('overdue days', 'late days', 'delinquent days'),
    order_quantity COMMENT = 'Order quantity' SYNONYMS ('quantity', 'units')
)
METRICS(
    total_revenue AS SUM(order_amount) COMMENT = 'Total order revenue' SYNONYMS ('total sales', 'total order value'),
    total_invoices AS COUNT(DISTINCT invoice_key) COMMENT = 'Total invoice count' SYNONYMS ('invoice count', 'number of invoices'),
    total_payments AS COUNT(DISTINCT payment_key) COMMENT = 'Total payment count' SYNONYMS ('payment count', 'number of payments'),
    total_ar_outstanding AS SUM(outstanding_amount) COMMENT = 'Total AR outstanding' SYNONYMS ('total receivables', 'total AR', 'unpaid invoices total'),
    avg_dso AS AVG(days_order_to_cash) COMMENT = 'Average days sales outstanding' SYNONYMS ('average DSO', 'mean DSO', 'average collection period'),
    avg_days_to_invoice AS AVG(days_order_to_invoice) COMMENT = 'Average billing time',
    avg_days_to_payment AS AVG(days_invoice_to_payment) COMMENT = 'Average collection time',
    median_dso AS MEDIAN(days_order_to_cash) COMMENT = 'Median DSO',
    total_unbilled AS SUM(unbilled_amount) COMMENT = 'Total unbilled amount'
)
COMMENT = 'O2C Reconciliation semantic view for Cortex Analyst - enables natural language queries about order-to-cash metrics'

