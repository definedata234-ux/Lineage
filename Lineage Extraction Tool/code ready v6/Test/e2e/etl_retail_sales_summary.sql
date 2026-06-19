-- DOMAIN: Retail
-- SUBJECT_AREA: Sales
-- SCHEDULE: 0 6 * * *
-- OWNER: data-engineering-team

/*
  Pipeline : etl_retail_sales_summary
  Purpose  : Aggregates daily retail transactions into a sales summary table
             and a customer metrics table. These tables feed the Power BI
             Retail Performance dashboard.
*/

CREATE OR REPLACE TABLE gold.retail_sales_summary AS
SELECT
    t.transaction_date,
    t.region,
    t.product_category,
    p.product_name,
    c.customer_segment,
    SUM(t.quantity)           AS total_units_sold,
    SUM(t.sale_amount)        AS total_revenue,
    AVG(t.sale_amount)        AS avg_order_value,
    COUNT(DISTINCT t.customer_id) AS unique_customers
FROM silver.retail_transactions t
JOIN silver.products p
    ON t.product_id = p.product_id
JOIN silver.customers c
    ON t.customer_id = c.customer_id
WHERE t.transaction_date >= DATEADD(day, -90, CURRENT_DATE)
  AND t.status = 'completed'
GROUP BY
    t.transaction_date,
    t.region,
    t.product_category,
    p.product_name,
    c.customer_segment;


CREATE OR REPLACE TABLE gold.customer_revenue_metrics AS
SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.region,
    SUM(t.sale_amount)         AS lifetime_value,
    COUNT(t.transaction_id)    AS total_orders,
    MAX(t.transaction_date)    AS last_purchase_date,
    AVG(t.sale_amount)         AS avg_order_value
FROM silver.customers c
JOIN silver.retail_transactions t
    ON c.customer_id = t.customer_id
WHERE t.status = 'completed'
GROUP BY
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.region;
