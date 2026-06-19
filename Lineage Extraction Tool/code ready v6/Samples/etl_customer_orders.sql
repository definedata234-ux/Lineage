-- DOMAIN: retail
-- SUBJECT_AREA: orders
-- SCHEDULE: 0 2 * * *
-- OWNER: data-engineering

-- Customer order summary pipeline
-- Tests: SELECT, JOIN, AGGREGATE, FILTER, CTEs, multi-target

WITH completed_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.product_id,
        o.quantity,
        o.unit_price,
        o.quantity * o.unit_price AS line_total
    FROM retail.orders o
    WHERE o.status = 'completed'
      AND o.order_date >= '2024-01-01'
),

customer_totals AS (
    SELECT
        co.customer_id,
        c.customer_name,
        c.region,
        c.customer_segment,
        COUNT(co.order_id)       AS total_orders,
        SUM(co.line_total)       AS total_revenue,
        AVG(co.unit_price)       AS avg_unit_price,
        MAX(co.order_date)       AS last_order_date
    FROM completed_orders co
    JOIN retail.customers c
        ON co.customer_id = c.customer_id
    GROUP BY
        co.customer_id,
        c.customer_name,
        c.region,
        c.customer_segment
)

INSERT INTO gold.customer_order_summary
SELECT
    ct.customer_id,
    ct.customer_name,
    ct.region,
    ct.customer_segment,
    ct.total_orders,
    ct.total_revenue,
    ct.avg_unit_price,
    ct.last_order_date
FROM customer_totals ct
WHERE ct.total_revenue > 0;

CREATE OR REPLACE TABLE gold.high_value_customers
AS
SELECT
    customer_id,
    customer_name,
    region,
    total_revenue
FROM gold.customer_order_summary
WHERE total_revenue >= 10000;
