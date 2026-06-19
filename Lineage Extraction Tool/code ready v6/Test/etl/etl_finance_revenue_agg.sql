-- DOMAIN: finance
-- SUBJECT_AREA: revenue
-- SCHEDULE: 0 3 * * *
-- OWNER: finance-data-team

/*
 * Complex revenue aggregation pipeline
 * Joins orders, customers, products, and regions
 * Produces daily revenue summary by region and category
 */

INSERT INTO frontier_silver.finance_daily_revenue_summary

WITH recent_orders AS (
    SELECT
        o.order_id,
        o.cust_id,
        o.prod_id,
        o.region_id,
        o.order_date,
        o.amount,
        o.status
    FROM frontier_bronze.orders o
    WHERE o.status = 'completed'
      AND o.order_date >= DATE_SUB(CURRENT_DATE(), 30)
),

customer_details AS (
    SELECT
        c.id,
        c.name,
        c.tier,
        c.region_code
    FROM frontier_bronze.customers c
    JOIN frontier_bronze.customer_tiers t
        ON c.tier_id = t.tier_id
),

product_catalog AS (
    SELECT
        p.id,
        p.product_name,
        p.category,
        p.sub_category,
        p.unit_cost
    FROM frontier_bronze.products p
    WHERE p.is_active = 1
)

SELECT
    ro.region_id,
    pc.category,
    pc.sub_category,
    cd.tier AS customer_tier,
    SUM(ro.amount) AS total_revenue,
    COUNT(DISTINCT ro.order_id) AS unique_orders,
    COUNT(DISTINCT ro.cust_id) AS unique_customers,
    AVG(ro.amount) AS avg_order_value,
    MAX(ro.amount) AS max_order_value,
    MIN(ro.amount) AS min_order_value,
    SUM(p.unit_cost) AS total_cost
FROM recent_orders ro
JOIN customer_details cd
    ON ro.cust_id = cd.id
JOIN product_catalog pc
    ON ro.prod_id = pc.id
JOIN frontier_bronze.regions r
    ON ro.region_id = r.region_id
WHERE ro.amount > 0
GROUP BY
    ro.region_id,
    pc.category,
    pc.sub_category,
    cd.tier
HAVING SUM(ro.amount) > 100;


select * from sinha.Onkar_data;
