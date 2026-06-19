-- DOMAIN: finance
-- SUBJECT_AREA: risk_analytics
-- SCHEDULE: 0 2 * * *
-- OWNER: risk-modeling-team

-- Test case: Multiple CTEs, self-joins, window functions, subqueries
WITH daily_transactions AS (
    SELECT 
        t.transaction_id,
        t.account_id,
        t.amount,
        t.transaction_date,
        ROW_NUMBER() OVER (PARTITION BY t.account_id ORDER BY t.transaction_date DESC) as rn
    FROM frontier_bronze.transactions t
    WHERE t.transaction_date >= DATE_SUB(CURRENT_DATE(), 365)
),

account_summary AS (
    SELECT 
        a.account_id,
        a.customer_id,
        SUM(dt.amount) as total_amount,
        AVG(dt.amount) as avg_transaction,
        MAX(dt.amount) as max_transaction,
        COUNT(*) as transaction_count
    FROM daily_transactions dt
    JOIN frontier_bronze.accounts a ON dt.account_id = a.account_id
    WHERE dt.rn <= 100
    GROUP BY a.account_id, a.customer_id
),

customer_risk_score AS (
    SELECT 
        c.customer_id,
        c.name,
        asumm.total_amount,
        CASE 
            WHEN asumm.total_amount > 1000000 THEN 'HIGH'
            WHEN asumm.total_amount > 100000 THEN 'MEDIUM'
            ELSE 'LOW'
        END as risk_category,
        ROW_NUMBER() OVER (ORDER BY asumm.total_amount DESC) as risk_rank
    FROM account_summary asumm
    JOIN frontier_bronze.customers c ON asumm.customer_id = c.customer_id
)

INSERT INTO frontier_gold.customer_risk_analysis
SELECT 
    crs.customer_id,
    crs.name,
    crs.total_amount,
    crs.risk_category,
    crs.risk_rank,
    CURRENT_TIMESTAMP() as processed_date
FROM customer_risk_score crs
WHERE crs.risk_rank <= 10000;