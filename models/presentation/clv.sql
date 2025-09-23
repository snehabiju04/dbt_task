WITH customer_stats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT invoice_no) AS num_orders,
        SUM(revenue) AS total_revenue,
        MIN(invoice_date) AS first_order_date,
        MAX(invoice_date) AS last_order_date
    FROM {{ ref('fact_data') }}
    GROUP BY customer_id
),
customer_metrics AS (
    SELECT
        customer_id,
        total_revenue / NULLIF(num_orders,0) AS aov, 
        num_orders,
        DATE_DIFF(last_order_date, first_order_date, DAY) AS retention_days
    FROM customer_stats
),
customer_clv AS (
    SELECT
        customer_id,
        aov,
        num_orders,
        retention_days,
        -- safe purchase frequency: divide only if retention_days > 0
        CASE 
            WHEN retention_days IS NOT NULL AND retention_days > 0 THEN CAST(num_orders AS FLOAT64) / retention_days
            ELSE CAST(num_orders AS FLOAT64)
        END AS purchase_frequency,
        -- safe CLV calculation
        aov *
        CASE 
            WHEN retention_days IS NOT NULL AND retention_days > 0 THEN CAST(num_orders AS FLOAT64) / retention_days
            ELSE CAST(num_orders AS FLOAT64)
        END *
        CASE
            WHEN retention_days IS NOT NULL AND retention_days > 0 THEN retention_days
            ELSE 1
        END AS clv
    FROM customer_metrics
)
SELECT *
FROM customer_clv
ORDER BY clv DESC
