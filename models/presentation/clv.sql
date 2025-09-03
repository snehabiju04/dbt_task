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
        total_revenue / num_orders AS aov, 
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
        CASE 
            WHEN retention_days > 0 THEN num_orders / retention_days
            ELSE num_orders
        END AS purchase_frequency,
        aov * 
        CASE 
            WHEN retention_days > 0 THEN num_orders / retention_days
            ELSE num_orders
        END * retention_days AS clv
    FROM customer_metrics
)
SELECT * FROM customer_clv ORDER BY clv DESC
