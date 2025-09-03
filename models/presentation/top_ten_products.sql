WITH product_quantity AS (
    SELECT
        stock_code,
        SUM(quantity) AS total_quantity
    FROM {{ ref("fact_data") }}
    GROUP BY stock_code
),
ranked_products AS (
    SELECT
        stock_code,
        total_quantity,
        ROW_NUMBER() OVER (ORDER BY total_quantity DESC) AS rn
    FROM product_quantity
)
SELECT
    stock_code,
    total_quantity
FROM ranked_products
WHERE rn <= 10
ORDER BY total_quantity DESC
