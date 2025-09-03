WITH fact AS (
    SELECT 
        invoice_no,
        stock_code,
        quantity,
        invoice_date,
        unit_price,
        customer_id,
        country,
        (quantity * unit_price) AS revenue,
        DATE(invoice_date) AS sale_date
    FROM {{ ref('clean_orders') }}
),
date_series AS (
    SELECT day
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            (SELECT MIN(sale_date) FROM fact),
            (SELECT MAX(sale_date) FROM fact)
        )
    ) AS day
)
SELECT
    d.day AS invoice_date,
    f.invoice_no,
    f.stock_code,
    f.quantity,
    f.unit_price,
    f.customer_id,
    f.country,
    COALESCE(f.revenue, 0) AS revenue
FROM date_series d
LEFT JOIN fact f
ON d.day = f.sale_date ORDER BY d.day, f.invoice_no
