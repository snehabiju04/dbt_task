WITH date_series AS (
    SELECT day
    FROM UNNEST(GENERATE_DATE_ARRAY('2010-12-01', '2011-12-09')) AS day
),
missing_dates AS (
    SELECT s.day
    FROM date_series s
    LEFT JOIN {{ ref('fact_data') }} f
      ON s.day = DATE(f.invoice_date)
    WHERE f.invoice_date IS NULL
)
SELECT * FROM missing_dates
