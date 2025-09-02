select invoice_no, stock_code, quantity, invoice_date, unit_price, customer_id, country,
       (quantity * unit_price) as revenue
       from {{ref("stg_clean_data")}}
