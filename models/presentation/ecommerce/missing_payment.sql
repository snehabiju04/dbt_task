{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

--List all orders with missing values for payment_method or with negative payment_fee. 

SELECT order_id,
       customer_id,
       product_id,
       payment_type,
       payment_value,
FROM {{ ref('fact_orders') }}
WHERE payment_type ="Unknown"
   OR payment_value < 0
ORDER BY order_id
