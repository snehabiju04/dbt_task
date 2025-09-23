{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

-- List all orders with items that had no corresponding payment (i.e., orders not fully paid). 
select order_id,
       customer_id,
       product_id,
       price,
       shipping_charges,
       payment_type,
       payment_value
from {{ ref('fact_orders') }}
where payment_value=0.0 
