{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

-- Count the number of orders for which quantity or item price is zero or negative.

select order_id,
       customer_id,
       product_id,
       price
from {{ ref('fact_orders') }}
where price <= 0
order by order_id