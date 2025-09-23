{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

-- Compute total spending (including payment_fee) for each customer
-- rank customers by total spend descending. Show the top 10. 

select customer_id,
price as product_price,
shipping_charges,
(payment_value - (shipping_charges+price)) as additional_charges,
sum(payment_value) as total_spending
from {{ ref('fact_orders') }} 
group by customer_id
order by total_spending desc
limit 10
