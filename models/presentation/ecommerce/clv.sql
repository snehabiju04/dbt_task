{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

-- wouldnt work since all customers have only 1 order in the sample data
with clv as (select customer_id,
       avg(payment_value) as average_order_value,
       count(order_id) as total_orders,
       date_diff(max(order_date), min(order_date),day) as retention_days
from {{ ref('fact_orders') }}
group by customer_id)
select customer_id,
       (average_order_value* total_orders*retention_days) as clv_value
       from clv