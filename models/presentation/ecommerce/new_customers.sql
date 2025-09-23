
{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

--List all new customers acquired in the last 30 days along with their first order information. 

with cte1 as (select
  customer_id,
  min(order_date) as first_order_date
from {{ ref('fact_orders') }} 
group by customer_id
having min(order_date) >= '2025-08-01')
-- no data to display since the dataset has order from 2016 to 2018

select 
  c.customer_id,
  c.first_order_date,
  o.order_id,
  o.product_id,
  o.price,
  o.shipping_charges,
  o.order_status,
  o.payment_value,
  o.payment_type
from cte1 c
join {{ ref('fact_orders') }} o
on c.customer_id = o.customer_id
order by c.first_order_date
