{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

--. Count the number of repeat customers—those who placed more than one order—last month. 

with c as (
    select customer_id,
    order_id from {{ ref('fact_orders') }} 
    where order_date>='2025-08-01'
)
select
  customer_id,
  count(*) as total_orders
from c 
group by customer_id
having total_orders >1