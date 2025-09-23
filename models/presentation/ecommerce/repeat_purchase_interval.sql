{% if var("ECOMMERCE") %} 
  {{ config(enabled=True, materialized='view') }}
{% else %}
  {{ config(enabled=False) }} 
{% endif %}

{{
  config(
    materialized='view'
  )
}}
-- Create a view showing all customers who made repeat purchases, the order dates,
-- and the interval in days between their consecutive orders.

select
  customer_id,
  order_date,
  lead(order_date) over (
    partition by customer_id
    order by order_date
  ) as next_order_date,
  date_diff (
    lead(order_date) over (
      partition by customer_id
      order by order_date
    ),
    order_date,
    day
  ) as days_between_orders
from {{ ref('fact_orders') }}

