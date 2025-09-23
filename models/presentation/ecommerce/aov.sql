{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

-- Use a CTE to calculate the average order value (total spend including fees) per calendar month.
-- In the outer query, select months where average order value exceeds $200. 
with aov as (
    select
        extract(year from order_date) as order_year,
        extract(month from order_date) as order_month,
        avg(payment_value) AS avg_order_value
    from {{ ref('fact_orders') }}
    group by 1, 2
)

select
    order_year,
    order_month,
    avg_order_value
from aov
where avg_order_value > 200
order by order_year, order_month
