--  Identify customers who have ordered the same product more than once, 
--  showing only one row per customer-product pair.

{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

select
    o.customer_id,
    o.product_id,
    p.product_category_name,
    count(*) as order_count
from {{ ref('fact_orders') }} o join {{ ref('dim_products') }} p
on o.product_id=p.product_id
group by o.customer_id,
o.product_id, p.product_category_name 
having count(o.product_id) > 1