{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

--Find all unique products ordered by each customer—ensure duplicates are not shown. 


-- select distinct
--     o.customer_id,
--     o.product_id,
--     p.product_category_name
-- from {{ ref('fact_orders') }} o  join {{ ref('dim_products') }} p
-- on o.product_id=p.product_id
--This query will process 2.97 MB when run.

select o.customer_id,
       o.product_id,
       p.product_category_name,
       row_number() over (partition by customer_id order by product_id) as rank
from {{ ref('fact_orders') }} o join {{ ref('dim_products') }} p
on o.product_id=p.product_id
where rank=1
--This query will process 2.97 MB when run.



