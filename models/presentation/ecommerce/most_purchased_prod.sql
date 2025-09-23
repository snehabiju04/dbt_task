{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

-- For each product, rank it by number of times it appears in orders, across all customers.

select o.product_id,
       p.product_category_name,
       count(o.product_id) as purchase_count,
       dense_rank() over (order by count(o.product_id) desc) as rank
       from {{ ref('fact_orders') }} o 
       join {{ ref('dim_products') }} p
       on o.product_id = p.product_id
       group by o.product_id, p.product_category_name
       order by purchase_count desc

