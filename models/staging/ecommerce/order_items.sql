{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

{% set item_relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('my_schema'),
table_pattern=var('ord_items_ptrn'),
exclude=var('exclude_ptrn'),
database=var('my_database')) %}

with unioned_ord_items as (
  {% for rel in item_relations %}
    select order_id,
       product_id,
       coalesce(seller_id,"Unknown") as seller_id,
       coalesce(price, 0.0) as price,
       coalesce(shipping_charges, 0.0) as shipping_charges
    from {{ rel }} where order_id IS NOT NULL and product_id IS NOT NULL
    {% if not loop.last %} union all {% endif %}
  {% endfor %}
)
select * from unioned_ord_items
