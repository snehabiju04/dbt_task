{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}


{% set prod_relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('my_schema'),
table_pattern=var('prod_ptrn'),
exclude=var('exclude_ptrn'),
database=var('my_database')) %}

with unioned_prod as (
  {% for rel in prod_relations %}
    select product_id,
       coalesce(product_category_name,"Unknown") as product_category_name,
       coalesce(product_weight_g, 0.0) as product_weight_g,
       coalesce(product_length_cm, 0.0) as product_length_cm,
       coalesce(product_height_cm, 0.0) as product_height_cm,
    from {{ rel }} where product_id IS NOT NULL 
    {% if not loop.last %} union all {% endif %}
  {% endfor %}
)
select * from unioned_prod