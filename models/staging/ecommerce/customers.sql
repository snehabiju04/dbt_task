{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

{% if is_incremental() %}
  where order_purchase_timestamp > (
    select max(order_purchase_timestamp) from {{ this }}
  )
{% endif %}

{% set customer_relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('my_schema'),
table_pattern=var('cust_ptrn'),
exclude=var('exclude_ptrn'),
database=var('my_database')) %}

with unioned_customers as (
  {% for rel in customer_relations %}
    select customer_id,
    coalesce(customer_zip_code_prefix, 0) AS customer_zip_code_prefix,
    coalesce(customer_city, "Unknown") AS customer_city,
    coalesce(customer_state, "Unknown") AS customer_state
    from {{ rel }} where customer_id IS NOT NULL
    {% if not loop.last %} union all {% endif %}
  {% endfor %}
)

select * from unioned_customers
