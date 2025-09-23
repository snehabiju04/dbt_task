{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}


{% set pay_relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('my_schema'),
table_pattern=var('pay_ptrn'),
exclude=var('exclude_ptrn'),
database=var('my_database')) %}

with unioned_payments as (
  {% for rel in pay_relations %}
    select order_id,
       coalesce(payment_type,"Unknown") as payment_type,
       coalesce(payment_installments,0) as payment_installments,
       coalesce(payment_value,0.0) as payment_value
    from {{ rel }} where order_id IS NOT NULL 
    {% if not loop.last %} union all {% endif %}
  {% endfor %}
)
select * from unioned_payments


