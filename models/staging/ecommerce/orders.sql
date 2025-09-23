{#
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id'
    )
}}
#}

{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

{#
{% if is_incremental() %}
    {% set max_loaded_date = 'order_purchase_timestamp > (select coalesce(max(order_purchase_timestamp), "2000-01-01") from ' ~ this ~ ')' %}
{% else %}
    {% set max_loaded_date = '1=1' %}
{% endif %}
#}

{% set ord_relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('my_schema'),
table_pattern=var('ord_ptrn'),
exclude=var('exclude_ptrn'),
database=var('my_database')) %}

with unioned_orders as (
  {% for rel in ord_relations %}
    select order_id,
       customer_id,
       coalesce(order_status,"Unknown") as order_status,
       coalesce(order_purchase_timestamp,'2000-01-01 00:00:00') as order_purchase_timestamp,
       coalesce(order_approved_at,'2000-01-01 00:00:00') as order_approved_at,
       coalesce(order_delivered_timestamp,'2000-01-01 00:00:00') as order_delivered_timestamp,
       coalesce(order_estimated_delivery_date,'2000-01-01') as order_estimated_delivery_date
    from {{ rel }} where order_id IS NOT NULL and customer_id IS NOT NULL
    -- and {{ max_loaded_date }} #}
    {% if not loop.last %} union all {% endif %}
  {% endfor %}
)
select * from unioned_orders


