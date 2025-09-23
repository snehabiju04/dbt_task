{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

-- with c as (
--     customer_id,
--     if date_diff

-- )
select customer_id,
    customer_zip_code_prefix,
    customer_state,
    customer_city
    from {{ ref('customers') }} 