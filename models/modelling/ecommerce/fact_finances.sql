{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

select order_id,
       payment_type,
       payment_installments,
       payment_value
       from {{ ref('payments') }}
