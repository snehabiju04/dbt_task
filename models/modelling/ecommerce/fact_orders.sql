{#
    {{
    config(
        partition_by={
            "field": "order_id",
            "data_type": "integer",
            "granularity": "day"
        },
    )
}}
#}
{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}
with ord as (
    select order_id,
    customer_id,
    order_status,
    order_purchase_timestamp
    from {{ ref('orders') }}
),
ord_item as (
    select order_id,
    product_id,
    price,
    shipping_charges
    from {{ ref('order_items') }}
),
paym as (
    select order_id,
    payment_type,
    payment_value
    from {{ ref('payments') }}
),
final as (
    select ord.order_id,
    ord.customer_id,
    ord_item.product_id,
    ord_item.price,
    ord_item.shipping_charges,
    ord.order_status,
    cast(ord.order_purchase_timestamp as date) as order_date,
    paym.payment_value ,
    paym.payment_type
    from ord
    join ord_item on ord.order_id = ord_item.order_id
    left join paym on ord.order_id = paym.order_id
)
select * from final

