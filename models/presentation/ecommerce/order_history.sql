{% if var("ECOMMERCE") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

--Retrieve the complete order history for a customer (customer_id = 101). 
--Show order date, product name, quantity, item price, payment fee, and total amount per order. 
--List, for each order, all associated payment methods and related customer information.

select o.order_date,
       p.product_category_name,
       o.price,
       o.payment_value,
       o.payment_type,
       c.customer_city,
       c.customer_state
       from {{ ref('fact_orders') }} o
       join {{ ref('dim_customers') }} c
       on o.customer_id=c.customer_id
       join {{ ref('dim_products') }} p
       on o.product_id=p.product_id
       where o.customer_id='8Ju4umL7b5Ke'
