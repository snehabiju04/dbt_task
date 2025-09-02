with t_sales as(
    select sum(revenue) as total_sales from {{ref("fct_data")}}
),
order_revenue as (
    select invoice_no, sum(revenue) as order_total
    from {{ ref('fct_data') }} group by invoice_no
),
aov_cte as(
    select avg(order_total) as aov from order_revenue
)
select t_sales.total_sales, aov_cte.aov from t_sales cross join aov_cte