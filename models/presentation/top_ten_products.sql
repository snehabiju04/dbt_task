select stock_code, sum(revenue) as tot_revenue
from {{ ref("fact_data") }}
group by stock_code
order by tot_revenue desc
limit 15
