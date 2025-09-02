select count(*) as total_records,
    count(distinct stock_code) as unique_items,
    count(distinct customer_id) as unique_customers,
    count(distinct country) as unique_countries
from {{ ref('clean_orders') }}