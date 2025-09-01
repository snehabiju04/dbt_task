select count(*) as total_records,
    count(distinct StockCode) as unique_items,
    count(distinct CustomerID) as unique_customers,
    count(distinct Country) as unique_countries
from {{ ref('stg_clean_data') }}