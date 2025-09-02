select format_date('%Y-%m', date(invoice_date)) as year_month, sum(revenue) as monthly_sales
   from {{ ref('fct_data') }} group by year_month