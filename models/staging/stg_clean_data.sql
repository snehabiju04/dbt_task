with source_data as (
    select safe_cast(InvoiceNo as int64)as invoice_no,
       safe_cast(StockCode as STRING) as stock_code ,
       safe_cast(Quantity as int64) as quantity ,
       safe_cast(InvoiceDate as TIMESTAMP) as invoice_date ,
       safe_cast(UnitPrice as float64) as unit_price ,
       safe_cast(CustomerID as int64) as customer_id ,
       safe_cast(Country as string) as country       
 from {{source("dbttask","data")}}
),

deduplicated_data as (
    select distinct * from source_data
),

final_cleaned_data as (
    select  invoice_no, 
        coalesce(stock_code,"not found") as stock_code ,
        coalesce(quantity,0) as quantity,
        coalesce(invoice_date,timestamp '1900-01-01 00:00:00') as invoice_date,
        coalesce(unit_price,0) as unit_price,
        customer_id,
        coalesce(country,"not found") as country,
        from deduplicated_data where customer_id is not null and invoice_no is not null
)

select * from final_cleaned_data
 