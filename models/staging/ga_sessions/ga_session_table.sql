{% if var("GASESSIONS") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

select
       coalesce(visitId,0) as session_id,
       coalesce(visitNumber,0) as session_no,
       coalesce(fullVisitorId,"unknown") as visitor_id,
       parse_date("%Y%m%d",date) as og_date,
       date_add(parse_date('%Y%m%d', date), interval 8 year) as parsed_date,
       coalesce(totals.transactionRevenue,0) as t_revenue

from {{ source('google_analytics_sample', 'ga_sessions') }}