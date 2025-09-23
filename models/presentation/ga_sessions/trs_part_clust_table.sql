{% if var("GASESSIONS") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

select
    format_date('%Y-%m', parsed_date) as month,
    sum(t_revenue) as total_transaction_revenue,
    count(distinct session_id) as total_sessions
from {{ ref('ga_sessions_part_clus') }}
group by month order by month
