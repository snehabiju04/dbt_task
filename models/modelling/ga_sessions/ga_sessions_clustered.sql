{% if var("GASESSIONS") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

{{ 
    config(
        materialized='table',
        cluster_by=["visitor_id"]   
    )
}}

select *
from {{ ref('ga_session_table') }}
