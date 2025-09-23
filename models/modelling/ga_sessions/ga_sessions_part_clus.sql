{% if var("GASESSIONS") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

{{ 
    config(
        materialized='table',
        partition_by={
          "field": "parsed_date",
          "data_type": "date"
        },
        cluster_by=["visitor_id"]
    )
}}

select *
from {{ ref('ga_session_table') }}
