{% if var("GASESSIONS") %} 
 {{ config(enabled=True) }}
{% else %}
 {{ config(enabled=False) }} 
{% endif %}

{{ 
    config(
    materialized = 'table',
    partition_by = {
      "field": "parsed_date", 
      "data_type": "date"
    }
) 
}}
 --since it is a sharded data whcih contains daily data from 2016-2017 parsed_date is used here
select * from {{ ref("ga_session_table") }}
