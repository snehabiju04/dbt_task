{% if var("GASESSIONS") %} 
  {{ config(enabled=True) }}
{% else %}
  {{ config(enabled=False) }} 
{% endif %}

{{ config(materialized="table") }}

with jobs as (
    select
      job_id,
      query,
      total_bytes_processed as bytes_processed,
      total_slot_ms as slot_ms,
      timestamp_diff(end_time, start_time, second) as runtime_seconds,
      creation_time
    from `region-us`.INFORMATION_SCHEMA.JOBS
    where project_id = 'dbtpractice-470116'
      and statement_type = 'CREATE_TABLE_AS_SELECT'
      and (
        query like '%trs_raw_table%'
        or query like '%trs_partitioned_table%'
        or query like '%trs_clustered_table%'
        or query like '%trs_part_clust_table%'
      )
)

select
  case
    when query like '%trs_raw_table%' then 'trs_raw_table'
    when query like '%trs_partitioned_table%' then 'trs_partitioned_table'
    when query like '%trs_clustered_table%' then 'trs_clustered_table'
    when query like '%trs_part_clust_table%' then 'trs_part_clust_table'
    else 'other'
  end as model_name,
  bytes_processed,
  (bytes_processed / pow(2,40) * 6.25) as estimated_cost,  
  slot_ms,
  runtime_seconds,
  creation_time
from jobs
order by creation_time desc
limit 4
