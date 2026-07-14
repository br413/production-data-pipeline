{{ config(materialized='table', schema='gold') }}

select
    date_trunc('day', occurred_at) as event_date,
    count(*) as event_count,
    sum(event_value) as total_value,
    avg(event_value) as avg_value
from {{ ref('stg_events') }}
group by 1
