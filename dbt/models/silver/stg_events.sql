{{ config(materialized='view', schema='silver') }}

select
    event_id,
    occurred_at,
    (payload ->> 'value')::numeric as event_value,
    ingested_at
from {{ source('bronze', 'raw_events') }} as r
where r.payload ? 'value'
  and not exists (
    select 1
    from {{ source('bronze', 'quarantine_events') }} as q
    where q.event_id = r.event_id
  )
