select
    event_id,
    occurred_at,
    (payload ->> 'value')::numeric as event_value,
    ingested_at
from {{ source('bronze', 'raw_events') }}
where payload ? 'value'
