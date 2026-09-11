select
    event_created_at_utc

from {{ ref('stg_greenery__events') }}
where event_created_at_utc > CURRENT_TIMESTAMP()