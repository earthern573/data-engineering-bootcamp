select
    created_at

from {{ ref('my_events') }}
where created_at > CURRENT_TIMESTAMP()