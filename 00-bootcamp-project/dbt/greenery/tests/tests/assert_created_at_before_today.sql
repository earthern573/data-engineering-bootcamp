select
    created_at

from {{ ref('stg_greenery__events') }}
where created_at > CURRENT_TIMESTAMP()