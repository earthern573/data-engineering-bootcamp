select
    created_at_utc
    , updated_at_utc

from {{ ref('stg_greenery__users') }}
where created_at_utc > updated_at_utc