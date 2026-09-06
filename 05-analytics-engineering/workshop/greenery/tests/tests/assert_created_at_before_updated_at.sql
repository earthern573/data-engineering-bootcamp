select
    created_at
    , updated_at

from {{ ref('stg_greenery__users') }}
where created_at > updated_at