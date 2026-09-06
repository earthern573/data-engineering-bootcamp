select
    created_at
    , updated_at

from {{ ref('my_users') }}
where created_at > updated_at