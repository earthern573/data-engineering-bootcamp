select
    email

from {{ ref('stg_greenery__users') }}
where NOT CONTAINS_SUBSTR(email, '@')