select
    email

from {{ ref('my_users') }}
where NOT CONTAINS_SUBSTR(email, '@')