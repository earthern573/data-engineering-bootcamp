{{
  config(
    materialized="table",
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

select
    event_id
    , event_type
    , user_id
    , created_at

from {{ source('greenery', 'events') }}