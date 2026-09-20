with

movements as (

    select * from {{ ref('stg_networkrail__movements') }}

)

, operating_companies as (

    select * from {{ ref('stg_networkrail__operating_companies') }}

)

, joined as (

    select
        event_type
        , actual_timestamp_utc
        , event_source
        , train_id
        , variation_status
        , m.toc_id as toc_id

    from movements as m
    left join operating_companies as oc
        on m.toc_id = oc.toc_id

)

, final as (

    select
        event_type
        , actual_timestamp_utc
        , event_source
        , train_id
        , variation_status
        , toc_id

    from joined

)

select * from final