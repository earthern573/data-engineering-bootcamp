with

source as (

    select * from {{ source('networkrail', 'movements') }}

)

, renamed_recasted as (

    -- Your code here
    select * from source

)

, final as (

    -- Your code here
    select * from renamed_recasted

)

select * from final
