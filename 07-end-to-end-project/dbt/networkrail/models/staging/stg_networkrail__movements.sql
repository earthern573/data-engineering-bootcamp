with

source as (

    select * from {{ source('networkrail', 'movements') }}

)

, renamed_recasted as (

    select
        actual_timestamp AS actual_timestamp_utc
        , auto_expected
        , correction_ind
        , current_train_id
        , delay_monitoring_point
        , direction_ind
        , division_code
        , event_source
        , event_type
        , gbtt_timestamp
        , line_ind
        , loc_stanox
        , next_report_run_time
        , next_report_stanox
        , offroute_ind
        , original_loc_stanox
        , original_loc_timestamp
        , planned_event_type
        , planned_timestamp
        , platform
        , reporting_stanox
        , route
        , timetable_variation
        , toc_id
        , train_id
        , train_file_address
        , train_service_code
        , train_terminated
        , variation_status
    from source

)

, final as (

    select
        event_type
        , actual_timestamp_utc
        , event_source
        , train_id
        , variation_status
        , toc_id
    from renamed_recasted

)

select * from final