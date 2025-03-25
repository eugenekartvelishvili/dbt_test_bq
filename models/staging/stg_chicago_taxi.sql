{{ config(materialized='view') }}

-- Берём нужные поля, фильтруем по дате
select
    taxi_id,
    trip_start_timestamp,
    date(trip_start_timestamp) as trip_date,
    trip_end_timestamp,
    trip_miles,
    fare,
    tips
from {{ source('chicago_taxi_trips_source','taxi_trips') }}
where
    trip_start_timestamp >= '2018-01-01'
    and trip_start_timestamp < '2019-01-01'
    and tips is not null