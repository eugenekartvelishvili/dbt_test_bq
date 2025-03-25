{{ config(materialized='table') }}

select
    taxi_id,
    trip_date,
    -- На случай если есть отрицательные значения
    greatest(trip_miles, 0) as trip_miles,
    fare,
    tips
from {{ ref('stg_chicago_taxi') }}
where fare is not null