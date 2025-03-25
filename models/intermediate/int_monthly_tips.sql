{{ config(materialized='table') }}

with monthly_tips as (
    select
        taxi_id,
        FORMAT_DATE('%Y-%m', DATE_TRUNC(trip_date, MONTH)) as year_month,
        sum(tips) as tips_sum
    from {{ ref('int_cleaned_chicago_taxi') }}
    group by 1, 2
)

select
    taxi_id,
    year_month,
    tips_sum
from monthly_tips