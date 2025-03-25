{{ config(
    materialized='incremental',
    unique_key='taxi_id_year_month'
) }}

with base as (
    select
        taxi_id,
        year_month,
        tips_sum
    from {{ ref('int_monthly_tips') }}
),

top_3 as (
    select
        taxi_id
    from base
    where year_month = '2018-04'
    order by tips_sum desc
    limit 3
),

top_3_monthly as (
    select
        b.taxi_id,
        b.year_month,
        b.tips_sum
    from base b
    join top_3 t on b.taxi_id = t.taxi_id
    where b.year_month >= '2018-04'
),

tips_with_prev as (
    select
        t.taxi_id,
        t.year_month,
        t.tips_sum,
        lag(t.tips_sum) over (
            partition by t.taxi_id
            order by t.year_month
        ) as prev_tips_sum
    from top_3_monthly t
)

select
    taxi_id,
    year_month,
    tips_sum,
    case
        when prev_tips_sum is null then null
        else round(((tips_sum - prev_tips_sum) / prev_tips_sum)*100, 2)
    end as tips_change,
    concat(taxi_id, '_', year_month) as taxi_id_year_month
from tips_with_prev

{% if is_incremental() %}
  where year_month > (
    select coalesce(max(year_month), '2018-04')
    from {{ this }}
  )
{% endif %}