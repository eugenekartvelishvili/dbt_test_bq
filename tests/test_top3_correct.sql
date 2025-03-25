-- Выбираем те машины, которые dbt поместил в финальную таблицу
-- но у которых rank > 3 (то есть они не должны были попасть в топ-3).
-- Если вернется 0 строк — всё ок, иначе ошибка.

with april_ranking as (
  select
    taxi_id,
    tips_sum,
    rank() over (order by tips_sum desc) as rnk
  from (
    -- Здесь берём "истинную" агрегату:
    select
      taxi_id,
      sum(tips) as tips_sum
    from {{ source('chicago_taxi_trips_source','taxi_trips') }}
    where EXTRACT(YEAR FROM trip_start_timestamp) = 2018
      and EXTRACT(MONTH FROM trip_start_timestamp) = 4
    group by taxi_id
  )
)

select
  a.taxi_id,
  a.tips_sum,
  a.rnk
from april_ranking a
join {{ ref('fct_top3_tips_monthly') }} f
  on a.taxi_id = f.taxi_id
 where f.year_month = '2018-04'
   and a.rnk > 3