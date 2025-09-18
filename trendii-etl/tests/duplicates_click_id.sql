-- fail if any duplicate click_id appears
with d as (
  select click_id, count(*) c
  from {{ ref('stg_clicks') }}
  group by click_id
  having count(*) > 1
)
select * from d