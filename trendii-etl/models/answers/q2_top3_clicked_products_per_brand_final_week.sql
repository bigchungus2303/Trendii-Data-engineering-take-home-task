{{ config(materialized='materialized_view') }}

with bounds as (
    select
        max(event_created_at)::date as max_day,
        (max(event_created_at)::date - interval '6 day')::date as min_day
    from {{ ref('f_clicks') }}
),
filtered as (
    select c.brand_id, c.product_id, c.product_name, count(*) as clicks
    from {{ ref('f_clicks') }} c
    join bounds b
      on c.event_created_at::date between b.min_day and b.max_day
    group by 1,2,3
),
ranked as (
    select
        brand_id, product_id, product_name, clicks,
        row_number() over (partition by brand_id order by clicks desc, product_id) as rn
    from filtered
)
select
    r.brand_id,
    r.product_id,
    r.product_name,
    r.clicks
from ranked r
left join {{ ref('dim_product') }} p
  on r.product_id = p.id
where r.rn <= 3
order by r.brand_id, r.clicks desc, r.product_id


