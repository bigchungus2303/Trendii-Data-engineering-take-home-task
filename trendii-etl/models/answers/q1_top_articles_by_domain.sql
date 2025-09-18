{{ config(materialized='materialized_view') }}

-- Top 5 articles by traffic per domain (traffic = tagloads)
with tl as (
  select domain, url, count(*) as tagloads
  from {{ ref('f_tagloads') }}
  group by 1,2
),
ranked as (
  select
    domain, url, tagloads,
    row_number() over (partition by domain order by tagloads desc, url) as rn
  from tl
)
select domain, url, tagloads
from ranked
where rn <= 5
order by domain, tagloads desc, url

