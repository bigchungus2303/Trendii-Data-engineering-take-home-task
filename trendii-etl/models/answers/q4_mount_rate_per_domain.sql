{{ config(materialized='materialized_view') }}

-- "Fill Rate" per prompt = mounts/tagloads (mounts per page load)
with tagloads as (
  select domain, count(*) as tagloads
  from {{ ref('stg_tagloads') }}
  group by 1
),
mounts as (
  select domain, count(*) as mounts
  from {{ ref('f_mounts') }}
  group by 1
)
select
  coalesce(m.domain, t.domain) as domain,
  coalesce(m.mounts, 0) as mounts,
  coalesce(t.tagloads, 0) as tagloads,
  case when coalesce(t.tagloads,0) = 0 then 0
       else coalesce(m.mounts,0)::numeric / t.tagloads::numeric
  end as fill_rate
from mounts m
full join tagloads t using (domain)
order by domain

