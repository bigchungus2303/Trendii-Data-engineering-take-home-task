{{ config(materialized='materialized_view') }}

-- "Fill Rate" per prompt = mounts/tagloads (mounts per page load)
with tl as (
  select lower(domain) as domain, count(*) as tagloads
  from {{ ref('stg_tagloads') }} group by 1
),
m as (
  select lower(domain) as domain, count(distinct pvid) as mounts
  from {{ ref('stg_mounts') }} group by 1
)
select
  coalesce(t.domain, m.domain) as domain,
  coalesce(m.mounts, 0)        as mounts,
  coalesce(t.tagloads, 0)      as tagloads,
  coalesce(m.mounts, 0)::numeric / nullif(coalesce(t.tagloads, 0), 0) as fill_rate
from tl t
full join m using (domain)
order by domain;

