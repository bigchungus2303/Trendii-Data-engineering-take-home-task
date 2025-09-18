{{ config(materialized='view') }}

-- Treat each unique pvid as one tagload (page view).
-- Use all events to reconstruct tagloads in case a dedicated "tagload" event isn't present.
with tagloaded as (
  select
    lower(event_name)                   as event_name_lc,
    event_created_at,
    (event_context->>'pvid')::text         as pvid,
    (event_context->>'domain')::text       as domain,
    (event_context->>'url')::text          as url,
    (event_context->>'publisher_id')::text as publisher_id,
    (event_context->>'did')::text          as did,
    (event_context->>'ua')::text           as ua
  from {{ ref('stg_events_base') }}
  where (event_context->>'pvid') is not null
    and lower(event_name) = 'tagloaded'          -- matches “TagLoaded”, “tagloaded”, etc.
),
dedup as (
  -- one tagload per pvid: earliest explicit TagLoaded
  select distinct on (pvid)
    pvid,
    domain,
    url,
    publisher_id,
    did,
    ua,
    event_created_at as tagload_at
  from tagloaded
  order by pvid, event_created_at
)
select * from dedup
