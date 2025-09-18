{{ config(materialized='view') }}

with base as (
  select
    event_name,
    event_created_at,
    event_data,
    event_context,
    -- common context
    (event_context->>'eid')::text          as eid,
    (event_context->>'publisher_id')::text as publisher_id,
    (event_context->>'domain')::text       as domain,
    (event_context->>'url')::text          as url,
    (event_context->>'did')::text          as did,
    (event_context->>'ua')::text           as ua,
    (event_context->>'pvid')::text         as pvid
  from {{ ref('src_events_raw') }}
)
select * from base

