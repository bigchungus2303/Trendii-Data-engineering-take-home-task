{{ config(materialized='view') }}

with mounts_events as (
  select *
  from {{ ref('stg_events_base') }}
  where lower(event_name) = 'mounts'
),
expanded as (
  select
    e.event_created_at,
    e.domain,
    e.url,
    e.publisher_id,
    e.did,
    e.pvid,
    (m->>'image_id')::text    as image_id,
    (m->>'mount_index')::int  as mount_index
  from mounts_events e,
  lateral jsonb_array_elements(e.event_data->'mounts') as m
)
select * from expanded

