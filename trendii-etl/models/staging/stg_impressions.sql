{{ config(materialized='view') }}

with imp_events as (
  select *
  from {{ ref('stg_events_base') }}
  where lower(event_name) ='productimpressions'
),
expanded as (
  select
    e.event_created_at,
    e.eid,
    e.domain,
    e.url,
    e.publisher_id,
    e.did,
    e.pvid,
    (e.event_data->>'image_id')::text              as image_id,
    (p->>'product_id')::text                       as product_id,
    (p->>'brand_id')::text                         as brand_id,
    (p->>'product_url')::text                      as product_url,
    (p->>'product_name')::text                     as product_name,
    (p->>'product_price')::numeric                 as product_price,
    (p->>'product_image_url')::text                as product_image_url
  from imp_events e,
  lateral jsonb_array_elements(e.event_data->'products') as p
)
select * from expanded

