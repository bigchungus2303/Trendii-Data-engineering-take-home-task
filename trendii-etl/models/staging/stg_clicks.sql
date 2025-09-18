{{ config(materialized='view') }}

with click_events as (
  select *
  from {{ ref('stg_events_base') }}
  where lower(event_name) = 'productclick'
)
select
  event_created_at,
  domain,
  url,
  publisher_id,
  did,
  pvid,
  (event_data->>'image_id')::text       as image_id,
  (event_data->>'product_id')::text     as product_id,
  (event_data->>'brand_id')::text       as brand_id,
  (event_data->>'product_url')::text    as product_url,
  (event_data->>'product_name')::text   as product_name,
  (event_data->>'product_price')::numeric as product_price,
  (event_data->>'product_image_url')::text as product_image_url,
  (event_data->>'click_id')::text       as click_id
from click_events

