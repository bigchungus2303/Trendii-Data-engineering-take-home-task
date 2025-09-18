{{ config(materialized='materialized_view') }}

-- Grain: one product impression
select
  i.event_created_at,
  i.domain,
  i.url,
  i.eid,
  i.publisher_id,
  i.did,
  i.pvid,
  i.image_id,
  i.product_id,
  i.product_url,
  i.product_name,
  i.product_price,
  i.product_image_url,
  i.brand_id
from {{ ref('stg_impressions') }} i

