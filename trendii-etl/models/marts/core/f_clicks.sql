{{ config(materialized='materialized_view') }}

-- Grain: one product click
select
  c.event_created_at,
  c.domain,
  c.url,
  c.publisher_id,
  c.did,
  c.pvid,
  c.image_id,
  c.product_id,
  c.product_url,
  c.product_name,
  c.product_price,
  c.product_image_url,
  c.brand_id,
  c.click_id
from {{ ref('stg_clicks') }} c

