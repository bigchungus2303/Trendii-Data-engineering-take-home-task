{{ config(materialized='materialized_view') }}

-- Grain: one mount occurrence
select
  m.event_created_at,
  m.domain,
  m.url,
  m.publisher_id,
  m.did,
  m.pvid,
  m.image_id,
  m.mount_index
from {{ ref('stg_mounts') }} m

