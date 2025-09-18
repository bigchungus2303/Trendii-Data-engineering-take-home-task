{{ config(materialized='materialized_view') }}

select 
    pvid,
    domain,
    url,
    publisher_id,
    did,
    ua,
    tagload_at
  from {{ ref('stg_tagloads') }} 