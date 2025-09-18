{{ config(materialized='materialized_view') }}

-- Unique users advertised to = distinct device IDs that received an impression
select
  count(distinct did) as unique_users_advertised_to
from {{ ref('f_impressions') }}

