{{ config(materialized='table') }}

select * from {{ ref('dim_campaign') }}  -- seeded

