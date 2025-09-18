{{ config(materialized='table') }}

select * from {{ ref('dim_product') }}  -- seeded

