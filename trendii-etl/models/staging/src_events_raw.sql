-- Source convenience layer
{{ config(materialized='view') }}

select * from {{ source('raw','events') }}

