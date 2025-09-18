-- Custom test for product pricing logic

{{ config(severity = 'error') }}

select 
    id,
    sku,
    price,
    sale_price
from {{ ref('dim_product') }}
where 
    -- Price should be positive
    price <= 0
    or  
    -- Sale price should be positive when not null
    (sale_price is not null and sale_price <= 0)