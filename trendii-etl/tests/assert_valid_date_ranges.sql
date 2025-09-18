-- Ensures valid_from is always before valid_to

{{ config(severity = 'error') }}

select 
    id,
    valid_from,
    valid_to
from {{ ref('dim_campaign') }}
where valid_from >= valid_to