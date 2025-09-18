-- Ensures current records have end date of 9999-12-31

{{ config(severity = 'error') }}

select 
    id,
    current_record,
    valid_to
from {{ ref('dim_campaign') }}
where current_record = true 
  and valid_to != '9999-12-31'