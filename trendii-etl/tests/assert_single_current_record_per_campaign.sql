-- Ensures only one current record exists per campaign ID

{{ config(severity = 'error') }}

select 
    id,
    count(*) as current_record_count
from {{ ref('dim_campaign') }}
where current_record = true
group by id
having count(*) > 1