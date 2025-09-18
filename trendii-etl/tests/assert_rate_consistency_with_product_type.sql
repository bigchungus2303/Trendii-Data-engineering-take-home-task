-- Ensures rate fields match the product_type

{{ config(severity = 'error') }}

select 
    id,
    product_type,
    cpc_rate,
    cpm_rate, 
    cpa_percentage
from {{ ref('dim_campaign') }}
where 
    -- CPC campaigns should have cpc_rate and nulls for others
    (product_type = 'CPC' and (cpc_rate is null or cpm_rate is not null or cpa_percentage is not null))
    or
    -- CPM campaigns should have cpm_rate and nulls for others  
    (product_type = 'CPM' and (cpm_rate is null or cpc_rate is not null or cpa_percentage is not null))
    or
    -- CPA campaigns should have cpa_percentage and nulls for others
    (product_type = 'CPA' and (cpa_percentage is null or cpc_rate is not null or cpm_rate is not null))
