{{ config(materialized='materialized_view') }}

-- Map impressions to campaigns via brand_id and event date falling in [valid_from, valid_to]
with joined as (
  select
    dc.id   as campaign_id,
    dc.name as campaign_name,
	dc.valid_from,
    dc.brand_id,
    i.product_id,
    count(distinct i.eid) as impressions          
  from {{ ref('f_impressions') }} i 
  join {{ ref('dim_campaigns') }}  dc     
    on dc.brand_id = i.brand_id
   and i.event_created_at::date between dc.valid_from and dc.valid_to
  group by 1,2,3,4,5
),
ranked as (
  select
    campaign_id, campaign_name, valid_from, brand_id, product_id, impressions,
    row_number() over (partition by campaign_id, valid_from order by impressions desc, product_id) as rn
  from joined
)
select campaign_id, campaign_name, valid_from, brand_id, product_id, impressions
from ranked
where rn = 1
order by campaign_id;
	

