-- Ensures no gaps in SCD Type 2 history for campaigns

{{ config(severity = 'warn') }}

with campaign_periods as (
    select 
        id,
        valid_from,
        valid_to,
        lag(valid_to) over (partition by id order by valid_from) as prev_valid_to
    from {{ ref('dim_campaign') }}
),
gaps as (
    select 
        id,
        valid_from,
        prev_valid_to
    from campaign_periods
    where prev_valid_to is not null 
      and valid_from != prev_valid_to + interval '1 day'
)
select * from gaps