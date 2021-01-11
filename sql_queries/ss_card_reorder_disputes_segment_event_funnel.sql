with dispute_reason as ( --dispute reason for ss-card-reorder, anyone for eligible or ineligible will be here
select distinct
	fce.user_ref,
    min(event_ts) as first_dispute_event_ts
from fact_client_event fce
join analytics.segment_web_app s on fce.event_ref = s.message_id
left join current_carmel_user_feature_flags c on fce.user_ref = c.user_id
	and feature_flag_name = 'self-service-reorder' and mode = 'OFF'
where event_value ilike '%recognize this transaction%' --only dispute transactions where they can reorder a card
	and event_ts >= '2020-06-24 19:20:00.000000' --time we turned on ss-card-reorder
	and fce.event_group = 'DISPUTES'
	and c.user_id is null --removes all customers with the feature flag off
group by 1
),

eligibility as ( --each date, is the person first eligible or ineligible
select
    fce.user_ref,
    fce.event_date,
    eligibility_first_event_ts,
    case when description = 'confirm dispute fraud lock card eligible' then 1
--         when description = 'confirm dispute fraud lock card ineligible' then 0
    end eligibility_flag
from fact_client_event fce
join (select distinct
        user_ref,
        event_date,
        min(event_ts) as eligibility_first_event_ts
      from fact_client_event
      where event_group = 'DISPUTES'
        and description in ('confirm dispute fraud lock card eligible')
							--'confirm dispute fraud lock card ineligible'
	    and event_ts >= '2020-06-24 19:20:00.000000'
      group by 1,2) elig on fce.user_ref = elig.user_ref and fce.event_ts = elig.eligibility_first_event_ts
    ),

card_reorder_dispute_funnel AS (
select
    fce.user_ref,
    fce.event_ts,
    fce.event_date,
--    client,
--    event_group,
    description,
    case
           when fce.description = 'confirm dispute fraud lock card eligible' then 1
--           when description = 'clicked confirm dispute fraud lock card eligible confirm' then 2
           when fce.description = 'dispute create questions' then 3
           when fce.description = 'clicked created dispute confirm' then 5
           when fce.description = 'dispute details' then 6
           when fce.description = 'clicked reorder card' and s.event_value = '{"buttonText":"Order New Card"}' then 7
           when fce.description = 'one time transaction before replacing card notification' then 8
           when fce.description = 'clicked one time transaction before replacing card notification confirm' then 9
           when fce.description = 'card reorder address confirmation' then 10
           when fce.description = 'clicked card reorder address confirmation' and s.event_value = '{"buttonText":"Confirm"}' then 11
--           when description = 'success' and category = 'CARD_REORDER_USER_STATE' then 12
        end as rank
from fact_client_event fce
join dispute_reason dr on fce.user_ref = dr.user_ref and fce.event_ts::date >= dr.first_dispute_event_ts::date
join eligibility e on fce.user_ref = e.user_ref 
  and fce.event_ts >= e.eligibility_first_event_ts
join analytics.segment_web_app s on fce.event_ref = s.message_id
where fce.event_group IN ('DISPUTES', 'ACCOUNT')
    and fce.event_date >= '2020-06-24'
    and eligibility_flag = 1 --only looking at people who are eligible

union

select
	c.user_ref,
    created_ts as event_ts,
	created_date as event_date,
    'new card created in backend' as description,
    100 as rank
from eligibility c
join curated.dim_card dc on c.user_ref = dc.user_ref --and c.event_date = dc.created_date
where is_self_service_card_reorder = true
  and created_ts >= '2020-06-24 19:20:00.000000'
  	)

select rank,
       description,
       count(distinct user_ref) AS customers
from card_reorder_dispute_funnel
where rank is not null
group by 1, 2
order by 1, 2;