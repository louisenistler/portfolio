with merchant_cvv_declines as (
select distinct merchant from
  (select card_acceptor_address as merchant,
          count(case when isos.response_code = 'CVV_VERIFICATION_FAILED' then 'CVV_VERIFICATION_FAILED' end) as cvv_fail,
          count(case when isos.response_code = 'APPROVED' then 'APPROVED' end) as appr,
          count(case when isos.response_code = 'APPROVED' and isos.message_function = 'ADVICE' then 'STIP_APPROVED' end) as stip,
          count(case when cards.card_id is not null and isos.response_code = 'APPROVED' and cards.card_status <> 'ACTIVE' then 'not_active' end) as appr_card_inactive,
          count(case when cards.card_id is not null and isos.response_code = 'APPROVED' and cards.card_status = 'ACTIVE' then 'active' end) as appr_card_active,
          count(case when fua.customer_account_type is not null and fua.customer_account_type = 'SHARED' then 'shared' end) as shared,
          sum(case when isos.response_code = 'APPROVED' then isos.transaction_amount else 0 end) as appr_amount
   from   current_cartesian_iso_8583_messages_safe isos
   left join current_cartesian_cards_safe cards using (card_id)
   left join dim_card on (dim_card.card_id = cards.card_id::text)
   left join fact_user_account fua using (user_ref, account_ref)
   where  isos.created >= '2020-08-26'
   and isos.card_acceptor_address not ilike '%google%'
   and isos.card_acceptor_address not ilike '%*square%'
   group by 1
   having cvv_fail > 1000
   order by 2 desc) as res

  ),

approvals as (
select
  type,
  dc.card_ref,
  min(isos.created) as first_approval_ts
from current_cartesian_iso_8583_messages_safe isos
join merchant_cvv_declines m on m.merchant = isos.card_acceptor_address
join current_cartesian_cards_safe cards on isos.card_id = cards.card_id
join dim_card dc on dc.card_id = cards.card_id::text
join fact_user_account fua using (user_ref, account_ref)
-- left join (select distinct correlation, amount
--               from current_cartesian_transactions cct
--               join current_cartesian_tran_codes cctc on cct.tran_code_id = cctc.tran_code_id
--               where cct.void_of_id is null
--                 and cct.voided_by_id is null
--                 and cctc.tran_code = '0027DR'
--                 and cct.created >= '2020-08-25') cct on isos.correlation = cct.correlation
where isos.response_code in ('APPROVED') --only looking to alert approvals
    and isos.created::date >= '2020-08-26'
	and isos.card_acceptor_address not ilike '%google%'
	and isos.card_acceptor_address not ilike '%*square%'
    and isos.card_acceptor_address not ilike '%DC%GOV%PAYMENT%'
    and isos.card_acceptor_address not ilike '%onlyfans%'
group by 1,2
    ),

approvals_by_hour as (
select
    dh.hour,
    isnull(count(distinct card_ref),0) as cards
from dim_hour dh
left join approvals a on dh.hour = date_trunc('hour',a.first_approval_ts)
where dh.date >= '2020-08-26' and dh.hour <= date_trunc('hour',current_timestamp)
group by 1
  ),

approvals_total_rolling_12 as ( --over the last 12 hours, what are the total number of distinct cards approved
select
	hour,
	cards,
	sum(cards) over (order by hour rows 11 preceding) as cards_rolling_12,
	rank() over (order by hour desc) as rank
from approvals_by_hour
where hour::date >= current_date - 2
    ),

previous_row as ( --need to say over the last 12 hours, have we already seen 50 before so we don't send alert. we cannot include the current row, so need to start with lag
select
    hour,
    cards_rolling_12,
    rank,
    lag(cards_rolling_12) over (order by hour) as previous_cards_rolling_12
from approvals_total_rolling_12
    ),

alert_data as ( --find the max rank for the previous 12 rows - if that is >= 50, then we don't want to send another alert
select
    rank,
    hour,
    cards_rolling_12,
    previous_cards_rolling_12,
    max(previous_cards_rolling_12) over (order by hour rows 10 preceding) as max_over_last_12_hours
from previous_row
    )

-- use the last column (cards_alert) to determine if we send the alert or not. if we have already sent it for having >= 50, then ignore this alert
select
    rank,
    hour,
    cards_rolling_12,
    previous_cards_rolling_12,
    max_over_last_12_hours,
    case when cards_rolling_12 >= 50 and max_over_last_12_hours >= 50 then 0 else cards_rolling_12 end as cards_alert_rolling_12
from alert_data
order by 1;