with badges_test as ( --pulls the experiment group the badges customers are in
select distinct
    du.user_ref,
	case
      when first_marketing_type = 'PAID' then 'PAID'
      else 'NON_PAID'
    end as paid_non_paid,
	case when bs.status = 'added-to-control' then 'control'
		 when bs.status = 'added-to-experiment' then 'test' end as experiment_group,
    bs.created_at::date as added_to_experiment_date,
    (added_to_experiment_date + 30) as badge_deadline
from analytics.current_mux_badge_status bs
join analytics.current_mux_badge b on bs.badge_id = b.badge_id
join curated.dim_user du on b.customer_reference = du.user_ref
where bs.status in ('added-to-control','added-to-experiment')
  	and added_to_experiment_date >= '2020-11-17'
    and added_to_experiment_date < current_date - {DAY_X_CONVERSION}::int --must be open for X number of days to appear
    ),

remove_false_positives as ( --people who funded prior to entering experiment or people who closed prior to experiment
select
  bt.user_ref,
  first_account_open_date,
  first_deposit_transaction_ts,
  added_to_experiment_date,
  close_date
from badges_test bt
join curated.dim_user du on bt.user_ref = du.user_ref
join curated.fact_user_account fua on du.user_ref = fua.user_ref
join curated.dim_account da on fua.account_ref = da.account_ref
where da.type = 'INDIVIDUAL_CHECKING'
  and (du.first_deposit_transaction_ts::date < added_to_experiment_date
	 or da.close_date < added_to_experiment_date)
  ),

first_badge as (
select
	du.user_ref,
    bt.experiment_group,
    bt.added_to_experiment_date,
  	min(case when status = 'account-funded' then bs.created_at end)::date as first_fund_date,
  	min(case when status = 'card-swipe' then bs.created_at end)::date as first_swipe_date,
  	min(case when status = 'goal-created' then bs.created_at end)::date as first_goal_date,
  	min(case when status = 'expense-created' then bs.created_at end)::date as first_expense_date,
  	min(case when status = 'eligible-for-fulfillment' then bs.created_at end)::date as first_eligible_date,
   	min(case when status = 'fulfillment-success' then bs.created_at end)::date as payout_date,
    case when first_swipe_date < bt.added_to_experiment_date then 'HAD_BADGE'
  		 when first_goal_date < bt.added_to_experiment_date then 'HAD_BADGE'
  		 when first_expense_date < bt.added_to_experiment_date then 'HAD_BADGE' 
  		 else 'HAD_NO_BADGE' end as previous_badge
from analytics.current_mux_badge_status bs
join analytics.current_mux_badge b on bs.badge_id = b.badge_id
join curated.dim_user du on b.customer_reference = du.user_ref
join badges_test bt on du.user_ref = bt.user_ref
left join remove_false_positives r on bt.user_ref = r.user_ref
where r.user_ref is null
  and (bs.created_at::date <= badge_deadline or status = 'fulfillment-success')
group by 1,2,3
	),

daily_badges_status as (
select
	fb.user_ref,
    fb.experiment_group,
  	fb.added_to_experiment_date,
    num_open_account_days,
    previous_badge,
    fcd.date,
    datediff('day', added_to_experiment_date, date) as num_days_since_badge_experiment_start,
    case when fcd.date >= first_fund_date then 1 else 0 end as _has_funded,
    case when fcd.date >= first_expense_date then 1 else 0 end as _has_expense,
    case when fcd.date >= first_goal_date then 1 else 0 end as _has_goal,
    case when fcd.date >= first_swipe_date then 1 else 0 end as _has_swiped,
	case when fcd.date >= first_eligible_date then 1 else 0 end as _is_eligible,
	case when fcd.date >= payout_date then 1 else 0 end as _is_payout,
    case when _has_funded = 1 and _has_expense = 1 and _has_goal = 1 and _has_swiped = 1 then 1 else 0 end as _is_completed_badges
from first_badge fb
join curated.fact_customer_day fcd on fb.user_ref = fcd.user_ref
where num_days_since_badge_experiment_start >= 0 and num_days_since_badge_experiment_start <= 30
	and fcd.date < current_date
	)

/*
days_to_badge as (
select
	eg.user_ref,
	eg.experiment_group,
	eg.added_to_experiment_date,
    eg.paid_non_paid,
    isnull(fb.previous_badge,'HAD_NO_BADGE') as previous_badge,
	date_diff('day', eg.added_to_experiment_date, first_fund_date) as num_days_to_fund,
	date_diff('day', eg.added_to_experiment_date, first_swipe_date) as num_days_to_swipe,
	date_diff('day', eg.added_to_experiment_date, first_goal_date) as num_days_to_goal,
	date_diff('day', eg.added_to_experiment_date, first_expense_date) as num_days_to_expense,
	date_diff('day', eg.added_to_experiment_date, first_eligible_date) as num_days_to_eligible,
	date_diff('day', eg.added_to_experiment_date, payout_date) as num_days_to_payout
from experiment_groups eg
join first_badge fb on eg.user_ref = fb.user_ref
	)
*/

select 
  date_trunc({DATE_BUCKET}, added_to_experiment_date)::date as enrollment_date,
  experiment_group,
  count(distinct user_ref) as num_customers,
  count(distinct case when _has_funded = 1 then user_ref end) as num_has_funded,
  count(distinct case when _has_expense = 1 then user_ref end) as num_has_expense,
  count(distinct case when _has_goal = 1 then user_ref end) as num_has_goal,
  count(distinct case when _has_swiped = 1 then user_ref end) as num_has_swiped,
  count(distinct case when _is_completed_badges = 1 then user_ref end) as num_is_completed_badges,
  count(distinct case when _is_payout = 1 then user_ref end) as num_is_payout
from daily_badges_status
where num_days_since_badge_experiment_start = {DAY_X_CONVERSION}::int
	and {EXPERIMENT_GROUP.IN('experiment_group')}
	and {PAID_NON_PAID.IN('paid_non_paid')}
	and {PREVIOUS_BADGE.IN('previous_badge')}
	and added_to_experiment_date >= {CALENDAR.START} and added_to_experiment_date < {CALENDAR.END}
group by 1, 2
order by 1, 2
;