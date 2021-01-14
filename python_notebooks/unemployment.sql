with paychecks_or_ui_deposit as ( --pulls in on paychecks and ui deposits
select distinct
    ft.user_ref,
    date_trunc('month',transaction_date)::date as month,
    case when is_possible_paycheck = true or is_possible_unemployment = true then 1 else 0 end is_paycheck_or_ui_deposit,
    case when is_possible_paycheck = true and is_possible_unemployment = false then 1 else 0 end as is_paycheck_not_ui_deposit,
    case when is_possible_unemployment = true then 1 else 0 end is_ui_deposit
from fact_transaction ft
join dim_user du on ft.user_ref = du.user_ref
where (is_possible_paycheck = true or is_possible_unemployment = true)
    and first_account_open_date < '2020-06-01'
    and transaction_date >= '2018-12-01'
    and transaction_date < '2020-07-01'
    ),

monthly_paychecks_or_ui as (
select
    fcm.user_ref,
    fcm.month_date as month,
    case when sum(is_paycheck_or_ui_deposit) > 0 then 1 else 0 end as is_paycheck_or_ui_deposit,
    case when sum(is_paycheck_not_ui_deposit) > 0 then 1 else 0 end is_paycheck_not_ui_deposit,
    case when sum(is_ui_deposit) > 0 then 1 else 0 end is_ui_deposit
from fact_customer_month fcm
join dim_user du on fcm.user_ref = du.user_ref
left join paychecks_or_ui_deposit p on fcm.user_ref = p.user_ref and fcm.month_date = p.month
where fcm.month_date >= '2018-12-01' and fcm.month_date < '2020-07-01'
    and first_account_open_date < '2020-06-01'
group by 1,2
    ),

previous_month as (
select
    month,
    user_ref,
    is_paycheck_or_ui_deposit,
    lag(is_paycheck_or_ui_deposit) over (partition by user_ref order by month) as previous_paycheck_or_ui_deposit,
    is_paycheck_not_ui_deposit,
    lag(is_paycheck_not_ui_deposit) over (partition by user_ref order by month) as previous_paycheck_no_ui,
    is_ui_deposit,
    lag(is_ui_deposit) over (partition by user_ref order by month) as previous_ui_deposit
from monthly_paychecks_or_ui
    )

select
  month,
  sum(is_paycheck_or_ui_deposit) as num_paycheck_or_ui_this_month,
  count(case when previous_paycheck_or_ui_deposit = 1 and is_paycheck_or_ui_deposit = 0 then user_ref end) as paycheck_or_ui_last_month_no_paycheck_no_ui_this_month,
  sum(is_paycheck_not_ui_deposit) as paycheck_this_month,
  count(case when previous_paycheck_no_ui = 1 and is_paycheck_or_ui_deposit = 0 then user_ref end) as paycheck_last_month_no_paycheck_or_ui_this_month,
  count(case when previous_paycheck_no_ui = 1 and is_paycheck_not_ui_deposit = 0 then user_ref end) as paycheck_last_month_no_paycheck_this_month,
  sum(is_ui_deposit) as num_ui_this_month,
  count(case when previous_ui_deposit = 1 and is_paycheck_or_ui_deposit = 0 then user_ref end) as ui_deposit_last_month_no_paycheck_or_ui_this_month,
  count(case when previous_ui_deposit = 1 and is_ui_deposit = 0 then user_ref end) as ui_deposit_last_month_no_ui_deposit_this_month,
  count(case when previous_paycheck_no_ui = 1 and is_paycheck_not_ui_deposit = 0 and is_ui_deposit = 1 then user_ref end) as paycheck_last_month_no_paycheck_this_month_ui_deposit_this_month
from previous_month
where month < date_trunc('month',current_date)
	and month >= '2019-01-01'
group by 1
order by 1;

