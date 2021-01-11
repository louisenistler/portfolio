with daily_transactions as (
select
    date,
    fad.account_ref,
    fad.type,
    sum(deposit_amount) over (partition by fad.account_ref order by date rows 29 preceding) as deposit_amount_last_30,
    sum(swipe_count) over (partition by fad.account_ref order by date rows 29 preceding) as swipe_count_last_30,
    case when ((
        sum(deposit_amount) over (partition by fad.account_ref order by date rows 29 preceding)) >= 400
        and sum(swipe_count) over (partition by fad.account_ref order by date rows 29 preceding) >= 1 )then 1 else 0 end eligibility
from curated.dim_account da
join curated.fact_account_day fad on da.account_ref = fad.account_ref
where fad.type in ('INDIVIDUAL_CHECKING','SHARED_CHECKING')
    and da.open_date < '2020-06-01' --to not add in new accounts to the mix
    and da.close_date is null --accounts are still open today
    and date >= '2020-01-01' --filter for query performance
    ),

previous_date_eligibility as (
select
    date,
    account_ref,
    type,
    eligibility,
    lag(eligibility) over (partition by account_ref order by date) as previous_day_eligibility
from daily_transactions
    )

select
    date,
    type,
    count(*) as accounts,
    sum(eligibility) as eligible_today,
    sum(previous_day_eligibility) as previous_day_eligibility,
    sum(case when previous_day_eligibility = 1 and eligibility = 0 then 1 else 0 end) as eligible_previous_day_not_eligible_current_day
from previous_date_eligibility
where date >= current_date - 90
group by 1,2
order by 1,2
;