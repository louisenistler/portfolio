select distinct
    du.user_ref,
    du.state as state_located,
    case when du.has_protected_account > 0 then 1 else 0 end has_protected_account,
    case when du.has_shared_checking_account > 0 then 1 else 0 end has_shared_checking_account,
    case when du.has_loan_accounts > 0 then 1 else 0 end as has_loan_accounts,
    case when fcd.num_expense_goals > 0 then 1 else 0 end has_expense,
    case when fcd.num_emergency_goals > 0 then 1 else 0 end has_emergency_fund,
    case when fcd.num_standard_goals > 0 then 1 else 0 end has_standard_goal,
    case when du.is_profitable > 0 then 1 else 0 end as is_profitable
from (select distinct user_ref 
      from curated.fact_transaction 
      where transaction_date >= '2020-04-01' and transaction_date <= '2020-06-30' 
      and is_possible_unemployment = true) ft
join curated.dim_user_pii du on ft.user_ref = du.user_ref
join curated.fact_customer_day fcd on du.user_ref = fcd.user_ref
where fcd.date = '2020-07-29'
    and du.first_account_open_date < '2020-06-01'
;