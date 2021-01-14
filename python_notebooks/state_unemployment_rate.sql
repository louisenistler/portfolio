select distinct
    du.state,
    count(distinct du.user_ref) as customers
from fact_transaction ft
join dim_user du on ft.user_ref = du.user_ref
where is_possible_paycheck = true or is_possible_unemployment = true
    and transaction_date  >= '2020-04-01' and transaction_date <= '2020-06-30'
    and first_account_open_date < '2020-06-01'
group by 1
order by 1;


