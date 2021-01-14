select
    date_trunc('month',transaction_date)::date as month,
    count(distinct du.user_ref)::int as customers
from curated.dim_user du
join curated.fact_transaction lut on du.user_ref = lut.user_ref
where first_account_open_date < '2020-03-01' --enrolled prior to covid-19
    and has_open_accounts = true
    and month >= '2019-01-01' and month < '2020-07-01'
    and is_possible_unemployment = true
group by 1
order by 1;
