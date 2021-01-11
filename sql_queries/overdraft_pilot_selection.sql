create table public.overdraft_pilot_accounts_customers as 
with swipe_last_30 as ( --pull all the accounts with a swipe in the last 30 days
select distinct
    account_ref
from fact_transaction ft
where transaction_date >= current_date - 30
    and transaction_date < current_date
    and is_interchange_swipe = true
    ),

insufficient_funds as ( --pull all the accounts with an insufficient funds decline in the last 30 days
select distinct
    account_ref
from coalfire.fact_iso iso
join curated.dim_account da on iso.account_key = da.account_id
where iso.iso_response_code = 'INSUFFICIENT_FUNDS'
  and iso.iso_created::date >= current_date - 30
  and iso.iso_created::date < current_date
group by 1
    ),

fraudsters_or_frozen_to_remove as ( --need to get a CTE for all the customers and accounts who are NOT eligible for overdraft because they are fraudster or they are frozen
select distinct
    fua3.account_ref,
    du2.user_ref,
    fua3.type
from curated.dim_user du --pulls in all the customers
join curated.fact_user_account fua on du.user_ref = fua.user_ref --who is the user(s) associated with this OG account
join curated.fact_user_account fua2 on fua.account_ref = fua2.account_ref --now find the partner account
join curated.fact_user_account fua3 on fua2.user_ref = fua3.user_ref --pull in the second partner
join curated.dim_user du2 on fua3.user_ref = du2.user_ref
where fua.type in ('INDIVIDUAL_CHECKING','SHARED_CHECKING')
--    and du.user_ref = '47685855-f934-40cf-a4b8-0fb869f4b62e' --amy's individual account. she does not have a shared partner, so should only return 1 row
--    and du.user_ref = '202036ac-a50b-4f70-9714-4e612d276920' --testing on Louise's user_ref - should return 4 customers + accounts (lou checking, lou shared, peter shared, peter checking)
    and fua3.type in ('INDIVIDUAL_CHECKING','SHARED_CHECKING')
    and (du.is_fraudster = true or du.is_frozen = true) --partner A is a fraudster or has a frozen account
    ),

bbva_insiders as ( --need to get a CTE for all the customers and accounts who are NOT eligible for overdraft because they are bbva insiders (list was emailed to louise of who to remove)
select distinct
    dup.first_name,
    dup.last_name,
    fua3.account_ref,
    du2.user_ref,
    fua3.type,
    du2.first_name,
    du2.last_name
from public.overdraft_remove_bbva_insider o --table with the bbva insiders to remove, was uploaded from an excel spreadsheet provided by bbva
join curated.dim_user_pii dup on o.last_name = dup.last_name and o.first_name = dup.first_name --pulls in all the customers
join curated.fact_user_account fua on dup.user_ref = fua.user_ref --who is the user(s) associated with this OG account
join curated.fact_user_account fua2 on fua.account_ref = fua2.account_ref --now find the partner account
join curated.fact_user_account fua3 on fua2.user_ref = fua3.user_ref --pull in the second partner
join curated.dim_user_pii du2 on fua3.user_ref = du2.user_ref
where fua.type in ('INDIVIDUAL_CHECKING','SHARED_CHECKING')
    and fua3.type in ('INDIVIDUAL_CHECKING','SHARED_CHECKING')
    ),

all_eligible_for_swipe_and_nsf as ( --must have swiped in last 30 days AND had an insufficient funds decline in last 30 days to be in pilot
select distinct
    da.account_ref
from dim_account da
join swipe_last_30 s on da.account_ref = s.account_ref
join insufficient_funds if on da.account_ref = if.account_ref
    ),

pilot_selection as ( --pulls 10,000 accounts at random to be in the pilot
select
    a.account_ref
from all_eligible_for_swipe_and_nsf a
left join fraudsters_or_frozen_to_remove f on a.account_ref =  f.account_ref
left join bbva_insiders bi on a.account_ref = bi.account_ref
where f.account_ref is null --no one can be a fraudster
    and bi.account_ref is null --no one can be in bbva insider list
order by random()
limit 10000
    )

--pulls in the other accounts and customers
select distinct --count(distinct fua4.account_ref) as accounts, count(distinct dup.user_ref) as customers
    fua4.account_ref,
    dup.user_ref
from pilot_selection p
join curated.fact_user_account fua on p.account_ref = fua.account_ref --who is the user(s) associated with this OG account
join curated.fact_user_account fua2 on fua.user_ref = fua2.user_ref --join to pull in other checking accounts from that first user
join curated.fact_user_account fua3 on fua2.account_ref = fua3.account_ref --now find the partner account
join curated.fact_user_account fua4 on fua3.user_ref = fua4.user_ref --pull in the second partner
join curated.dim_account da on fua4.account_ref = da.account_ref
join curated.dim_user_pii dup on fua4.user_ref = dup.user_ref
where fua.type in ('INDIVIDUAL_CHECKING','SHARED_CHECKING')
--    and fua.account_ref = '5d17c3bf-b654-438d-8aef-afb6ceaf7be7' --louise's shared account ref
--    and fua.account_ref = 'cb08ead3-4632-392e-a95f-a1118bdc279d' --louise indie account_ref
--     and fua.account_ref = 'fe40985b-c88e-46d0-ba53-ba90f9bf8ef8' -- peter's indie checking
    and fua4.type in ('INDIVIDUAL_CHECKING','SHARED_CHECKING')
    and da.close_date is null --only look for accounts that are still open today
;