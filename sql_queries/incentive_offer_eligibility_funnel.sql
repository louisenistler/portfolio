------- INCENTIVE OFFER ELIGIBILITY FUNNEL -------
----- Incentive requirements
---- Fine print: https://banksimple.atlassian.net/wiki/spaces/MAR/pages/862453780/Incentive+Offer
--- July ($250 per account): Open PGA in July, fund to $10K by E8/15 EOD, maintain (at EOD) $10K until 10/31 EOD
--- August ($250 per account): Open PGA in August, fund to $10K by 9/16 EOD, maintain (at EOD) $10K until 12/31 EOD
--- September ($150 per account): Open PGA in September, fund to $10K by 10/15 EOD, maintain (at EOD) $10K until 12/31 EOD

with offer_reqs as ( --defining reqirements for each month of the offer
  select
    month_date as offer_month,
    '%_CHECKING'::text as offer_account_type, --Louise update here to see the number of checking accounts --threw an error if not defined as text
    10000 as min_balance,
    case
      when offer_month = '2019-07-01' then 250
      when offer_month = '2019-08-01' then 250
      when offer_month = '2019-09-01' then 150
    end as min_balance_payout,
    case
      when offer_month = '2019-07-01' then '2019-08-15'
      when offer_month = '2019-08-01' then '2019-09-16'
      when offer_month = '2019-09-01' then '2019-10-15'
    end as funding_deadline,
    case
      when offer_month = '2019-07-01' then '2019-10-31'
      when offer_month = '2019-08-01' then '2019-12-31'
      when offer_month = '2019-09-01' then '2019-12-31'
    end as hold_deadline,
    case
      when offer_month = '2019-07-01' then '2019-07-29'
      when offer_month = '2019-08-01' then '2019-08-29'
      when offer_month = '2019-09-01' then '2019-09-25'
    end as email_to_open_pga_send_date --Louise add: determine if customers had unsubscribed prior to receiving email from Simple to move funds to PGA
  from curated.dim_date dd
  where month_date between '2019-07-01' and '2019-09-01'
  group by 1
  ),

cohort_accounts as ( --all accounts potentially eligible for the offer
  select
    da.account_ref,
    da.type,
    open_date,
    close_date,
    offer_month,
    funding_deadline,
    hold_deadline,
    min_balance,
    min_balance_payout,
		da.parent_account_ref,
		email_to_open_pga_send_date
  from curated.dim_account da
  join offer_reqs req 
    on date_trunc('month', da.open_ts)::date = req.offer_month --account opened in offer month
      and da.type ilike req.offer_account_type --account is correct type
  ),

predeadline_balance as ( --account's most recent balance before the deadline, regardless of bonus qualification 
  select distinct
    ca.account_ref,
    nvl(last_value(fad.ledger_balance_eod)
      over(partition by fad.account_ref
      order by fad.date asc
      rows between unbounded preceding and unbounded following
      ), 0) as predeadline_balance --check most recent balance before funding deadline
  from cohort_accounts ca
  join curated.fact_account_day fad using(account_ref)
  where fad.date <= ca.funding_deadline --last/most recent balance before funding deadline
    and fad.date <= current_date-1
  ),

funded_predeadline as ( --accounts where most recent pre-deadline balance qualifies for a bonus (will be the same as funded on deadline once deadline has passed)
  select distinct
    pb.account_ref,
    predeadline_balance,
    current_date-1 >= ca.funding_deadline as past_deadline --check if funding deadline has passed
  from cohort_accounts ca
  join predeadline_balance pb using(account_ref)
  where predeadline_balance >= min_balance --funded to offer min
  ),

dropped_balance as ( --accounts funded by the deadline that dropped below the min balance before the hold deadline
  select distinct
    fp.account_ref
  from funded_predeadline fp
  join cohort_accounts ca using(account_ref)
  join curated.fact_account_day fad using(account_ref)
  where past_deadline is true --only measure after funding deadline has passed
    and fad.date between ca.funding_deadline and ca.hold_deadline
    and fad.date <= current_date-1
    and fad.ledger_balance_eod < min_balance --balance dropped below min balance
  ),

maintained_balance as ( --accounts funded by the deadline that maintained balance through hold deadline
  select
    fp.account_ref  
  from funded_predeadline fp
  join cohort_accounts ca using(account_ref)
  left join dropped_balance db using(account_ref)
  where past_deadline is true --only measure after funding deadline has passed
    and db.account_ref is null --account not in dropped balance list
    and (ca.close_date > ca.hold_deadline or ca.close_date is null) --account remained open through hold deadline
  ),

paid_out as ( --accounts receiving incentive payout
  select distinct
    ft.account_ref,
    ft.amount as payout_amount
  from cohort_accounts ca
  join curated.fact_transaction ft using(account_ref)
  where ft.tran_code = 'I001CR' --tran_code for customer incentive credits
    and ft.amount = min_balance_payout
    and ft.transaction_ts between hold_deadline and date_add('month', 1, hold_deadline::date) --looking for transactions 1 month after hold deadline
  ),

opened_pga as ( --CTE added topull in the list of all **protected** accounts that will be paid out so as to account for customers who had funds of $10K in both checking and protected
  select distinct
    account_ref,
		parent_account_ref,
    min_balance_payout as pg_payout_amount
  from public.eligible_accounts_for_payout_louise
  ),

maintained_balance_for_pga_account as ( --CTE added topull in the list of all **protected** accounts that will be paid out so as to account for customers who had funds of $10K in both checking and protected
  select distinct
    account_ref,
		parent_account_ref,
    min_balance_payout as pg_payout_amount
  from public.eligible_accounts_for_payout_louise
  where maintained_balance is not null
  ),

email_unsubscribes as ( --pulling this at the account level. for shared, if one partner unsubscribed from the email then the account is considered an unsubscribe
	select distinct
		ca.account_ref,
		email_to_open_pga_send_date
	from cohort_accounts ca
	join fact_user_account fua on ca.account_ref = fua.account_ref
	join dim_user du on fua.user_ref = du.user_ref
	join analytics.pgkafka_carmel_user_email_preferences	e	on du.user_ref = e.user_id
	where (send_marketing_emails = false OR send_announcement_emails = false) --anyone who has unsubscirbed
		and horizon_timestamp::date < email_to_open_pga_send_date --anyone who unsubscribed prior to the date the email was sent
	),

received_first_email as (
	select distinct
		ca.account_ref,
		email_campaign_ref,
		first_delivered_date,
		first_opened_date,
		first_clicked_date
	from cohort_accounts ca
	join fact_user_account fua on ca.account_ref = fua.account_ref
	join dim_email de on fua.user_ref = de.user_ref
	where email_campaign_ref in (719833,763274,804171) --email campaign refs of the original email that was sent out to customers telling them to open a pga
	)

--output REMOVES any parent accounts whose child account is eligible to recieve payout
--in other words: if I had $10K in indie checking, but ALSO had $10K in protected and am currently eligible to receive the bonus, I will NOT be in this list
select
  ca.offer_month,
  'ALL' as account_type,
  count(ca.account_ref) as eligible_accounts,
  count(fp1.account_ref) as funded_predeadline,
  nullif(count(fp2.account_ref), 0) as funded_on_deadline,
	nullif(count(e.account_ref), 0) as unsubscribed_pre_first_email,
--	nullif(count(rfe.account_ref), 0) as received_first_email,
--	nullif(count(rfe.first_opened_date), 0) as opened_first_email,	
  nullif(count(mb.account_ref), 0) as maintained_balance,
  nullif(count(po.account_ref),0) as paid_out,
	nullif(count(op.parent_account_ref),0) as opened_pga,
  case
    when maintained_balance is null then funded_predeadline*min_balance_payout
    when paid_out is null then maintained_balance*min_balance_payout
    else sum(payout_amount)
  end as payout_liability
from cohort_accounts ca
left join funded_predeadline fp1 on ca.account_ref = fp1.account_ref
left join funded_predeadline fp2 on ca.account_ref = fp2.account_ref and fp2.past_deadline is true --only measure after deadline has passed
left join maintained_balance mb on ca.account_ref = mb.account_ref
left join paid_out po on ca.account_ref = po.account_ref
left join maintained_balance_for_pga_account mb_pga on ca.account_ref = mb_pga.parent_account_ref --remove accounts if parent account to a protected account that was paid out. don't want to pay these accounts.
left join opened_pga op on fp2.account_ref = op.parent_account_ref --determine of the people who funded on the deadline who opened a pga (doesn't matter when it was open)
left join email_unsubscribes e on fp2.account_ref = e.account_ref
left join received_first_email rfe on fp2.account_ref = rfe.account_ref
where mb_pga.parent_account_ref is null
group by 1, 2, min_balance_payout

union

select
  ca.offer_month,
  ca.type as account_type,
  count(ca.account_ref) as eligible_accounts,
  count(fp1.account_ref) as funded_predeadline,
  nullif(count(fp2.account_ref), 0) as funded_on_deadline,
	nullif(count(e.account_ref), 0) as unsubscribed_pre_first_email,
--	nullif(count(rfe.account_ref), 0) as received_first_email,
--	nullif(count(rfe.first_opened_date), 0) as opened_first_email,	
  nullif(count(mb.account_ref), 0) as maintained_balance,
  nullif(count(po.account_ref),0) as paid_out,
	nullif(count(op.parent_account_ref),0) as opened_pga,
  case
    when maintained_balance is null then funded_predeadline*min_balance_payout
    when paid_out is null then maintained_balance*min_balance_payout
    else sum(payout_amount)
  end as payout_liability
from cohort_accounts ca
left join funded_predeadline fp1 on ca.account_ref = fp1.account_ref
left join funded_predeadline fp2 on ca.account_ref = fp2.account_ref and fp2.past_deadline is true --only measure after deadline has passed
left join maintained_balance mb on ca.account_ref = mb.account_ref
left join paid_out po on ca.account_ref = po.account_ref
left join maintained_balance_for_pga_account mb_pga on ca.account_ref = mb_pga.parent_account_ref --remove accounts if parent account to a protected account that was paid out. don't want to pay these accounts.
left join opened_pga op on fp2.account_ref = op.parent_account_ref --determine of the people who funded on the deadline who opened a pga (doesn't matter when it was open)
left join email_unsubscribes e on fp2.account_ref = e.account_ref
left join received_first_email rfe on fp2.account_ref = rfe.account_ref
where mb_pga.parent_account_ref is null
group by 1, 2, min_balance_payout
order by 1, 2
;