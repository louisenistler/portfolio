with loan_transactions as ( --pulls together all loan transactions
select
    account_ref,
    open_date as transaction_date,
    open_ts as transaction_ts,
    loan_amount as amount
from curated.dim_loan_account
where campaign_name <> 'EMPLOYEE_PILOT'

union

select
    dla.account_ref,
    transaction_date,
    transaction_ts,
    amount*-1 as amount
from curated.dim_loan_account dla
join curated.fact_transaction ft on dla.account_ref = ft.account_ref
where tran_code = 'L001CR' --payment tran_code
    and campaign_name <> 'EMPLOYEE_PILOT'
    ),

running_loan_balance as ( --orders transactions and gets running total ledger balance
select
    account_ref,
    transaction_ts,
    transaction_date,
    amount,
    sum(amount) over (partition by account_ref order by transaction_ts rows unbounded preceding) as ledger_balance,
    rank() over (partition by account_ref order by transaction_ts) as n
from loan_transactions
    ),

loan_balance_ranked as ( --rejoins to grab the first transaction which is the open_ts of the loan origination
select
    r.account_ref,
    open_ts,
    open_ts::date as open_date,
    r.transaction_ts,
    r.transaction_date,
    amount,
    ledger_balance,
    n
from running_loan_balance r
join (select account_ref, transaction_ts as open_ts  --pulls the first transaction
      from running_loan_balance
      where n = 1) lo on r.account_ref = lo.account_ref
    ),

ledger_balance_eod as ( --gets the account balance at the end of the day
  SELECT
    account_ref,
    transaction_date,
    ledger_balance
  FROM (
    SELECT
      account_ref,
      transaction_date,
      ledger_balance,
      ROW_NUMBER() OVER (
        PARTITION BY account_ref, transaction_date
        ORDER BY transaction_ts DESC
      ) AS row_num
    FROM
      loan_balance_ranked l
  ) tt
  WHERE
    tt.row_num = 1 -- last transaction of the day
    ),

daily_table as ( --joins back to fact_account_day
select
    fad.account_ref,
    date,
    ledger_balance
from dim_loan_account dla
join fact_account_day fad on dla.account_ref = fad.account_ref
left join ledger_balance_eod lbe on fad.account_ref = lbe.account_ref and fad.date = lbe.transaction_date
where dla.campaign_name <> 'EMPLOYEE_PILOT'
    ),

fact_loan_day as ( --fills in all the mising balance dates
  SELECT
    dt.account_ref,
    date,
    nvl(
      CASE
        WHEN dt.ledger_balance IS NULL
        -- partition on temp_account_day columns to prevent issues related to NULL values FROM the left join
        THEN LAG(dt.ledger_balance, 1) IGNORE NULLS OVER(PARTITION BY dt.account_ref ORDER BY date)
        ELSE dt.ledger_balance
      END
      , 0  -- default to 0 on first day
    ) AS ledger_balance_eod -- end-of-day ledger balance
  FROM
    daily_table dt
    )

select
    date,
    campaign_name,
    sum(ledger_balance_eod) as loan_balance
from fact_loan_day fld
join dim_loan_account dla on fld.account_ref = dla.account_ref
where {CAMPAIGN_NAME.IN('campaign_name')}
group by 1,2
order by 1,2
;