--table to pull in 2020 marketing data
WITH traffic AS (
  SELECT
    anonymous_ref,
    visitor_ref,
    first_event_ref AS event_ref,
    user_ref,
    first_event_date AS event_date,
    first_source AS source,
    first_source_campaign_id AS source_campaign_id,
    first_search_type AS search_type,
    first_campaign_type AS campaign_type,
    first_marketing_product AS marketing_product,
    first_channel AS channel,
    first_medium AS medium,
    first_campaign_name AS campaign_name,
    first_marketing_type AS marketing_type,
    first_ad_group_ref AS ad_group_ref,
    first_ad_group_name AS ad_group_name
  FROM curated.dim_visitor dv
--  WHERE first_event_date between '2019-01-01' and '2019-12-31' --2019 version
  WHERE first_event_date >= '2020-01-01' --2020 version
--  WHERE first_event_date >= '2020-09-01' --testing version
),

app_start AS (
  SELECT
    t.visitor_ref,
    MIN(fce.event_date) AS app_start_date
  FROM traffic t
  JOIN curated.fact_client_event fce ON t.visitor_ref = nvl(fce.user_ref, fce.device_id, fce.anonymous_ref) --this is how visitor ref is defined
    AND (category = 'USERNAME_AND_PASSWORD_CREATION' OR description = 'screen account info' OR description = 'account info')
  LEFT JOIN curated.dim_user du ON fce.user_ref = du.user_ref
    AND fce.event_date > du.created_date --user landed on page after creating creds
  WHERE du.user_ref IS NULL --make sure event happened before creds (if applicable)
--    AND fce.event_ts >= '2019-01-01' --filter for query performance --2019 version
    AND fce.event_ts >= '2020-01-01' --filter for query performance --2020 version
--    AND fce.event_ts >= '2020-09-01' --testing version version
  GROUP BY 1
),

creds_to_apps AS (
  SELECT DISTINCT
    du.user_ref,
    event_date,
    created_date,
    applied_date
  FROM curated.dim_user du
  JOIN traffic t ON du.first_event_ref = t.event_ref
),

enrollments AS (
  SELECT DISTINCT
    du.user_ref,
    event_date,
    first_account_open_date,
    cohort_date
  FROM curated.dim_user du
  JOIN traffic t ON du.first_event_ref = t.event_ref
  WHERE du.first_account_open_date IS NOT NULL
),

accounts AS (
  SELECT
    user_ref,
    COUNT(*) AS accounts_m0
  FROM (
    SELECT
      e.user_ref,
      e.cohort_date,
      da.account_ref,
      da.open_date
    FROM enrollments e
    JOIN curated.fact_user_account fua ON e.user_ref = fua.user_ref
    JOIN curated.dim_account da ON fua.account_ref = da.account_ref
    WHERE da.account_type IN ('CHECKING', 'PROTECTED', 'CERTIFICATE_OF_DEPOSIT', 'EXPRESS_PERSONAL_LOAN')
    ) accounts
  WHERE cohort_date = DATE_TRUNC('month', open_date)::date
  GROUP BY 1
),

transactions_m0 as (
  SELECT
    e.user_ref,
    SUM(bbva_active_transaction_count) AS transactions_m0
  FROM enrollments e
  JOIN curated.fact_customer_day fcd ON e.user_ref = fcd.user_ref
  WHERE fcd.date BETWEEN cohort_date AND last_day(cohort_date) --between 1st and last day of M0
    AND fcd.date <= current_date-1
  GROUP BY 1
),

--create a change log of fraudster group status (start and end times of being included/excluded from group)
fraudster_change_log AS (
  SELECT
    a.user_id AS user_ref,
    g.group_name,
    a.pg_operation,
    a.pg_operation = 'insert' AS is_in_group, --insert means they're added to group (other operation is delete, which is being removed from group)
    a.pg_sort_key,
    CASE
      WHEN a.pg_txn_timestamp = '2000-01-01 00:00:00.000000' THEN a.horizon_timestamp --handle weird pg_txn_timestamp
      ELSE a.pg_txn_timestamp --timestamp that this transaction (group addition or removal) was committed to the backend postgres database
    END AS start_ts,
    start_ts::date AS start_date,
    LEAD(start_ts,1)
      OVER(
        PARTITION BY a.user_id, a.group_id
        ORDER BY a.pg_sort_key, start_ts
        ) AS end_ts,
    end_ts::date AS end_date
  FROM analytics.pgkafka_carmel_user_group a
  JOIN analytics.current_carmel_banksimple_group g ON a.group_id = g.group_id
  WHERE group_name IN ('ID THEFT', 'Fraudsters!! :(', 'BlackHat Group', 'UI Scam') --fraudster groups as defined in dim_user
    AND a.pg_operation != 'bkfill' --don't include backfill records
),

--one record per customer per fraudster group status per day.  if a customer was added/removed during the day, all changes/records will appear in this table. (that gets cleaned up in the next cte)
-- only includes groups customers were added to and only dates on or after the first date they were added to the group
fact_customer_fraudster_group_day AS (
  SELECT DISTINCT
    fcd.date,
    e.user_ref,
    fcl.group_name,
    CASE
      WHEN is_in_group is true THEN 1
      ELSE 0
    END AS is_in_group,
    ROW_NUMBER()
      OVER(
        PARTITION BY e.user_ref, fcd.date, fcl.group_name
        ORDER BY fcl.end_ts DESC
        ) AS group_n --desc order of groups end_ts for that day
  FROM enrollments e
  JOIN curated.fact_customer_day fcd ON e.user_ref = fcd.user_ref
  JOIN fraudster_change_log fcl ON e.user_ref = fcl.user_ref
    AND fcd.date BETWEEN fcl.start_date AND nvl(fcl.end_date, current_date) --date between start and end or today if end_date is null
),

--one record per customer per fraudster status per day.  reflects status at the end of the day (so, if a customer was removed from the groups mid-day, they are NOT flagged as a fraudster)
-- only includes dates on or after the first date they were flagged as a fraudster
fact_customer_fraudster_day AS (
  SELECT
    date,
    user_ref,
    sum(is_in_group) > 0 AS _flagged_as_fraudster
  FROM fact_customer_fraudster_group_day
  WHERE group_n = 1 --grab the end state for any given day (in case customer was added or removed mid-day, grab the "final" state)
  GROUP BY 1, 2
),

user_level_m0 AS (
  SELECT
    e.user_ref,
    fcd.date,
    nvl(f._flagged_as_fraudster, false) AS fraudster_flag_m0,
    fcd.balance_eod AS balance_m0,
    fcd.running_total_swipe_count AS swipes_m0,
    fcd.num_loan_accounts AS loan_accounts_m0,
    CASE WHEN (balance_m0 >= 100 OR transactions_m0 > 0 OR loan_accounts_m0 > 0) AND fraudster_flag_m0 IS FALSE THEN e.user_ref END AS bbva_active_m0
  FROM enrollments e
  JOIN curated.fact_customer_day fcd ON e.user_ref = fcd.user_ref
    AND last_day(e.cohort_date) = fcd.date --last day of enrollment month
  LEFT JOIN transactions_m0 tm ON e.user_ref = tm.user_ref
  LEFT JOIN fact_customer_fraudster_day f ON e.user_ref = f.user_ref
    AND fcd.date = f.date
  WHERE e.cohort_date < DATE_TRUNC('month', current_date-1) --customers enrolled before this month

  UNION

  SELECT
    e.user_ref,
    fcd.date,
    nvl(f._flagged_as_fraudster, false) AS fraudster_flag_m0,
    fcd.balance_eod AS balance_m0,
    fcd.running_total_swipe_count as swipes_m0,
    fcd.num_loan_accounts AS loan_accounts_m0,
    CASE WHEN (balance_m0 >= 100 OR transactions_m0 > 0 OR loan_accounts_m0 > 0) AND fraudster_flag_m0 IS FALSE THEN e.user_ref END AS bbva_active_m0
  FROM enrollments e
  JOIN curated.fact_customer_day fcd ON e.user_ref = fcd.user_ref
    AND fcd.date = current_date-1 --most recent day
  JOIN curated.dim_user du ON e.user_ref = du.user_ref
  LEFT JOIN transactions_m0 tm ON e.user_ref = tm.user_ref
  LEFT JOIN fact_customer_fraudster_day f ON e.user_ref = f.user_ref
    AND fcd.date = f.date
  WHERE e.cohort_date = DATE_TRUNC('month', current_date-1) --customers enrolled this month
),

user_level_m0_non_seasoning AS ( --new logic to make the M0 metric not season
  SELECT
    e.user_ref,
    fcd.date,
    nvl(f._flagged_as_fraudster, false) AS fraudster_flag_m0_no_season,
    fcd.balance_eod AS balance_m0_no_season,
    fcd.running_total_swipe_count AS swipes_m0_no_season,
    fcd.num_loan_accounts AS loan_accounts_m0_no_season,
    CASE WHEN (balance_m0_no_season >= 100 OR transactions_m0 > 0 OR loan_accounts_m0_no_season > 0) AND fraudster_flag_m0_no_season IS FALSE THEN e.user_ref END AS bbva_active_m0_no_season
  FROM enrollments e
  JOIN curated.fact_customer_day fcd ON e.user_ref = fcd.user_ref
    AND last_day(e.cohort_date) = fcd.date --last day of enrollment month
  JOIN curated.dim_user du ON e.user_ref = du.user_ref
  LEFT JOIN transactions_m0 tm ON e.user_ref = tm.user_ref
  LEFT JOIN fact_customer_fraudster_day f ON e.user_ref = f.user_ref
    AND fcd.date = f.date
  WHERE e.cohort_date < DATE_TRUNC('month', current_date-1) --customers enrolled before this month
    AND DATE_TRUNC('month', e.event_date)::date = e.cohort_date --add in logic saying that they had the enroll in the month they first came to simple

  UNION

  SELECT
    e.user_ref,
    fcd.date,
    nvl(f._flagged_as_fraudster, false) AS fraudster_flag_m0_no_season,
    fcd.balance_eod AS balance_m0_no_season,
    fcd.running_total_swipe_count AS swipes_m0_no_season,
    fcd.num_loan_accounts AS loan_accounts_m0_no_season,
    CASE WHEN (balance_m0_no_season >= 100 OR transactions_m0 > 0 OR loan_accounts_m0_no_season > 0) AND fraudster_flag_m0_no_season IS FALSE THEN e.user_ref END AS bbva_active_m0_no_season
  FROM enrollments e
  JOIN curated.fact_customer_day fcd ON e.user_ref = fcd.user_ref
    AND fcd.date = current_date-1 --most recent day
  JOIN curated.dim_user du ON e.user_ref = du.user_ref
  LEFT JOIN transactions_m0 tm ON e.user_ref = tm.user_ref
  LEFT JOIN fact_customer_fraudster_day f ON e.user_ref = f.user_ref
    AND fcd.date = f.date
  WHERE e.cohort_date = DATE_TRUNC('month', current_date-1) --customers enrolled this month
    AND DATE_TRUNC('month', e.event_date)::date = e.cohort_date --add in logic saying that they had the enroll in the month they first came to simple
),

balance_day_x AS (--normal metrics anchored to enrollment date; no season metrics anchored to traffic date
  SELECT
    user_ref,
    SUM(day_7_balance) AS day_7_balance,
    SUM(day_14_balance) AS day_14_balance,
    SUM(day_30_balance) AS day_30_balance,
    SUM(day_45_balance) AS day_45_balance,
    SUM(day_60_balance) AS day_60_balance,
    SUM(day_90_balance) AS day_90_balance,
    SUM(day_7_balance_no_season) AS day_7_balance_no_season,
    SUM(day_14_balance_no_season) AS day_14_balance_no_season,
    SUM(day_30_balance_no_season) AS day_30_balance_no_season,
    SUM(day_45_balance_no_season) AS day_45_balance_no_season,
    SUM(day_60_balance_no_season) AS day_60_balance_no_season,
    SUM(day_90_balance_no_season) AS day_90_balance_no_season
  FROM (
    SELECT
      e.user_ref,
      fcd.date,
      CASE WHEN num_open_account_days = 0 THEN balance_eod END AS day_0_balance,
      CASE WHEN num_open_account_days = 7 THEN balance_eod END AS day_7_balance,
      CASE WHEN num_open_account_days = 14 THEN balance_eod END AS day_14_balance,
      CASE WHEN num_open_account_days = 30 THEN balance_eod END AS day_30_balance,
      CASE WHEN num_open_account_days = 45 THEN balance_eod END AS day_45_balance,
      CASE WHEN num_open_account_days = 60 THEN balance_eod END AS day_60_balance,
      CASE WHEN num_open_account_days = 90 THEN balance_eod END AS day_90_balance,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 7 THEN balance_eod END AS day_7_balance_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 14 THEN balance_eod END AS day_14_balance_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 30 THEN balance_eod END AS day_30_balance_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 45 THEN balance_eod END AS day_45_balance_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 60 THEN balance_eod END AS day_60_balance_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 90 THEN balance_eod END AS day_90_balance_no_season
    FROM enrollments e
    JOIN curated.fact_customer_day fcd ON e.user_ref = fcd.user_ref
    WHERE (num_open_account_days IN (0, 7, 14, 30, 45, 60, 90)
      OR date_diff('day', e.event_date, fcd.date) IN (7, 14, 30, 45, 60, 90))
    ) balance
  GROUP BY 1
),

swipes_day_x AS ( --normal metrics anchored to enrollment date; no season metrics anchored to traffic date
  SELECT
    user_ref,
    SUM(day_7_swipes) AS day_7_swipes,
    SUM(day_14_swipes) AS day_14_swipes,
    SUM(day_30_swipes) AS day_30_swipes,
    SUM(day_45_swipes) AS day_45_swipes,
    SUM(day_60_swipes) AS day_60_swipes,
    SUM(day_90_swipes) AS day_90_swipes,
    SUM(day_7_swipes_no_season) AS day_7_swipes_no_season,
    SUM(day_14_swipes_no_season) AS day_14_swipes_no_season,
    SUM(day_30_swipes_no_season) AS day_30_swipes_no_season,
    SUM(day_45_swipes_no_season) AS day_45_swipes_no_season,
    SUM(day_60_swipes_no_season) AS day_60_swipes_no_season,
    SUM(day_90_swipes_no_season) AS day_90_swipes_no_season
  FROM (
    SELECT
      e.user_ref,
      fcd.date,
      CASE WHEN num_open_account_days = 0 THEN running_total_swipe_count END AS day_0_swipes,
      CASE WHEN num_open_account_days = 7 THEN running_total_swipe_count END AS day_7_swipes,
      CASE WHEN num_open_account_days = 14 THEN running_total_swipe_count END AS day_14_swipes,
      CASE WHEN num_open_account_days = 30 THEN running_total_swipe_count END AS day_30_swipes,
      CASE WHEN num_open_account_days = 45 THEN running_total_swipe_count END AS day_45_swipes,
      CASE WHEN num_open_account_days = 60 THEN running_total_swipe_count END AS day_60_swipes,
      CASE WHEN num_open_account_days = 90 THEN running_total_swipe_count END AS day_90_swipes,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 7 THEN running_total_swipe_count END AS day_7_swipes_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 14 THEN running_total_swipe_count END AS day_14_swipes_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 30 THEN running_total_swipe_count END AS day_30_swipes_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 45 THEN running_total_swipe_count END AS day_45_swipes_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 60 THEN running_total_swipe_count END AS day_60_swipes_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 90 THEN running_total_swipe_count END AS day_90_swipes_no_season
    FROM enrollments e
    JOIN curated.fact_customer_day fcd on e.user_ref = fcd.user_ref
    WHERE (num_open_account_days IN (0, 7, 14, 30, 45, 60, 90)
      OR date_diff('day', e.event_date, fcd.date) IN (7, 14, 30, 45, 60, 90))
    ) swipes
  GROUP BY 1
),

funded_day_x AS ( --normal metrics anchored to enrollment date; no season metrics anchored to traffic date
  SELECT
    user_ref,
    SUM(funded_m0_flag) AS funded_accounts_m0,
    SUM(funded_m0_no_season_flag) AS funded_accounts_m0_no_season,
    SUM(funded_day_7_flag) AS funded_accounts_day_7,
    SUM(funded_day_14_flag) AS funded_accounts_day_14,
    SUM(funded_day_30_flag) AS funded_accounts_day_30,
    SUM(funded_day_45_flag) AS funded_accounts_day_45,
    SUM(funded_day_60_flag) AS funded_accounts_day_60,
    SUM(funded_day_90_flag) AS funded_accounts_day_90,
    SUM(funded_day_7_no_season_flag) AS funded_accounts_day_7_no_season,
    SUM(funded_day_14_no_season_flag) AS funded_accounts_day_14_no_season,
    SUM(funded_day_30_no_season_flag) AS funded_accounts_day_30_no_season,
    SUM(funded_day_45_no_season_flag) AS funded_accounts_day_45_no_season,
    SUM(funded_day_60_no_season_flag) AS funded_accounts_day_60_no_season,
    SUM(funded_day_90_no_season_flag) AS funded_accounts_day_90_no_season
  FROM (
    SELECT DISTINCT
      e.user_ref,
      da.account_ref,
      DATE_TRUNC('day', da.first_deposit_ts)::date AS first_deposit_date,
      CASE
        WHEN cohort_date = DATE_TRUNC('month', first_deposit_ts)::date THEN 1
        ELSE 0
      END funded_m0_flag,
      CASE
        WHEN DATE_TRUNC('month', e.event_date)::date = DATE_TRUNC('month', first_deposit_ts)::date THEN 1
        ELSE 0
      END funded_m0_no_season_flag,
      CASE WHEN e.first_account_open_date >= first_deposit_ts::date - 7 THEN 1 ELSE 0 END AS funded_day_7_flag,
      CASE WHEN e.first_account_open_date >= first_deposit_ts::date - 14 THEN 1 ELSE 0 END AS funded_day_14_flag,
      CASE WHEN e.first_account_open_date >= first_deposit_ts::date - 30 THEN 1 ELSE 0 END AS funded_day_30_flag,
      CASE WHEN e.first_account_open_date >= first_deposit_ts::date - 45 THEN 1 ELSE 0 END AS funded_day_45_flag,
      CASE WHEN e.first_account_open_date >= first_deposit_ts::date - 60 THEN 1 ELSE 0 END AS funded_day_60_flag,
      CASE WHEN e.first_account_open_date >= first_deposit_ts::date - 90 THEN 1 ELSE 0 END AS funded_day_90_flag,
      CASE WHEN e.event_date >= first_deposit_ts::date - 7 THEN 1 ELSE 0 END AS funded_day_7_no_season_flag,
      CASE WHEN e.event_date >= first_deposit_ts::date - 14 THEN 1 ELSE 0 END AS funded_day_14_no_season_flag,
      CASE WHEN e.event_date >= first_deposit_ts::date - 30 THEN 1 ELSE 0 END AS funded_day_30_no_season_flag,
      CASE WHEN e.event_date >= first_deposit_ts::date - 45 THEN 1 ELSE 0 END AS funded_day_45_no_season_flag,
      CASE WHEN e.event_date >= first_deposit_ts::date - 60 THEN 1 ELSE 0 END AS funded_day_60_no_season_flag,
      CASE WHEN e.event_date >= first_deposit_ts::date - 90 THEN 1 ELSE 0 END AS funded_day_90_no_season_flag
    FROM enrollments e
    JOIN curated.fact_user_account fua ON e.user_ref = fua.user_ref
    JOIN curated.dim_account da ON fua.account_ref = da.account_ref
    WHERE da.first_deposit_ts IS NOT NULL
    ) funded
  GROUP BY 1
),

card_activation_day_x AS (
  SELECT
    user_ref,
    CASE WHEN first_account_open_date >= first_card_activation_date - 7 THEN 1 ELSE 0 END AS card_activated_day_7,
    CASE WHEN first_account_open_date >= first_card_activation_date - 10 THEN 1 ELSE 0 END AS card_activated_day_10,
    CASE WHEN first_account_open_date >= first_card_activation_date - 14 THEN 1 ELSE 0 END AS card_activated_day_14,
    CASE WHEN first_account_open_date >= first_card_activation_date - 30 THEN 1 ELSE 0 END AS card_activated_day_30,
    CASE WHEN first_account_open_date >= first_card_activation_date - 45 THEN 1 ELSE 0 END AS card_activated_day_45,
    CASE WHEN first_account_open_date >= first_card_activation_date - 60 THEN 1 ELSE 0 END AS card_activated_day_60,
    CASE WHEN first_account_open_date >= first_card_activation_date - 90 THEN 1 ELSE 0 END AS card_activated_day_90
  FROM (
    SELECT
      e.user_ref,
      e.first_account_open_date,
      first_card_activation_ts::date AS first_card_activation_date
    FROM enrollments e
    JOIN curated.dim_user du ON e.user_ref = du.user_ref
    ) activation
),

fraudster_day_x AS ( --normal metrics anchored to enrollment date; no season metrics anchored to traffic date
  SELECT
    user_ref,
    SUM(day_7_fraudster_flag) AS day_7_fraudster_flag,
    SUM(day_14_fraudster_flag) AS day_14_fraudster_flag,
    SUM(day_30_fraudster_flag) AS day_30_fraudster_flag,
    SUM(day_45_fraudster_flag) AS day_45_fraudster_flag,
    SUM(day_60_fraudster_flag) AS day_60_fraudster_flag,
    SUM(day_90_fraudster_flag) AS day_90_fraudster_flag,
    SUM(day_7_fraudster_flag_no_season) AS day_7_fraudster_flag_no_season,
    SUM(day_14_fraudster_flag_no_season) AS day_14_fraudster_flag_no_season,
    SUM(day_30_fraudster_flag_no_season) AS day_30_fraudster_flag_no_season,
    SUM(day_45_fraudster_flag_no_season) AS day_45_fraudster_flag_no_season,
    SUM(day_60_fraudster_flag_no_season) AS day_60_fraudster_flag_no_season,
    SUM(day_90_fraudster_flag_no_season) AS day_90_fraudster_flag_no_season
  FROM (
    SELECT
      e.user_ref,
      fcd.date,
      CASE WHEN num_open_account_days = 7 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_7_fraudster_flag,
      CASE WHEN num_open_account_days = 14 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_14_fraudster_flag,
      CASE WHEN num_open_account_days = 30 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_30_fraudster_flag,
      CASE WHEN num_open_account_days = 45 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_45_fraudster_flag,
      CASE WHEN num_open_account_days = 60 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_60_fraudster_flag,
      CASE WHEN num_open_account_days = 90 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_90_fraudster_flag,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 7 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_7_fraudster_flag_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 14 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_14_fraudster_flag_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 30 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_30_fraudster_flag_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 45 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_45_fraudster_flag_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 60 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_60_fraudster_flag_no_season,
      CASE WHEN date_diff('day', e.event_date, fcd.date) = 90 AND f._flagged_as_fraudster IS TRUE THEN 1 ELSE 0 END AS day_90_fraudster_flag_no_season
    FROM enrollments e
    JOIN curated.fact_customer_day fcd on e.user_ref = fcd.user_ref
    LEFT JOIN fact_customer_fraudster_day f ON e.user_ref = f.user_ref
      AND fcd.date = f.date
    WHERE (num_open_account_days IN (7, 14, 30, 45, 60, 90)
      OR date_diff('day', e.event_date, fcd.date) IN (7, 14, 30, 45, 60, 90))
    ) fraudster
  GROUP BY 1
),


bbva_active_day_x AS ( --normal metrics anchored to enrollment date; no season metrics anchored to traffic date
  SELECT
    bbva.user_ref,
    CASE WHEN (day_0_30_avg_balance >= 100 OR day_0_30_transaction_count > 0 OR day_0_30_has_loans > 0) AND day_30_fraudster_flag IS FALSE THEN bbva.user_ref END AS day_30_bbva_active,
    CASE WHEN (day_31_60_avg_balance >= 100 OR day_31_60_transaction_count > 0 OR day_31_60_has_loans > 0) AND day_60_fraudster_flag IS FALSE THEN bbva.user_ref END AS day_60_bbva_active,
    CASE WHEN (day_61_90_avg_balance >= 100 OR day_61_90_transaction_count > 0 OR day_61_90_has_loans > 0) AND day_90_fraudster_flag IS FALSE THEN bbva.user_ref END AS day_90_bbva_active,
    CASE WHEN (day_0_30_avg_balance_no_season >= 100 OR day_0_30_transaction_count_no_season > 0 OR day_0_30_has_loans_no_season > 0) AND day_30_fraudster_flag_no_season IS FALSE THEN bbva.user_ref END AS day_30_bbva_active_no_season,
    CASE WHEN (day_31_60_avg_balance_no_season >= 100 OR day_31_60_transaction_count_no_season > 0 OR day_31_60_has_loans_no_season > 0) AND day_60_fraudster_flag_no_season IS FALSE THEN bbva.user_ref END AS day_60_bbva_active_no_season,
    CASE WHEN (day_61_90_avg_balance_no_season >= 100 OR day_61_90_transaction_count_no_season > 0 OR day_61_90_has_loans_no_season > 0) AND day_90_fraudster_flag_no_season IS FALSE THEN bbva.user_ref END AS day_90_bbva_active_no_season
  FROM (
    SELECT
      e.user_ref,
      AVG(CASE WHEN num_open_account_days BETWEEN 0 AND 30 THEN balance_eod END) AS day_0_30_avg_balance,
      SUM(CASE WHEN num_open_account_days BETWEEN 0 AND 30 THEN bbva_active_transaction_count END) AS day_0_30_transaction_count,
      SUM(CASE WHEN num_open_account_days BETWEEN 0 AND 30 THEN num_loan_accounts END) AS day_0_30_has_loans,
      AVG(CASE WHEN num_open_account_days BETWEEN 31 AND 60 THEN balance_eod END) AS day_31_60_avg_balance,
      SUM(CASE WHEN num_open_account_days BETWEEN 31 AND 60 THEN bbva_active_transaction_count END) AS day_31_60_transaction_count,
      SUM(CASE WHEN num_open_account_days BETWEEN 31 AND 60 THEN num_loan_accounts END) AS day_31_60_has_loans,
      AVG(CASE WHEN num_open_account_days BETWEEN 61 AND 90 THEN balance_eod END) AS day_61_90_avg_balance,
      SUM(CASE WHEN num_open_account_days BETWEEN 61 AND 90 THEN bbva_active_transaction_count END) AS day_61_90_transaction_count,
      SUM(CASE WHEN num_open_account_days BETWEEN 61 AND 90 THEN num_loan_accounts END) AS day_61_90_has_loans,
      AVG(CASE WHEN date_diff('day', e.event_date, fcd.date) BETWEEN 0 AND 30 THEN balance_eod END) AS day_0_30_avg_balance_no_season,
      SUM(CASE WHEN date_diff('day', e.event_date, fcd.date) BETWEEN 0 AND 30 THEN bbva_active_transaction_count END) AS day_0_30_transaction_count_no_season,
      SUM(CASE WHEN date_diff('day', e.event_date, fcd.date) BETWEEN 0 AND 30 THEN num_loan_accounts END) AS day_0_30_has_loans_no_season,
      AVG(CASE WHEN date_diff('day', e.event_date, fcd.date) BETWEEN 31 AND 60 THEN balance_eod END) AS day_31_60_avg_balance_no_season,
      SUM(CASE WHEN date_diff('day', e.event_date, fcd.date) BETWEEN 31 AND 60 THEN bbva_active_transaction_count END) AS day_31_60_transaction_count_no_season,
      SUM(CASE WHEN date_diff('day', e.event_date, fcd.date) BETWEEN 31 AND 60 THEN num_loan_accounts END) AS day_31_60_has_loans_no_season,
      AVG(CASE WHEN date_diff('day', e.event_date, fcd.date) BETWEEN 61 AND 90 THEN balance_eod END) AS day_61_90_avg_balance_no_season,
      SUM(CASE WHEN date_diff('day', e.event_date, fcd.date) BETWEEN 61 AND 90 THEN bbva_active_transaction_count END) AS day_61_90_transaction_count_no_season,
      SUM(CASE WHEN date_diff('day', e.event_date, fcd.date) BETWEEN 61 AND 90 THEN num_loan_accounts END) AS day_61_90_has_loans_no_season
    FROM enrollments e
    JOIN curated.fact_customer_day fcd ON e.user_ref = fcd.user_ref
    WHERE num_open_account_days BETWEEN 0 AND 90
    GROUP BY 1
    ) bbva
  JOIN fraudster_day_x f ON bbva.user_ref = f.user_ref
),

customer_level_dataset AS ( --normal metrics anchored to enrollment date; no season metrics anchored to traffic date
  SELECT DISTINCT
    t.event_ref,
    t.event_date,
    source,
    marketing_type,
    search_type,
    campaign_type,
    marketing_product,
    channel,
    medium,
    campaign_name,
    source_campaign_id,
    ad_group_ref,
    ad_group_name,
    app.visitor_ref AS started_app,
    CASE WHEN DATE_TRUNC('month', t.event_date) = DATE_TRUNC('month', app.app_start_date) THEN app.visitor_ref END AS started_app_m0,
    CASE WHEN t.event_date = app.app_start_date THEN app.visitor_ref END AS started_app_day_0,       --note this is anchored off TRAFFIC date
    CASE WHEN t.event_date >= app.app_start_date - 1 THEN app.visitor_ref END AS started_app_day_1,
    CASE WHEN t.event_date >= app.app_start_date - 3 THEN app.visitor_ref END AS started_app_day_3,
    CASE WHEN t.event_date >= app.app_start_date - 7 THEN app.visitor_ref END AS started_app_day_7,
    CASE WHEN t.event_date >= app.app_start_date - 14 THEN app.visitor_ref END AS started_app_day_14,
    CASE WHEN t.event_date >= app.app_start_date - 30 THEN app.visitor_ref END AS started_app_day_30,
    c.user_ref AS created_creds,
    CASE WHEN DATE_TRUNC('month', t.event_date) = DATE_TRUNC('month', c.created_date) THEN c.user_ref END AS created_creds_m0,
    CASE WHEN t.event_date = c.created_date THEN c.user_ref END AS created_creds_day_0,       --note this is anchored off TRAFFIC date
    CASE WHEN t.event_date >= c.created_date - 1 THEN c.user_ref END AS created_creds_day_1,
    CASE WHEN t.event_date >= c.created_date - 3 THEN c.user_ref END AS created_creds_day_3,
    CASE WHEN t.event_date >= c.created_date - 7 THEN c.user_ref END AS created_creds_day_7,
    CASE WHEN t.event_date >= c.created_date - 14 THEN c.user_ref END AS created_creds_day_14,
    CASE WHEN t.event_date >= c.created_date - 30 THEN c.user_ref END AS created_creds_day_30,
    CASE WHEN c.applied_date IS NOT NULL THEN c.user_ref END AS applied,
    CASE WHEN DATE_TRUNC('month', t.event_date) = DATE_TRUNC('month', c.applied_date) THEN c.user_ref END AS applied_m0,
    CASE WHEN t.event_date = c.applied_date THEN c.user_ref END AS applied_day_0,
    CASE WHEN t.event_date >= c.applied_date - 1 THEN c.user_ref END AS applied_day_1,
    CASE WHEN t.event_date >= c.applied_date - 3 THEN c.user_ref END AS applied_day_3,
    CASE WHEN t.event_date >= c.applied_date - 7 THEN c.user_ref END AS applied_day_7,
    CASE WHEN t.event_date >= c.applied_date - 14 THEN c.user_ref END AS applied_day_14,      --note to tell andy applied by x date
    CASE WHEN t.event_date >= c.applied_date - 30 THEN c.user_ref END AS applied_day_30,
    e.user_ref AS enrolled,
    CASE WHEN DATE_TRUNC('month', t.event_date) = DATE_TRUNC('month', e.first_account_open_date) THEN e.user_ref END AS enrolled_m0,
    CASE WHEN t.event_date = e.first_account_open_date THEN e.user_ref END AS enrolled_day_0, --note this is anchored off TRAFFIC date
    CASE WHEN t.event_date >= e.first_account_open_date - 1 THEN e.user_ref END AS enrolled_day_1,
    CASE WHEN t.event_date >= e.first_account_open_date - 3 THEN e.user_ref END AS enrolled_day_3,
    CASE WHEN t.event_date >= e.first_account_open_date - 7 THEN e.user_ref END AS enrolled_day_7,
    CASE WHEN t.event_date >= e.first_account_open_date - 14 THEN e.user_ref END AS enrolled_day_14,
    CASE WHEN t.event_date >= e.first_account_open_date - 30 THEN e.user_ref END AS enrolled_day_30,
    e.first_account_open_date,
    balance_m0,
    balance_m0_no_season,
    swipes_m0,
    swipes_m0_no_season,
    accounts_m0,
    day_7_balance,
    day_14_balance,
    day_30_balance,
    day_45_balance,
    day_60_balance,
    day_90_balance,
    day_7_balance_no_season,
    day_14_balance_no_season,
    day_30_balance_no_season,
    day_45_balance_no_season,
    day_60_balance_no_season,
    day_90_balance_no_season,
    funded_accounts_m0,
    funded_accounts_m0_no_season,
    funded_accounts_day_7,
    funded_accounts_day_14,
    funded_accounts_day_30,
    funded_accounts_day_45,
    funded_accounts_day_60,
    funded_accounts_day_90,
    funded_accounts_day_7_no_season,
    funded_accounts_day_14_no_season,
    funded_accounts_day_30_no_season,
    funded_accounts_day_45_no_season,
    funded_accounts_day_60_no_season,
    funded_accounts_day_90_no_season,
    CASE WHEN funded_accounts_m0 > 0 THEN f.user_ref END AS funded_m0,
    CASE WHEN funded_accounts_m0_no_season > 0 THEN f.user_ref END AS funded_m0_no_season,
    CASE WHEN funded_accounts_day_7 > 0 THEN f.user_ref END AS funded_day_7,
    CASE WHEN funded_accounts_day_14 > 0 THEN f.user_ref END AS funded_day_14,
    CASE WHEN funded_accounts_day_30 > 0 THEN f.user_ref END AS funded_day_30,
    CASE WHEN funded_accounts_day_45 > 0 THEN f.user_ref END AS funded_day_45,
    CASE WHEN funded_accounts_day_60 > 0 THEN f.user_ref END AS funded_day_60,
    CASE WHEN funded_accounts_day_90 > 0 THEN f.user_ref END AS funded_day_90,
    CASE WHEN funded_accounts_day_7_no_season > 0 THEN f.user_ref END AS funded_day_7_no_season,
    CASE WHEN funded_accounts_day_14_no_season > 0 THEN f.user_ref END AS funded_day_14_no_season,
    CASE WHEN funded_accounts_day_30_no_season > 0 THEN f.user_ref END AS funded_day_30_no_season,
    CASE WHEN funded_accounts_day_45_no_season > 0 THEN f.user_ref END AS funded_day_45_no_season,
    CASE WHEN funded_accounts_day_60_no_season > 0 THEN f.user_ref END AS funded_day_60_no_season,
    CASE WHEN funded_accounts_day_90_no_season > 0 THEN f.user_ref END AS funded_day_90_no_season,
    day_7_swipes,
    day_14_swipes,
    day_30_swipes,
    day_45_swipes,
    day_60_swipes,
    day_90_swipes,
    day_7_swipes_no_season,
    day_14_swipes_no_season,
    day_30_swipes_no_season,
    day_45_swipes_no_season,
    day_60_swipes_no_season,
    day_90_swipes_no_season,
    CASE WHEN swipes_m0 > 0 THEN sd.user_ref END AS swiped_m0,
    CASE WHEN swipes_m0_no_season > 0 THEN sd.user_ref END AS swiped_m0_no_season,
    CASE WHEN day_7_swipes > 0 THEN sd.user_ref END AS swiped_day_7,
    CASE WHEN day_14_swipes > 0 THEN sd.user_ref END AS swiped_day_14,
    CASE WHEN day_30_swipes > 0 THEN sd.user_ref END AS swiped_day_30,
    CASE WHEN day_45_swipes > 0 THEN sd.user_ref END AS swiped_day_45,
    CASE WHEN day_60_swipes > 0 THEN sd.user_ref END AS swiped_day_60,
    CASE WHEN day_90_swipes > 0 THEN sd.user_ref END AS swiped_day_90,
    CASE WHEN day_7_swipes_no_season > 0 THEN sd.user_ref END AS swiped_day_7_no_season,
    CASE WHEN day_14_swipes_no_season > 0 THEN sd.user_ref END AS swiped_day_14_no_season,
    CASE WHEN day_30_swipes_no_season > 0 THEN sd.user_ref END AS swiped_day_30_no_season,
    CASE WHEN day_45_swipes_no_season > 0 THEN sd.user_ref END AS swiped_day_45_no_season,
    CASE WHEN day_60_swipes_no_season > 0 THEN sd.user_ref END AS swiped_day_60_no_season,
    CASE WHEN day_90_swipes_no_season > 0 THEN sd.user_ref END AS swiped_day_90_no_season,
    card_activated_day_7,
    card_activated_day_10,
    card_activated_day_14,
    card_activated_day_30,
    card_activated_day_45,
    card_activated_day_60,
    card_activated_day_90,
    CASE WHEN fraudster_flag_m0 IS TRUE THEN m0.user_ref END AS fraudster_flag_m0,
    CASE WHEN fraudster_flag_m0_no_season IS TRUE THEN ns.user_ref END AS fraudster_flag_m0_no_season,
    CASE WHEN day_7_fraudster_flag > 0 THEN fra.user_ref END AS day_7_fraudster_flag,
    CASE WHEN day_14_fraudster_flag > 0 THEN fra.user_ref END AS day_14_fraudster_flag,
    CASE WHEN day_30_fraudster_flag > 0 THEN fra.user_ref END AS day_30_fraudster_flag,
    CASE WHEN day_45_fraudster_flag > 0 THEN fra.user_ref END AS day_45_fraudster_flag,
    CASE WHEN day_60_fraudster_flag > 0 THEN fra.user_ref END AS day_60_fraudster_flag,
    CASE WHEN day_90_fraudster_flag > 0 THEN fra.user_ref END AS day_90_fraudster_flag,
    CASE WHEN day_7_fraudster_flag_no_season > 0 THEN fra.user_ref END AS day_7_fraudster_flag_no_season,
    CASE WHEN day_14_fraudster_flag_no_season > 0 THEN fra.user_ref END AS day_14_fraudster_flag_no_season,
    CASE WHEN day_30_fraudster_flag_no_season > 0 THEN fra.user_ref END AS day_30_fraudster_flag_no_season,
    CASE WHEN day_45_fraudster_flag_no_season > 0 THEN fra.user_ref END AS day_45_fraudster_flag_no_season,
    CASE WHEN day_60_fraudster_flag_no_season > 0 THEN fra.user_ref END AS day_60_fraudster_flag_no_season,
    CASE WHEN day_90_fraudster_flag_no_season > 0 THEN fra.user_ref END AS day_90_fraudster_flag_no_season,
    bbva_active_m0,
    bbva_active_m0_no_season,
    day_30_bbva_active,
    day_60_bbva_active,
    day_90_bbva_active,
    day_30_bbva_active_no_season,
    day_60_bbva_active_no_season,
    day_90_bbva_active_no_season
  FROM traffic t
  LEFT JOIN app_start app ON t.visitor_ref = app.visitor_ref
  LEFT JOIN creds_to_apps c ON t.user_ref = c.user_ref
  LEFT JOIN enrollments e ON t.user_ref = e.user_ref
  LEFT JOIN accounts ac ON e.user_ref = ac.user_ref
  LEFT JOIN user_level_m0 m0 ON e.user_ref = m0.user_ref
  LEFT JOIN user_level_m0_non_seasoning ns ON e.user_ref = ns.user_ref
  LEFT JOIN balance_day_x b ON e.user_ref = b.user_ref
  LEFT JOIN funded_day_x f ON e.user_ref = f.user_ref
  LEFT JOIN swipes_day_x sd ON e.user_ref = sd.user_ref
  LEFT JOIN card_activation_day_x ca ON e.user_ref = ca.user_ref
  LEFT JOIN fraudster_day_x fra ON e.user_ref = fra.user_ref
  LEFT JOIN bbva_active_day_x a ON e.user_ref = a.user_ref
),

impressions_clicks AS (
  SELECT
    date,
    source,
    campaign_name,
    source_campaign_id,
    ad_group_id,
    ad_group_name,
    SUM(num_impressions) AS num_impressions,
    SUM(num_clicks) AS num_clicks,
    SUM(spend_amount) AS spend_amount
  FROM curated.fact_paid_campaign_day
--  WHERE date between '2019-01-01' and '2019-12-31' --2019 version
  WHERE date >= '2020-01-01' --2020 version
--  WHERE date >= '2020-09-01' --testing version
  GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
  event_date,
  channel,
  cld.source,
  medium,
  search_type,
  marketing_type,
  campaign_type,
  marketing_product,
  cld.campaign_name,
  cld.source_campaign_id,
  cld.ad_group_ref,
  cld.ad_group_name,
  nvl(ic1.num_impressions, ic2.num_impressions) AS num_impressions,
  nvl(ic1.num_clicks, ic2.num_clicks) AS num_clicks,
  nvl(ic1.spend_amount, ic2.spend_amount) AS spend_amount,
  COUNT(DISTINCT event_ref) AS traffic,
  COUNT(DISTINCT started_app) AS started_app,
  COUNT(DISTINCT started_app_m0) AS started_app_m0,
  COUNT(DISTINCT started_app_day_0) AS started_app_day_0,
  COUNT(DISTINCT started_app_day_1) AS started_app_day_1,
  COUNT(DISTINCT started_app_day_3) AS started_app_day_3,
  COUNT(DISTINCT started_app_day_7) AS started_app_day_7,
  COUNT(DISTINCT started_app_day_14) AS started_app_day_14,
  COUNT(DISTINCT started_app_day_30) AS started_app_day_30,
  COUNT(DISTINCT created_creds) AS created_creds,
  COUNT(DISTINCT created_creds_m0) AS created_creds_m0,
  COUNT(DISTINCT created_creds_day_0) AS created_creds_day_0,
  COUNT(DISTINCT created_creds_day_1) AS created_creds_day_1,
  COUNT(DISTINCT created_creds_day_3) AS created_creds_day_3,
  COUNT(DISTINCT created_creds_day_7) AS created_creds_day_7,
  COUNT(DISTINCT created_creds_day_14) AS created_creds_day_14,
  COUNT(DISTINCT created_creds_day_30) AS created_creds_day_30,
  COUNT(DISTINCT applied) AS applied,
  COUNT(DISTINCT applied_m0) AS applied_m0,
  COUNT(DISTINCT applied_day_0) AS applied_day_0,
  COUNT(DISTINCT applied_day_1) AS applied_day_1,
  COUNT(DISTINCT applied_day_3) AS applied_day_3,
  COUNT(DISTINCT applied_day_7) AS applied_day_7,
  COUNT(DISTINCT applied_day_14) AS applied_day_14,
  COUNT(DISTINCT applied_day_30) AS applied_day_30,
  COUNT(DISTINCT enrolled) AS enrolled,
  COUNT(DISTINCT enrolled_m0) AS enrolled_m0,
  COUNT(DISTINCT enrolled_day_0) AS enrolled_day_0,
  COUNT(DISTINCT enrolled_day_1) AS enrolled_day_1,
  COUNT(DISTINCT enrolled_day_3) AS enrolled_day_3,
  COUNT(DISTINCT enrolled_day_7) AS enrolled_day_7,
  COUNT(DISTINCT enrolled_day_14) AS enrolled_day_14,
  COUNT(DISTINCT enrolled_day_30) AS enrolled_day_30,
  SUM(accounts_m0) AS accounts_m0,
  SUM(balance_m0) AS total_balance_m0,
  SUM(balance_m0_no_season) AS total_balance_m0_no_season,
  SUM(day_7_balance) AS total_day_7_balance,
  SUM(day_14_balance) AS total_day_14_balance,
  SUM(day_30_balance) AS total_day_30_balance,
  SUM(day_45_balance) AS total_day_45_balance,
  SUM(day_60_balance) AS total_day_60_balance,
  SUM(day_90_balance) AS total_day_90_balance,
  SUM(day_7_balance_no_season) AS total_day_7_balance_no_season,
  SUM(day_14_balance_no_season) AS total_day_14_balance_no_season,
  SUM(day_30_balance_no_season) AS total_day_30_balance_no_season,
  SUM(day_45_balance_no_season) AS total_day_45_balance_no_season,
  SUM(day_60_balance_no_season) AS total_day_60_balance_no_season,
  SUM(day_90_balance_no_season) AS total_day_90_balance_no_season,
  SUM(funded_accounts_m0) AS funded_accounts_m0,
  SUM(funded_accounts_m0_no_season) AS funded_accounts_m0_no_season,
  SUM(funded_accounts_day_7) AS funded_accounts_day_7,
  SUM(funded_accounts_day_14) AS funded_accounts_day_14,
  SUM(funded_accounts_day_30) AS funded_accounts_day_30,
  SUM(funded_accounts_day_45) AS funded_accounts_day_45,
  SUM(funded_accounts_day_60) AS funded_accounts_day_60,
  SUM(funded_accounts_day_90) AS funded_accounts_day_90,
  SUM(funded_accounts_day_7_no_season) AS funded_accounts_day_7_no_season,
  SUM(funded_accounts_day_14_no_season) AS funded_accounts_day_14_no_season,
  SUM(funded_accounts_day_30_no_season) AS funded_accounts_day_30_no_season,
  SUM(funded_accounts_day_45_no_season) AS funded_accounts_day_45_no_season,
  SUM(funded_accounts_day_60_no_season) AS funded_accounts_day_60_no_season,
  SUM(funded_accounts_day_90_no_season) AS funded_accounts_day_90_no_season,
  COUNT(DISTINCT funded_m0) AS funded_m0,
  COUNT(DISTINCT funded_m0_no_season) AS funded_m0_no_season,
  COUNT(DISTINCT funded_day_7) AS funded_day_7,
  COUNT(DISTINCT funded_day_14) AS funded_day_14,
  COUNT(DISTINCT funded_day_30) AS funded_day_30,
  COUNT(DISTINCT funded_day_45) AS funded_day_45,
  COUNT(DISTINCT funded_day_60) AS funded_day_60,
  COUNT(DISTINCT funded_day_90) AS funded_day_90,
  COUNT(DISTINCT funded_day_7_no_season) AS funded_day_7_no_season,
  COUNT(DISTINCT funded_day_14_no_season) AS funded_day_14_no_season,
  COUNT(DISTINCT funded_day_30_no_season) AS funded_day_30_no_season,
  COUNT(DISTINCT funded_day_45_no_season) AS funded_day_45_no_season,
  COUNT(DISTINCT funded_day_60_no_season) AS funded_day_60_no_season,
  COUNT(DISTINCT funded_day_90_no_season) AS funded_day_90_no_season,
  SUM(swipes_m0) AS total_swipes_m0,
  SUM(swipes_m0_no_season) AS total_swipes_m0_no_season,
  SUM(day_7_swipes) AS day_7_swipes,
  SUM(day_14_swipes) AS day_14_swipes,
  SUM(day_30_swipes) AS day_30_swipes,
  SUM(day_45_swipes) AS day_45_swipes,
  SUM(day_60_swipes) AS day_60_swipes,
  SUM(day_90_swipes) AS day_90_swipes,
  SUM(day_7_swipes_no_season) AS day_7_swipes_no_season,
  SUM(day_14_swipes_no_season) AS day_14_swipes_no_season,
  SUM(day_30_swipes_no_season) AS day_30_swipes_no_season,
  SUM(day_45_swipes_no_season) AS day_45_swipes_no_season,
  SUM(day_60_swipes_no_season) AS day_60_swipes_no_season,
  SUM(day_90_swipes_no_season) AS day_90_swipes_no_season,
  COUNT(DISTINCT swiped_m0) AS swiped_m0,
  COUNT(DISTINCT swiped_m0_no_season) AS swiped_m0_no_season,
  COUNT(DISTINCT swiped_day_7) AS swiped_day_7,
  COUNT(DISTINCT swiped_day_14) AS swiped_day_14,
  COUNT(DISTINCT swiped_day_30) AS swiped_day_30,
  COUNT(DISTINCT swiped_day_45) AS swiped_day_45,
  COUNT(DISTINCT swiped_day_60) AS swiped_day_60,
  COUNT(DISTINCT swiped_day_90) AS swiped_day_90,
  COUNT(DISTINCT swiped_day_7_no_season) AS swiped_day_7_no_season,
  COUNT(DISTINCT swiped_day_14_no_season) AS swiped_day_14_no_season,
  COUNT(DISTINCT swiped_day_30_no_season) AS swiped_day_30_no_season,
  COUNT(DISTINCT swiped_day_45_no_season) AS swiped_day_45_no_season,
  COUNT(DISTINCT swiped_day_60_no_season) AS swiped_day_60_no_season,
  COUNT(DISTINCT swiped_day_90_no_season) AS swiped_day_90_no_season,
  SUM(card_activated_day_7) AS card_activated_day_7,
  SUM(card_activated_day_10) AS card_activated_day_10,
  SUM(card_activated_day_14) AS card_activated_day_14,
  SUM(card_activated_day_30) AS card_activated_day_30,
  SUM(card_activated_day_45) AS card_activated_day_45,
  SUM(card_activated_day_60) AS card_activated_day_60,
  SUM(card_activated_day_90) AS card_activated_day_90,
  COUNT(DISTINCT fraudster_flag_m0) AS fraudster_flag_m0,
  COUNT(DISTINCT fraudster_flag_m0_no_season) AS fraudster_flag_m0_no_season,
  COUNT(DISTINCT day_7_fraudster_flag) AS day_7_fraudster_flag,
  COUNT(DISTINCT day_14_fraudster_flag) AS day_14_fraudster_flag,
  COUNT(DISTINCT day_30_fraudster_flag) AS day_30_fraudster_flag,
  COUNT(DISTINCT day_45_fraudster_flag) AS day_45_fraudster_flag,
  COUNT(DISTINCT day_60_fraudster_flag) AS day_60_fraudster_flag,
  COUNT(DISTINCT day_90_fraudster_flag) AS day_90_fraudster_flag,
  COUNT(DISTINCT day_7_fraudster_flag_no_season) AS day_7_fraudster_flag_no_season,
  COUNT(DISTINCT day_14_fraudster_flag_no_season) AS day_14_fraudster_flag_no_season,
  COUNT(DISTINCT day_30_fraudster_flag_no_season) AS day_30_fraudster_flag_no_season,
  COUNT(DISTINCT day_45_fraudster_flag_no_season) AS day_45_fraudster_flag_no_season,
  COUNT(DISTINCT day_60_fraudster_flag_no_season) AS day_60_fraudster_flag_no_season,
  COUNT(DISTINCT day_90_fraudster_flag_no_season) AS day_90_fraudster_flag_no_season,
  COUNT(DISTINCT bbva_active_m0) AS bbva_active_m0,
  COUNT(DISTINCT bbva_active_m0_no_season) AS bbva_active_m0_no_season,
  COUNT(DISTINCT day_30_bbva_active) AS day_30_bbva_active,
  COUNT(DISTINCT day_60_bbva_active) AS day_60_bbva_active,
  COUNT(DISTINCT day_90_bbva_active) AS day_90_bbva_active,
  COUNT(DISTINCT day_30_bbva_active_no_season) AS day_30_bbva_active_no_season,
  COUNT(DISTINCT day_60_bbva_active_no_season) AS day_60_bbva_active_no_season,
  COUNT(DISTINCT day_90_bbva_active_no_season) AS day_90_bbva_active_no_season
FROM customer_level_dataset cld
LEFT JOIN impressions_clicks ic1 ON cld.event_date = ic1.date --this join grabs ad group-level campaigns
  AND cld.source_campaign_id = ic1.source_campaign_id
  AND cld.ad_group_ref = ic1.ad_group_id
LEFT JOIN impressions_clicks ic2 ON cld.event_date = ic2.date --this join grabs campaigns without an ad_group_ref
  AND cld.campaign_name = ic2.campaign_name
  AND split_part(replace(cld.source, ' ', ''), '.COM', 1) = split_part(replace(ic2.source, ' ', ''), '.COM', 1) --remove any spaces or .COM in source
  AND ic1.campaign_name IS NULL --ignore campaigns already picked up in join above (shouldn't need this with conditions below, but feels like a good thing just in case)
  AND (ic2.ad_group_id IS NULL OR ic2.ad_group_id = 0) --ignore ad group-level campaigns that should have been picked up above (0 is a placeholder for some BLMA campaigns, so we don't want to exclude those)
  AND cld.ad_group_ref IS NULL --ignore ad group-level campaigns that weren't picked up in the join above because there wasn't any spend
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
LIMIT {QUERY_LIMIT} --for chartio data store
;