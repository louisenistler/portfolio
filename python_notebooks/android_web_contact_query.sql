with ss_card_reorder_test_groups as (
select
    du.user_ref,
    mode,
    'CONTROL' as test_group
from public.louise_self_service_card_reorder_control c
join curated.dim_user du on c.user_ref = du.user_ref
where num_open_accounts > 0

union

select
    du.user_ref,
    'ON' as mode,
    test_group
from public.louise_self_service_card_reorder_test p
join curated.dim_user du on p.user_ref = du.user_ref
where num_open_accounts > 0
    ),

ios_events as (
select distinct s.user_ref
from ss_card_reorder_test_groups s
join fact_client_event_pii fce on s.user_ref = fce.user_ref
where event_date >= '2020-01-01'
    and client = 'IOS'
    )

select
    s.user_ref,
    s.test_group,
    count(distinct case_number) as num_cases,
    count(distinct case when origin = 'Phone' then case_number end) as num_calls,
    count(distinct case when origin = 'Phone' and contact_reason = 'New Card' then case_number end) as phone_new_card,
    count(distinct case when origin = 'Chat' then case_number end) as num_chats,
    count(distinct case when origin = 'Support Message' then id end) as num_messages,
    count(distinct case when origin = 'Chat' and contact_reason = 'New Card' then case_number end) as chat_new_card,
    count(distinct case when origin = 'Support Message' and contact_reason = 'New Card' then id end) as messages_new_card
from ss_card_reorder_test_groups s
left join (
    select
        user_ref,
        sfc.id,
        sfc.case_number,
        sfc.origin,
        sfc.created_date::date,
        sft.topic_options_c as contact_reason
    from segment_salesforce.cases sfc
    join ss_card_reorder_test_groups s on sfc.new_customer_uuid_c = s.user_ref
    left join segment_salesforce.topics sft on sfc.id = sft.case_c
    where sfc.created_date::date >= '2020-12-10' and sfc.created_date::date < '2020-12-23' --Amount of time back from being able to answer chats
        and origin in ('Phone','Support Message','Chat')
        and record_type_lookup_c = 'CRCS'
    ) c on c.user_ref = s.user_ref
left join ios_events ios on s.user_ref = ios.user_ref
where ios.user_ref is null
group by 1,2
;