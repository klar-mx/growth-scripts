/* Funnel section - Tracker Overall */
with start_signups as (
    with not_completed_signup as (
        select date_trunc('day', signup_created_timestamp) as day_notC,
               count(email)                                as not_c_signup
        from klar.signup_not_completed
        group by day_notC
    ),
         started_signup as (
             select date_trunc('day', started_web_signup_mx) as day_su,
                    count(user_id)                        as started_signup
             from klar.cck_funnel
             group by day_su
         )
    select day_notC                      as day,
           not_c_signup + started_signup as signups_started
    from not_completed_signup
             left join started_signup on not_completed_signup.day_notC = started_signup.day_su
    order by day
),
     completed_signups as (
          select date_trunc('day', ended_web_signup_mx) as day,
                    count(user_id) as signups_completed
             from klar.cck_funnel
          where ended_web_signup_mx notnull
          group by day
),
     sms_confirmed as (
          select date_trunc('day', sms_confirmed_mx) as day,
                    count(user_id) as sms_confirmed
             from klar.cck_funnel
          where sms_confirmed_mx notnull
          group by day
),
     cck_offers as (
          select date_trunc('day', first_cck_line_timestamp_mx) as day,
                    count(user_id) as cck_offers
             from klar.cck_funnel
          where first_cck_line_timestamp_mx notnull
          and first_cck_line_timestamp_mx >'2022-10-03'
          and (saca_risk_band_timestamp_mx notnull or migrated_user = 1)
          group by day
),
     cck_toggles as (
          select date_trunc('day', first_cck_toggle_on_timestamp_mx) as day,
                    count(user_id) as cck_toggles
             from klar.cck_funnel
          where first_cck_toggle_on_timestamp_mx notnull
          and first_cck_toggle_on_timestamp_mx >'2022-10-03'
          group by day
),
     ftas as (
          select date_trunc('day', least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx)) as day,
                    count(user_id) as ftas
             from klar.cck_funnel
          group by day
),
     first_purchase as (
          select date_trunc('day', first_purchase_timestamp_mx) as day,
                    count(user_id) as first_purchase
             from klar.cck_funnel
          where first_purchase_timestamp_mx notnull
          group by day
)
select
       start_signups.day,
       coalesce(start_signups.signups_started,0) as started_signups,
       coalesce(completed_signups.signups_completed,0) as completed_signups,
       coalesce(sms_confirmed.sms_confirmed,0) as sms_confirmed,
       coalesce(cck_offers.cck_offers,0) as cck_offers,
       coalesce(cck_toggles.cck_toggles,0) as cck_toggles,
       coalesce(ftas.ftas,0) as ftas,
       coalesce(first_purchase.first_purchase,0) as first_purchases
from start_signups
left join completed_signups on completed_signups.day = start_signups.day
left join sms_confirmed on sms_confirmed.day = start_signups.day
left join cck_offers on cck_offers.day = start_signups.day
left join cck_toggles on cck_toggles.day = start_signups.day
left join ftas on ftas.day = start_signups.day
left join first_purchase on first_purchase.day = start_signups.day
where start_signups.day between '2022-10-03' and dateadd(days,-1,current_date)
order by start_signups.day;
/* Funnel section - Tracker CCK */
with sms_confirmed as (
          select date_trunc('day', sms_confirmed_mx) as day,
                    count(user_id) as sms_confirmed
             from klar.cck_funnel
          where sms_confirmed_mx notnull
          group by day
),
     cck_offers as (
          select date_trunc('day', first_cck_line_timestamp_mx) as day,
                    count(user_id) as cck_offers
             from klar.cck_funnel
          where first_cck_line_timestamp_mx notnull
          and first_cck_line_timestamp_mx >'2022-10-03'
          and (saca_risk_band_timestamp_mx notnull or migrated_user = 1)
          group by day
),
     cck_new_users as (
          select date_trunc('day', first_cck_line_timestamp_mx) as day,
                    count(user_id) as new_users
             from klar.cck_funnel
          where first_cck_line_timestamp_mx notnull
          and first_cck_line_timestamp_mx >'2022-10-03'
          and saca_risk_band_timestamp_mx notnull
          and migrated_user = 0
          group by day
),
     cck_migrated as (
          select date_trunc('day', first_cck_line_timestamp_mx) as day,
                    count(user_id) as migrated_users
             from klar.cck_funnel
          where first_cck_line_timestamp_mx notnull
          and first_cck_line_timestamp_mx >'2022-10-03'
          and migrated_user = 1
          group by day
),
     cck_recovery_path as (
          select date_trunc('day', first_cck_line_timestamp_mx) as day,
                    0 as recovered_users
          from klar.cck_funnel
          group by day
),
     cck_toggles as (
          select date_trunc('day', first_cck_toggle_on_timestamp_mx) as day,
                    count(user_id) as cck_toggles
             from klar.cck_funnel
          where first_cck_toggle_on_timestamp_mx notnull
          and first_cck_toggle_on_timestamp_mx >'2022-10-03'
          group by day
),
     first_cashloan as (
          select date_trunc('day', first_cash_installment_loan_timestamp_mx) as day,
                    count(user_id) as cashloans
             from klar.cck_funnel
          where first_cash_installment_loan_timestamp_mx notnull
          and first_cash_installment_loan_timestamp_mx >'2022-10-03'
          group by day
),
     ftas as (
          select date_trunc('day', least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx)) as day,
                    count(user_id) as credit_ftas
             from klar.cck_funnel
          where first_cck_line_timestamp_mx notnull
          group by day
),
     first_purchase as (
          select
             date_trunc('day', credit_first_purchase_timestamp_mx) as day,
              count(user_id) as first_credit_purchase
          from klar.cck_funnel
          where credit_first_purchase_timestamp_mx notnull
            and datediff(minute,first_purchase_timestamp_mx,credit_first_purchase_timestamp_mx) < 2
          group by day
          order by day desc
)
select
       sms_confirmed.day,
       coalesce(sms_confirmed.sms_confirmed,0) as sms_confirmed,
       coalesce(cck_offers.cck_offers,0) as cck_offers,
       coalesce(cck_new_users.new_users,0) as new_users,
       coalesce(cck_migrated.migrated_users,0) as migrated_users,
       coalesce(cck_recovery_path.recovered_users,0) as recovered_users,
       coalesce(cck_toggles.cck_toggles,0) as cck_toggles,
       coalesce(first_cashloan.cashloans,0) as cashloans,
       coalesce(ftas.credit_ftas,0) as credit_ftas,
       coalesce(first_purchase.first_credit_purchase,0) as first_credit_purchase
from sms_confirmed
left join cck_offers on cck_offers.day = sms_confirmed.day
left join cck_new_users on cck_new_users.day = sms_confirmed.day
left join cck_migrated on cck_migrated.day = sms_confirmed.day
left join cck_recovery_path on cck_recovery_path.day = sms_confirmed.day
left join cck_toggles on cck_toggles.day = sms_confirmed.day
left join first_cashloan on first_cashloan.day = sms_confirmed.day
left join ftas on ftas.day = sms_confirmed.day
left join first_purchase on first_purchase.day = sms_confirmed.day
where sms_confirmed.day between '2022-10-03' and dateadd(days,-1,current_date)
order by sms_confirmed.day;
/* Funnel section - Tracker Debit Path */
with sms_confirmed as (
          select date_trunc('day', sms_confirmed_mx) as day,
                    count(user_id) as sms_confirmed
             from klar.cck_funnel
          where sms_confirmed_mx notnull
          group by day
),
     ftas as (
          select date_trunc('day', first_deposit_mx) as day,
                    count(user_id) as debit_ftas
             from klar.cck_funnel
          where first_deposit_mx notnull
          and first_cck_line_timestamp_mx is null
          group by day
),
     first_purchase as (
          select date_trunc('day', first_purchase_timestamp_mx) as day,
               count(user_id)                                 as first_debit_purchase
        from klar.cck_funnel
        where ((first_purchase_timestamp_mx notnull and credit_first_purchase_timestamp_mx is null)
           or (datediff(minute,first_purchase_timestamp_mx,credit_first_purchase_timestamp_mx) >= 2)
              )
       group by day
       order by day desc
)
select
       sms_confirmed.day,
       coalesce(sms_confirmed.sms_confirmed,0) as sms_confirmed,
       coalesce(ftas.debit_ftas,0) as credit_ftas,
       coalesce(first_purchase.first_debit_purchase,0) as first_credit_purchase
from sms_confirmed
left join ftas on ftas.day = sms_confirmed.day
left join first_purchase on first_purchase.day = sms_confirmed.day
where sms_confirmed.day between '2022-10-03' and dateadd(days,-1,date_trunc('day', current_date))
order by sms_confirmed.day;
/* SMS query */
select
distinct (funnel.user_id),
funnel.sms_confirmed_mx as sms_confirmed
from klar.cck_funnel as funnel
where sms_confirmed > '2022-10-03';
/* FTA query */
select
distinct (funnel.user_id),
least(credit_first_purchase_timestamp_mx, first_deposit_mx, first_cash_installment_loan_timestamp_mx) as fta
from klar.cck_funnel as funnel
where fta > '2022-10-03';
/* FTA Debit query */
select
distinct (funnel.user_id),
first_deposit_mx as debit_fta
from klar.cck_funnel as funnel
where debit_fta > '2022-10-03'
and first_cck_line_timestamp_mx is null;
/* 1P query */
select
distinct (funnel.user_id),
funnel.first_purchase_timestamp_mx as first_purchase
from klar.cck_funnel as funnel
where first_purchase > '2022-10-03';
/* CCK Offers */
select
distinct (funnel.user_id),
funnel.first_cck_line_timestamp_mx as cck_offers
from klar.cck_funnel as funnel
where first_cck_line_timestamp_mx >'2022-10-03'
and saca_risk_band_timestamp_mx notnull
and first_cck_line_timestamp_mx notnull
and migrated_user = 0;
/* Credit First Purchase */
select
distinct (funnel.user_id),
funnel.credit_first_purchase_timestamp_mx as credit_first_p
from klar.cck_funnel as funnel
where first_cck_line_timestamp_mx >'2022-10-03';
/* Adjust query */
select adjust.klaruserid as user_id,
           split_part(replace(replace(replace(replace(replace(replace(replace(replace(tracker_name, '5fow643x', 'ZoomD'),'ZoomD_42b-ADN', 'ZoomD'), 'Unattributed','Facebook'), 'Google Ads ACE', 'Google'),'Google Ads ACI', 'Google'), 'Google Ads (unknown)', 'Google'),'Google Ads Search', 'Google'), 'UA48736', 'ZoomD'), '::', 1) as channel,
           split_part(replace(replace(replace(replace(replace(replace(replace(replace(last_tracker_name, '5fow643x', 'ZoomD'),'ZoomD_42b-ADN', 'ZoomD'), 'Unattributed','Facebook'), 'Google Ads ACE', 'Google'),'Google Ads ACI', 'Google'), 'Google Ads (unknown)', 'Google'),'Google Ads Search', 'Google'), 'UA48736', 'ZoomD'), '::', 1) as past_channel
from public.adjust as adjust
where klaruserid notnull and channel notnull;
/* Referral */
select *
from growth.referral_info