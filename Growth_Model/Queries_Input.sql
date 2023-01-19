/* Started siqnup query */
select
distinct (funnel.user_id),
funnel.started_web_signup as signup
from klar.dim_funnel_v2 as funnel
where signup > '2021-07-01'::date;
/* Sms query */
select
distinct (funnel.user_id),
funnel.sms_confirmed as sms
from klar.dim_funnel_v2 as funnel
where sms > '2021-07-01'::date;
/* Adjust query */
select adjust.klaruserid as user_id,
           split_part(replace(replace(replace(replace(replace(replace(replace(replace(tracker_name, '5fow643x', 'ZoomD'),'ZoomD_42b-ADN', 'ZoomD'), 'Unattributed','Facebook'), 'Google Ads ACE', 'Google'),'Google Ads ACI', 'Google'), 'Google Ads (unknown)', 'Google'),'Google Ads Search', 'Google'), 'UA48736', 'ZoomD'), '::', 1) as channel,
           split_part(replace(replace(replace(replace(replace(replace(replace(replace(last_tracker_name, '5fow643x', 'ZoomD'),'ZoomD_42b-ADN', 'ZoomD'), 'Unattributed','Facebook'), 'Google Ads ACE', 'Google'),'Google Ads ACI', 'Google'), 'Google Ads (unknown)', 'Google'),'Google Ads Search', 'Google'), 'UA48736', 'ZoomD'), '::', 1) as past_channel
from public.adjust as adjust
where klaruserid notnull and channel notnull;
/* Funnel */
select
user_id,
started_web_signup,
completed_web_signup,
account_created,
sms_confirmed,
first_deposit as fta,
first_purchase,
tenth_purchase
from klar.dim_funnel_v2
where account_created notnull;
/* Referral */
select *
from growth.referral_info;
/* Referral Cohorts */
with referre_info as (
select
    referral.referrer_user_id as referrer,
    referral.referree_user_id as referree,
    funnel.completed_web_signup as referee_completed_signup
from growth.referral_info as referral
join klar.dim_funnel_v2 as funnel
on referral.referree_user_id = funnel.user_id
order by 1 desc)
select
    referre_info.referrer,
    referre_info.referree,
    referre_info.referee_completed_signup,
    funnel.sms_confirmed as referrer_sms_confirmed,
    date_trunc('month',referre_info.referee_completed_signup) as month_cohort,
    datediff(day,referrer_sms_confirmed,referre_info.referee_completed_signup) as days
from referre_info
join klar.dim_funnel_v2 as funnel
on referre_info.referrer = funnel.user_id
order by 1 desc
