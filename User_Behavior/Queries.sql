/* Balance table */
with days30 as (select balances.user_id,
       round(avg(balances.current_balance)) as days30_avg_balance
from analytics_bi.balances_mex as balances
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = balances.user_id
where (balances.balance_date BETWEEN user_info.first_deposit AND dateadd(day,30,user_info.first_deposit)) AND balance_type is not null
group by balances.user_id),
days60 as (select balances.user_id,
       round(avg(balances.current_balance)) as days60_avg_balance
from analytics_bi.balances_mex as balances
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = balances.user_id
where (balances.balance_date BETWEEN dateadd(day,30,user_info.first_deposit) AND dateadd(day,60,user_info.first_deposit)) AND balance_type is not null
group by balances.user_id),
days90 as (select balances.user_id,
       round(avg(balances.current_balance)) as days90_avg_balance
from analytics_bi.balances_mex as balances
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = balances.user_id
where (balances.balance_date BETWEEN dateadd(day,60,user_info.first_deposit) AND dateadd(day,90,user_info.first_deposit)) AND balance_type is not null
group by balances.user_id)
select coalesce(days30.user_id,days60.user_id,days90.user_id) as user_id,
       days30.days30_avg_balance,
       days60.days60_avg_balance,
       days90.days90_avg_balance
from days30
full outer join  days60 on days30.user_id = days60.user_id
full outer join  days90 on days30.user_id = days90.user_id;
/* Transfer/deposit table */
with days30_cash as (select transactions.user_id,
       count(distinct (transactions.transaction_id)) as days30_count_trans
from analytics_bi.transactions as transactions
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = transactions.user_id
where transactions.timestamp_mx_created_at BETWEEN user_info.first_deposit AND dateadd(day,30,user_info.first_deposit) AND (type like 'TRANSFER' OR type like 'DEPOSIT')
group by transactions.user_id),
days60_cash as (select transactions.user_id,
       count(distinct (transactions.transaction_id)) as days60_count_trans
from analytics_bi.transactions as transactions
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = transactions.user_id
where transactions.timestamp_mx_created_at BETWEEN dateadd(day,30,user_info.first_deposit) AND dateadd(day,60,user_info.first_deposit) AND (type like 'TRANSFER' OR type like 'DEPOSIT')
group by transactions.user_id),
days90_cash as (select transactions.user_id,
       count(distinct (transactions.transaction_id)) as days90_count_trans
from analytics_bi.transactions as transactions
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = transactions.user_id
where transactions.timestamp_mx_created_at BETWEEN dateadd(day,60,user_info.first_deposit) AND dateadd(day,90,user_info.first_deposit) AND (type like 'TRANSFER' OR type like 'DEPOSIT')
group by transactions.user_id)
select coalesce(days30_cash.user_id, days60_cash.user_id,days90_cash.user_id) as user_id,
       days30_cash.days30_count_trans,
       days60_cash.days60_count_trans,
       days90_cash.days90_count_trans
from days30_cash
full outer join  days60_cash on days30_cash.user_id = days60_cash.user_id
full outer join days90_cash on days30_cash.user_id = days90_cash.user_id;
/* Rewards table */
with days30_rewards as (select transactions.user_id,
       count(distinct (transactions.transaction_id)) as days30_count_reward
from analytics_bi.transactions as transactions
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = transactions.user_id
where transactions.timestamp_mx_created_at BETWEEN user_info.first_deposit AND dateadd(day,30,user_info.first_deposit) AND type like 'REWARD'
group by transactions.user_id),
days60_rewards as (select transactions.user_id,
       count(distinct (transactions.transaction_id)) as days60_count_reward
from analytics_bi.transactions as transactions
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = transactions.user_id
where transactions.timestamp_mx_created_at BETWEEN dateadd(day,30,user_info.first_deposit) AND dateadd(day,60,user_info.first_deposit) AND type like 'REWARD'
group by transactions.user_id),
days90_rewards as (select transactions.user_id,
       count(distinct (transactions.transaction_id)) as days90_count_reward
from analytics_bi.transactions as transactions
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = transactions.user_id
where transactions.timestamp_mx_created_at BETWEEN dateadd(day,60,user_info.first_deposit) AND dateadd(day,90,user_info.first_deposit) AND type like 'REWARD'
group by transactions.user_id)
select coalesce(days30_rewards.user_id,days60_rewards.user_id, days90_rewards.user_id) as user_id,
       days30_rewards.days30_count_reward,
       days60_rewards.days60_count_reward,
       days90_rewards.days90_count_reward
from days30_rewards
full outer join  days60_rewards on days30_rewards.user_id = days60_rewards.user_id
full outer join  days90_rewards on days30_rewards.user_id = days90_rewards.user_id;
/* Purchase vol and count table */
with days30 as (select vol_purch.user_id,
                       sum(amount) as days30_vol_purch,
                       count(amount) as days30_count_purch
from analytics_bi.transactions as vol_purch
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = vol_purch.user_id
where (vol_purch.timestamp_mx_created_at BETWEEN user_info.first_deposit AND dateadd(day,30,user_info.first_deposit)) AND type like 'PURCHASE'
group by vol_purch.user_id),
days60 as (select vol_purch.user_id,
                  sum(amount) as days60_vol_purch,
                  count(amount) as days60_count_purch
from analytics_bi.transactions as vol_purch
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = vol_purch.user_id
where (vol_purch.timestamp_mx_created_at BETWEEN dateadd(day,30,user_info.first_deposit) AND dateadd(day,60,user_info.first_deposit)) AND type like 'PURCHASE'
group by vol_purch.user_id),
days90 as (select vol_purch.user_id,
                  sum(amount) as days90_vol_purch,
                  count(amount) as days90_count_purch
from analytics_bi.transactions as vol_purch
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = vol_purch.user_id
where (vol_purch.timestamp_mx_created_at BETWEEN dateadd(day,60,user_info.first_deposit) AND dateadd(day,90,user_info.first_deposit)) AND type like 'PURCHASE'
group by vol_purch.user_id)
select days30.user_id,
       days30.days30_vol_purch,
       days30.days30_count_purch,
       days60.days60_vol_purch,
       days60.days60_count_purch,
       days90.days90_vol_purch,
       days90.days90_count_purch
from days30
left join  days60 on days30.user_id = days60.user_id
left join  days90 on days30.user_id = days90.user_id;
/* Credit table for Klayuda*/
with offers_credit_30days as (
    select credit.user_id,
        count(user_status) as days30_num_offers
from public.db_loans_credit_batch_import_renewal_union as credit
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit.user_id
where to_date(credit.start_batch,'YYYY-MM-DD', FALSE) BETWEEN user_info.first_deposit AND dateadd(day,30,user_info.first_deposit)
group by credit.user_id),
accepted_credit_30days as (
    select credit.user_id,
        count(user_status) as days30_accepted,
        sum(amount_max) as amount30_days
from public.db_loans_credit_batch_import_renewal_union as credit
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit.user_id
where to_date(credit.acceptance_date,'YYYY-MM-DD', FALSE) BETWEEN user_info.first_deposit AND dateadd(day,30,user_info.first_deposit)
group by credit.user_id),
offered_credit_60days as (
    select credit.user_id,
        count(user_status) as days60_num_offers
from public.db_loans_credit_batch_import_renewal_union as credit
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit.user_id
where to_date(credit.start_batch,'YYYY-MM-DD', FALSE) BETWEEN dateadd(day,30,user_info.first_deposit) AND dateadd(day,60,user_info.first_deposit)
group by credit.user_id),
accepted_credit_60days as (
    select credit.user_id,
        count(user_status) as days60_accepted,
        sum(amount_max) as amount60_days
from public.db_loans_credit_batch_import_renewal_union as credit
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit.user_id
where to_date(credit.acceptance_date,'YYYY-MM-DD', FALSE) BETWEEN dateadd(day,30,user_info.first_deposit) AND dateadd(day,60,user_info.first_deposit)
group by credit.user_id),
offered_credit_90days as (
    select credit.user_id,
        count(user_status) as days90_num_offers
from public.db_loans_credit_batch_import_renewal_union as credit
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit.user_id
where to_date(credit.start_batch,'YYYY-MM-DD', FALSE) BETWEEN dateadd(day,60,user_info.first_deposit) AND dateadd(day,90,user_info.first_deposit)
group by credit.user_id),
accepted_credit_90days as (
    select credit.user_id,
        count(user_status) as days90_accepted,
        sum(amount_max) as amount90_days
from public.db_loans_credit_batch_import_renewal_union as credit
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit.user_id
where to_date(credit.acceptance_date,'YYYY-MM-DD', FALSE) BETWEEN dateadd(day,60,user_info.first_deposit) AND dateadd(day,90,user_info.first_deposit)
group by credit.user_id)
select coalesce(offers_credit_30days.user_id,offered_credit_60days.user_id, offered_credit_90days.user_id) as user_id,
       offers_credit_30days.days30_num_offers,
       accepted_credit_30days.days30_accepted,
       accepted_credit_30days.amount30_days,
       offered_credit_60days.days60_num_offers,
       accepted_credit_60days.days60_accepted,
       accepted_credit_60days.amount60_days,
       offered_credit_90days.days90_num_offers,
       accepted_credit_90days.days90_accepted,
       accepted_credit_90days.amount90_days
from offers_credit_30days
full outer join accepted_credit_30days on offers_credit_30days.user_id = accepted_credit_30days.user_id
full outer join  offered_credit_60days on offers_credit_30days.user_id = offered_credit_60days.user_id
full outer join accepted_credit_60days on offered_credit_60days.user_id = accepted_credit_60days.user_id
full outer join  offered_credit_90days on offered_credit_60days.user_id = offered_credit_90days.user_id
full outer join accepted_credit_90days on offered_credit_90days.user_id = accepted_credit_90days.user_id;
/* Credit table for CrediKlar*/
with offers_credit_30days as (
    select credit_user.user_id,
        count(credit.id) as days30_num_offers
from analytics_history.loanofferrequestevent__loan_offers as credit
join analytics_history.loanofferrequestevent as credit_user
on credit_user.__id = credit.__id
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit_user.user_id
where convert_timezone('UTC','1970-01-01 00:00:00'::timestamp without time zone + credit.created * 1000::double precision * '00:00:00.001'::interval)::date BETWEEN user_info.first_deposit AND dateadd(day,30,user_info.first_deposit)
group by credit_user.user_id),
accepted_credit_30days as (
    select credit.user_id,
        count(loan_id) as days30_accepted,
           sum(loan_amount) as amount30_days
from credit.loanbook as credit
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit.user_id
where to_date(credit.disbursement_date,'YYYY-MM-DD', FALSE) BETWEEN user_info.first_deposit AND dateadd(day,30,user_info.first_deposit)
group by credit.user_id),
offered_credit_60days as (
    select credit_user.user_id,
        count(credit.id) as days60_num_offers
from analytics_history.loanofferrequestevent__loan_offers as credit
join analytics_history.loanofferrequestevent as credit_user
on credit_user.__id = credit.__id
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit_user.user_id
where convert_timezone('UTC','1970-01-01 00:00:00'::timestamp without time zone + credit.created * 1000::double precision * '00:00:00.001'::interval)::date BETWEEN dateadd(day,30,user_info.first_deposit) AND dateadd(day,60,user_info.first_deposit)
group by credit_user.user_id),
accepted_credit_60days as (
    select credit.user_id,
        count(loan_id) as days60_accepted,
           sum(loan_amount) as amount60_days
from credit.loanbook as credit
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit.user_id
where to_date(credit.disbursement_date,'YYYY-MM-DD', FALSE) BETWEEN dateadd(day,30,user_info.first_deposit) AND dateadd(day,60,user_info.first_deposit)
group by credit.user_id),
offered_credit_90days as (
    select credit_user.user_id,
        count(credit.id) as days90_num_offers
from analytics_history.loanofferrequestevent__loan_offers as credit
join analytics_history.loanofferrequestevent as credit_user
on credit_user.__id = credit.__id
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit_user.user_id
where convert_timezone('UTC','1970-01-01 00:00:00'::timestamp without time zone + credit.created * 1000::double precision * '00:00:00.001'::interval)::date BETWEEN dateadd(day,60,user_info.first_deposit) AND dateadd(day,90,user_info.first_deposit)
group by credit_user.user_id),
accepted_credit_90days as (
    select credit.user_id,
        count(loan_id) as days90_accepted,
           sum(loan_amount) as amount90_days
from credit.loanbook as credit
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = credit.user_id
where to_date(credit.disbursement_date,'YYYY-MM-DD', FALSE) BETWEEN dateadd(day,60,user_info.first_deposit) AND dateadd(day,90,user_info.first_deposit)
group by credit.user_id)
select coalesce(offers_credit_30days.user_id,offered_credit_60days.user_id, offered_credit_90days.user_id) as user_id,
       offers_credit_30days.days30_num_offers,
       accepted_credit_30days.days30_accepted,
       accepted_credit_30days.amount30_days,
       offered_credit_60days.days60_num_offers,
       accepted_credit_60days.days60_accepted,
       accepted_credit_60days.amount60_days,
       offered_credit_90days.days90_num_offers,
       accepted_credit_90days.days90_accepted,
       accepted_credit_90days.amount90_days
from offers_credit_30days
full outer join accepted_credit_30days on offers_credit_30days.user_id = accepted_credit_30days.user_id
full outer join  offered_credit_60days on offers_credit_30days.user_id = offered_credit_60days.user_id
full outer join accepted_credit_60days on offered_credit_60days.user_id = accepted_credit_60days.user_id
full outer join  offered_credit_90days on offered_credit_60days.user_id = offered_credit_90days.user_id
full outer join accepted_credit_90days on offered_credit_90days.user_id = accepted_credit_90days.user_id;
/* User info table */
select email,
       user_id,
       account_created as cuenta,
       first_deposit as fta,
       first_purchase as P1,
       tenth_purchase as P10
from klar.dim_funnel_v2
where cuenta is not null;
/* Channel table */
select adjust.klaruserid,
adjust.network_name,
adjust.event_name
from public.adjust
where convert_timezone('UTC','1970-01-01 00:00:00'::timestamp without time zone + adjust.created_at_milli::double precision * '00:00:00.001'::interval)::date  between '2021-10-01'::date AND '2022-01-01'::date AND
network_name is not null and klaruserid is not null;
/* Referral Table*/
WITH referrees AS (      
        SELECT  uk.email AS referree_email,
        uk.id AS referree_user_id,
        uk.first_name AS referree_first_name,
        uk.second_name AS referree_second_name,
        uk.first_surname AS referree_first_surname,
        uk.second_surname AS referree_second_surname,
        code AS referral_code,
        CONVERT_TIMEZONE('UTC','America/Mexico_City', uks.signed_up) AS completed_signup_date           
        FROM    db_kyc_public_used_referral_codes urc 
        JOIN    db_kyc_public_user_kyc uk on urc.user_id = uk.id
        JOIN    db_kyc_public_user_kyc_status uks ON urc.user_id = uks.user_id                        
),
referrers AS (          
        SELECT  r.referree_user_id,
                uk.id AS referrer_user_id,
                uk.email AS referrer_email,
                uk.first_name AS referrer_first_name,
                uk.second_name AS referrer_second_name,
                uk.first_surname AS referrer_first_surname,
                uk.second_surname AS referrer_second_surname                     
        FROM    referrees r 
        JOIN    db_kyc_public_user_kyc uk on r.referral_code = uk.referral_code
)
        SELECT  rr.referrer_email,
                rr.referrer_user_id,
                re.referree_email AS referree_email,
                re.referree_user_id,
                re.completed_signup_date AS referree_completed_signup_date
        FROM    referrers rr 
        LEFT JOIN  referrees re ON rr.referree_user_id = re.referree_user_id
        AND    referral_code NOT IN ('QTLDM0QY','MDRGQTAX','OTJENZKY','QTA2MJQ5','OUY0OUIZ','QJVDRJA4','OTQXOTKZ','QZK1MDI1','OEMZMJHE',
        'RKE0MTQZ','MTUYNJCW','OTNDNDY4','NDK2NZUZ','QZQ1MDC2','MEMZMZJE','NTMXN0Y4','OUJFMJZD','NKVFRKUZ','MDKXOTKW','QUJCOTIW','N0E3NDEW',
        'RDFGOEI2','MZU2NTMZ','ODC0NTC2','RTFGNUUY','RKNEMJQ2','RJC1RKFE','QUUYN0RC','QUJFQUFC','NUVGRKFD','NJY2NEYW','MKNBQZKZ','MUFDOTKY','M0RBN0M2','OEFFNEE2');
/* Deliquent credit users for Klayuda */
select distinct user_id,
       user_status as deliquent
from credit_ops.credit_user
WHERE user_status like 'Delinquent';
/* Deliquent credit users for CrediKlar*/
select distinct user_id,
                user_status as deliquent
from credit.credit_user
WHERE user_status like 'DELINQUENT';
/* Retention */
with retention_30days as (
    select t.user_id,
       CASE
            WHEN count(type) > 0 then 1
            else 0
        end as retention
from analytics_bi.transactions as t
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = t.user_id
where (t.timestamp_mx_created_at BETWEEN user_info.first_deposit AND dateadd(day,30,user_info.first_deposit)) AND (type like 'DISBURSEMENT' OR type like 'FEE' OR type like 'PURCHASE' OR type like 'TRANSFER' OR type like 'DEPOSIT')
group by t.user_id),
retention_60days as (
    select t.user_id,
       CASE
            WHEN count(type) > 0 then 1
            else 0
        end as retention
from analytics_bi.transactions as t
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = t.user_id
where (t.timestamp_mx_created_at BETWEEN dateadd(day,30,user_info.first_deposit) AND dateadd(day,60,user_info.first_deposit)) AND (type like 'DISBURSEMENT' OR type like 'FEE' OR type like 'PURCHASE' OR type like 'TRANSFER' OR type like 'DEPOSIT')
group by t.user_id),
retention_90days as (
    select t.user_id,
       CASE
            WHEN count(type) > 0 then 1
            else 0
        end as retention
from analytics_bi.transactions as t
left join klar.dim_funnel_v2 as user_info
on user_info.user_id = t.user_id
where (t.timestamp_mx_created_at BETWEEN dateadd(day,60,user_info.first_deposit) AND dateadd(day,90,user_info.first_deposit)) AND (type like 'DISBURSEMENT' OR type like 'FEE' OR type like 'PURCHASE' OR type like 'TRANSFER' OR type like 'DEPOSIT')
group by t.user_id)
select coalesce(retention_30days.user_id,retention_60days.user_id, retention_90days.user_id) as user_id,
       retention_30days.retention as retention_30_days,
       retention_60days.retention as retention_60_days,
       retention_90days.retention as retention_90_days
from retention_30days
full outer join  retention_60days on retention_30days.user_id = retention_60days.user_id
full outer join  retention_90days on retention_30days.user_id = retention_90days.user_id