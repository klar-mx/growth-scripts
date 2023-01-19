/* Channel table */
select distinct adjust.klaruserid,
adjust.network_name,
adjust.event_name
from public.adjust
where network_name is not null and klaruserid is not null;
/* Referral Table*/
select *
from growth.referral_info;
/* Funnel Query */
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
where user_id notnull;
/* Overdraft */
with loan_table as (
    select o.user_id,
           o.free_limit_started,
           o.started_mx,
           o.ended_mx,
           o.available_balance_started
    from ds_overdraft.overdraft o
             join (select user_id,
                          started_mx::date as started_date_mx,
                          max(started_mx)  as started_mx
                   from ds_overdraft.overdraft
                   group by user_id, started_date_mx) lo
                  on o.user_id = lo.user_id and o.started_mx = lo.started_mx),

     loan_table_v2 as (
         select dl.user_id,
                dl.available_balance_started,
                left(started_mx, 23)                                                as started_mx,
                left(ended_mx, 23)                                                  as ended_mx,
                sum(coalesce(amount, 0))                                            as incoming,
                max(timestamp_mx_created_at)                                           last_incoming_date,
                datediff(days, greatest(started_mx::date, last_incoming_date::date),
                         coalesce(ended_mx::date, convert_timezone('America/Mexico_City'::character varying::text,
                                                                   getdate()))) + 1 as days_diff,
                sum(amount)                                                         as payment_amount_,
                case
                    when payment_amount_ > dl.available_balance_started * -1
                        then dl.available_balance_started * -1
                    when ended_mx is not null then dl.available_balance_started * -1
                    else payment_amount_ end                                        as payment_amount
         from loan_table dl
                  left join (select *
                             from analytics_bi.transactions
                             where data_science_description in ('CLABE_INCOMING', 'DEPOSIT')
                               and state = 'SETTLED') tx
                            on dl.user_id = tx.user_id and
                               tx.timestamp_mx_created_at >= dl.started_mx and
                               tx.timestamp_mx_created_at <=
                               least(dl.ended_mx::date, convert_timezone('America/Mexico_City'::character varying::text,
                                                                         getdate()))
         group by dl.user_id, dl.started_mx, dl.ended_mx, dl.available_balance_started
     ),

     first_offers as (SELECT h.user_id,
                             convert_timezone('America/Mexico_City'::character varying::text,
                                              h.operation_timestamp)                              AS set_current_limit_operation_mx,
                             json_extract_path_text(h.offer::text, 'freeLimit'::character varying::text,
                                                    'units'::character varying::text)             AS free_limit,
                             convert_timezone('America/Mexico_City'::text,
                                              mtim.first_accepted_offer)                          AS first_accepted_offer_mx,
                             json_extract_path_text(h.offer::text, 'id'::character varying::text) AS offer_id,
                             rank()
                             OVER (
                                 PARTITION BY h.user_id
                                 ORDER BY set_current_limit_operation_mx asc)                     AS r
                      FROM overdraft_current_limits_history h
                               JOIN (SELECT foa.user_id, min(foa.operation_timestamp) AS first_accepted_offer
                                     FROM overdraft_current_limits_history foa
                                     GROUP BY foa.user_id) mtim
                                    ON h.user_id::text = mtim.user_id::text and
                                       h.operation::text <> 'D'::character varying::text),
     offers_table as (
         select *
         from first_offers
         where r = 1),

     od_loan_book as (
         select
             --   concat(oft.user_id, o.started_mx) as od_loan_id,
             ROW_NUMBER() OVER (ORDER BY o.started_mx asc)                    as od_loan_id,
             oft.user_id,
             oft.offer_id,
             oft.first_accepted_offer_mx,
             free_limit,
             o.started_mx                                                     as disbursement_date,
             o.ended_mx                                                       as closed_date,
             left(dateadd(days, 30, o.started_mx::date), 10)                  as maturity_date,
             case
                 when last_incoming_date::date is null then left(closed_date::date, 10)
                 when last_incoming_date::date < closed_date::date then left(closed_date, 10)
                 else left(last_incoming_date, 10) end                        as last_payment_date,
             (days_diff - 30)                                                 as days_past_due,
             o.available_balance_started                                      as overdraft_loan_amount,
             payment_amount,
             case
                 when ended_mx is not null then 0
                 when overdraft_loan_amount * -1 > payment_amount then overdraft_loan_amount * -1 - payment_amount
                 when last_payment_date is null and payment_amount is null then overdraft_loan_amount*-1
                 else overdraft_loan_amount*-1 - payment_amount end                                      as balance,
             rank() over (partition by oft.user_id order by o.started_mx asc) as Sequence,
             case
                 when ended_mx is null and days_diff <= 30 then 'OPENED'
                 when ended_mx is null and days_diff > 30 and days_diff <= 210 then 'DELINQUENT'
                 when ended_mx is null and days_diff > 210 then 'DEFAULTED'
                 when ended_mx is not null then 'CLOSED'
                 else 'UNKNOWN'
                 end                                                          as loan_status
         from loan_table_v2 o
                  left join offers_table oft
                            on oft.user_id = o.user_id
         order by started_mx)

select *
from od_loan_book;
/* Overdraft with purchases*/
with loan_table as (
    select o.user_id,
           o.free_limit_started,
           o.started_mx,
           o.ended_mx,
           o.available_balance_started
    from ds_overdraft.overdraft o
             join (select user_id,
                          started_mx::date as started_date_mx,
                          max(started_mx)  as started_mx
                   from ds_overdraft.overdraft
                   group by user_id, started_date_mx) lo
                  on o.user_id = lo.user_id and o.started_mx = lo.started_mx),

     loan_table_v2 as (
         select dl.user_id,
                dl.available_balance_started,
                left(started_mx, 23)                                                as started_mx,
                left(ended_mx, 23)                                                  as ended_mx,
                sum(coalesce(amount, 0))                                            as incoming,
                max(timestamp_mx_created_at)                                           last_incoming_date,
                datediff(days, greatest(started_mx::date, last_incoming_date::date),
                         coalesce(ended_mx::date, convert_timezone('America/Mexico_City'::character varying::text,
                                                                   getdate()))) + 1 as days_diff,
                sum(amount)                                                         as payment_amount_,
                case
                    when payment_amount_ > dl.available_balance_started * -1
                        then dl.available_balance_started * -1
                    when ended_mx is not null then dl.available_balance_started * -1
                    else payment_amount_ end                                        as payment_amount
         from loan_table dl
                  left join (select *
                             from analytics_bi.transactions
                             where data_science_description in ('CLABE_INCOMING', 'DEPOSIT')
                               and state = 'SETTLED') tx
                            on dl.user_id = tx.user_id and
                               tx.timestamp_mx_created_at >= dl.started_mx and
                               tx.timestamp_mx_created_at <=
                               least(dl.ended_mx::date, convert_timezone('America/Mexico_City'::character varying::text,
                                                                         getdate()))
         group by dl.user_id, dl.started_mx, dl.ended_mx, dl.available_balance_started
     ),

     first_offers as (SELECT h.user_id,
                             convert_timezone('America/Mexico_City'::character varying::text,
                                              h.operation_timestamp)                              AS set_current_limit_operation_mx,
                             json_extract_path_text(h.offer::text, 'freeLimit'::character varying::text,
                                                    'units'::character varying::text)             AS free_limit,
                             convert_timezone('America/Mexico_City'::text,
                                              mtim.first_accepted_offer)                          AS first_accepted_offer_mx,
                             json_extract_path_text(h.offer::text, 'id'::character varying::text) AS offer_id,
                             rank()
                             OVER (
                                 PARTITION BY h.user_id
                                 ORDER BY set_current_limit_operation_mx asc)                     AS r
                      FROM overdraft_current_limits_history h
                               JOIN (SELECT foa.user_id, min(foa.operation_timestamp) AS first_accepted_offer
                                     FROM overdraft_current_limits_history foa
                                     GROUP BY foa.user_id) mtim
                                    ON h.user_id::text = mtim.user_id::text and
                                       h.operation::text <> 'D'::character varying::text),
     offers_table as (
         select *
         from first_offers
         where r = 1),

     od_loan_book as (
         select
             --   concat(oft.user_id, o.started_mx) as od_loan_id,
             ROW_NUMBER() OVER (ORDER BY o.started_mx asc)                    as od_loan_id,
             oft.user_id,
             oft.offer_id,
             oft.first_accepted_offer_mx,
             free_limit,
             o.started_mx                                                     as disbursement_date,
             o.ended_mx                                                       as closed_date,
             left(dateadd(days, 30, o.started_mx::date), 10)                  as maturity_date,
             case
                 when last_incoming_date::date is null then left(closed_date::date, 10)
                 when last_incoming_date::date < closed_date::date then left(closed_date, 10)
                 else left(last_incoming_date, 10) end                        as last_payment_date,
             (days_diff - 30)                                                 as days_past_due,
             o.available_balance_started                                      as overdraft_loan_amount,
             payment_amount,
             case
                 when ended_mx is not null then 0
                 when overdraft_loan_amount * -1 > payment_amount then overdraft_loan_amount * -1 - payment_amount
                 when last_payment_date is null and payment_amount is null then overdraft_loan_amount*-1
                 else overdraft_loan_amount*-1 - payment_amount end                                      as balance,
             rank() over (partition by oft.user_id order by o.started_mx asc) as Sequence,
             case
                 when ended_mx is null and days_diff <= 30 then 'OPENED'
                 when ended_mx is null and days_diff > 30 and days_diff <= 210 then 'DELINQUENT'
                 when ended_mx is null and days_diff > 210 then 'DEFAULTED'
                 when ended_mx is not null then 'CLOSED'
                 else 'UNKNOWN'
                 end                                                          as loan_status
         from loan_table_v2 o
                  left join offers_table oft
                            on oft.user_id = o.user_id
         order by started_mx)

select t.transaction_id,
       t.type,
       t.amount,
       t.timestamp_mx
from analytics_bi.transactions_new as t
where t.type = 'PURCHASE' and t.user_id in (
    select
    user_id
    from loan_table
    )
