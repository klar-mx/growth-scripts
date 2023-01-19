import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from tqdm import tqdm

# BD Connection
f = open("C:\\Users\\gabri\\Documents\\Queries\\db_klarprod_connection.txt", "r")
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)
# Query for Max Business Month
max_business_month_query = '''
with all_fta_users as (
select f.user_id,
       f.email,
       f.phone_number,
       f.first_deposit,
       date_trunc('month', f.first_deposit ::date) as monthly_cohort
from klar.dim_funnel_v2 f
where f.first_deposit is not null
),


full_transaction_data as (
select tx.transaction_id,
	   tx.timestamp_mx_created_at,
	   tx.timestamp_mx,
       tx.amount,
       tx.currency_code,
       tx.provider_id,
       tx.type,
       tx.state,
       tx.short_description,
       tx.business_description,
       tx.data_science_description,
       tx.description,
       tx.source_type,
       tx.target_type,
       tx.card_type,
       tx.source_account_internal_id,
       cus.*,
	   datediff(days, cus.first_deposit::date, tx.timestamp_mx_created_at::date) as days_since_activation,
	   days_since_activation / 30 as business_month,
	   case when tx.type in ('PURCHASE', 'DEPOSIT', 'QUASI-CASH') and tx.provider_id = 'GALILEO' then 1 else 0 end as purchase,
	   case when tx.type in ('PURCHASE', 'DEPOSIT', 'QUASI-CASH') and tx.provider_id = 'GALILEO' then tx.amount else 0 end as purchase_amount,
	   case when tx.type = 'DISBURSEMENT' and tx.provider_id = 'GALILEO' then 1 else 0 end as withdrawal,
	   case when tx.type = 'DISBURSEMENT' and tx.provider_id = 'GALILEO' then tx.amount else 0 end as withdrawal_amount,
	   case when tx.type = 'FEE' and tx.provider_id = 'GALILEO' then 1 else 0 end as balance_check,
	   case when tx.type = 'FEE' and tx.provider_id = 'GALILEO' then tx.amount else 0 end as balance_check_amount,
	   case when tx.type in ('ADJUSTMENT', 'DEPOSIT', 'DISBURSEMENT', 'FEE', 'PURCHASE', 'QUASI-CASH') and tx.provider_id = 'GALILEO' then 1 else 0 end as card_transaction,
	   case when tx.type in ('ADJUSTMENT', 'DEPOSIT', 'DISBURSEMENT', 'FEE', 'PURCHASE', 'QUASI-CASH') and tx.provider_id = 'GALILEO' then tx.amount else 0 end as card_transaction_amount,
	   case when tx.type in ('DEPOSIT', 'REVERSAL') and tx.provider_id = 'OPEN_PAY' then 1 else 0 end as openpay_deposit,
	   case when tx.type in ('DEPOSIT', 'REVERSAL') and tx.provider_id = 'OPEN_PAY' then tx.amount else 0 end as openpay_deposit_amount,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' then 1 else 0 end as transfer,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' then tx.amount else 0 end as transfer_amount,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' and tx.data_science_description = 'CLABE_OUTGOING' then 1 else 0 end as transfer_out,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' and tx.data_science_description = 'CLABE_OUTGOING' then tx.amount else 0 end as transfer_out_amount,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' and tx.data_science_description = 'CLABE_INCOMING' then 1 else 0 end as transfer_in,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' and tx.data_science_description = 'CLABE_INCOMING' then tx.amount else 0 end as transfer_in_amount,
	   case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Referidos', '+Referidos') then 1 else 0 end as rewards_referral,
	   case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Referidos', '+Referidos') then tx.amount else 0 end as rewards_referral_amount,
       case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Beneficios', '+Beneficios', 'Giveaway 14-02') then 1 else 0 end as rewards_mktcampaign,
       case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Beneficios', '+Beneficios', 'Giveaway 14-02') then tx.amount else 0 end as rewards_mktcampaign_amount,
       case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Reembolsos', '+Reembolsos', 'Reembolso max de $25 en hasta 3 retiros en cajero al mes') then 1 else 0 end as rewards_reembolso_fee,
       case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Reembolsos', '+Reembolsos', 'Reembolso max de $25 en hasta 3 retiros en cajero al mes') then tx.amount else 0 end as rewards_reembolso_fee_amount,
	   case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Cashback', '+Cashback', 'Recibe 1% en Cashback después de completar 10 compras') then 1 else 0 end as rewards_cashback,
	   case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Cashback', '+Cashback', 'Recibe 1% en Cashback después de completar 10 compras') then tx.amount else 0 end as rewards_cashback_amount
from analytics_bi.transactions tx
left join all_fta_users as cus using(user_id)
),

total_number as (
select
full_transaction_data.monthly_cohort,
count(distinct full_transaction_data.user_id) as total_number
from full_transaction_data
group by 1 ),

final_table_tx as (
select user_id,
	   monthly_cohort,
       business_month,
       sum(purchase) as count_purchases,
       sum(purchase_amount) as purchases_amount,
       sum(withdrawal) as count_withdrawals,
       sum(withdrawal_amount) as withdrawals_amount,
       sum(balance_check) as balance_checks,
       sum(balance_check_amount) as balance_checks_amount,
       sum(card_transaction) as card_transactions,
       sum(card_transaction_amount) as card_transactions_amount,
       sum(openpay_deposit) as openpay_deposits,
       sum(openpay_deposit_amount) as openpay_deposits_amount,
       sum(transfer) as transfers,
       sum(transfer_amount) as transfers_amount,
       sum(transfer_out) as transfers_out,
       sum(transfer_out_amount) as transfers_out_amount,
       sum(transfer_in) as transfers_in,
       sum(transfer_in_amount) as transfers_in_amount,
       sum(rewards_referral) as rewards_referrals,
       sum(rewards_referral_amount) as rewards_referrals_amount,
       sum(rewards_mktcampaign) as rewards_mktcampaigns,
       sum(rewards_mktcampaign_amount) as rewards_mktcampaigns_amount,
       sum(rewards_reembolso_fee) as rewards_reembolsos_fees,
       sum(rewards_reembolso_fee_amount) as rewards_reembolsos_fees_amount,
       sum(rewards_cashback) as rewards_cashbacks,
       sum(rewards_cashback_amount) as rewards_cashbacks
from full_transaction_data as tx
where tx.state = 'SETTLED' and tx.source_account_internal_id <> '0000000000000000' AND tx.source_account_internal_id <> '00000000-0000-0000-0000-000000000000' and business_month >= 0
group by user_id, tx.monthly_cohort, business_month),

credit_info_1 as (select
cus.*,
t.loan_id,
t.loan_amount,
datediff(days, cus.first_deposit::date, t.disbursement_date::date) as days_since_disbursement,
	   days_since_disbursement / 30 as business_month
from credit.klar_loans t
left join all_fta_users as cus on cus.user_id = t.user_id),

credit_info_final as (select
ci.user_id,
ci.monthly_cohort,
ci.business_month,
sum(ci.loan_amount) as loan_amount
from credit_info_1 ci
group by 1,2,3),

adjust_processed as (
 select
         DISTINCT adjust.klaruserid ,network_name
     from public.adjust
         join (select
         klaruserid,
                max (last_session_time) as max_created
         from public.adjust
         group by klaruserid) as max_time_user
     on adjust.klaruserid = max_time_user.klaruserid and adjust.last_session_time = max_time_user.max_created
     where adjust.klaruserid notnull
     order by 1
 ),
 
 
channel_final as (select distinct funnel.user_id,
        funnel.first_deposit,
        adjust.network_name,
        referral.referral_code,
        case
            when referral_code notnull then 'Referral'
            when network_name = '5fow643x' and referral_code is null then 'ZoomD'
            when network_name = 'Apple Search Ads' and referral_code is null then 'Apple Search'
            when network_name = 'Carlos Estrada' and referral_code is null then 'Influencers'
            when network_name = 'Eduardo Rosas' and referral_code is null then 'Influencers'
            when network_name like '%Facebook%' and referral_code is null then 'Facebook'
            when network_name like '%Google%' and referral_code is null then 'Google'
            when network_name like '%Influencer%' and referral_code is null then 'Influencers'
            when network_name like 'Instagram Installs' and referral_code is null then 'Facebook'
            when network_name like 'Joel Video' and referral_code is null then 'Influencers'
            when network_name like 'Klar.mx' and referral_code is null then 'Klar'
            when network_name like '%Liftoff%' and referral_code is null then 'Liftoff'
            when network_name like 'New_Respaldo' and referral_code is null then 'Klar'
            when network_name like 'Snapchar Installs' and referral_code is null then 'Snapchat'
            when network_name like 'TikTok ZoomD' and referral_code is null then 'TikTok'
            when network_name like 'Tiktok Organic' and referral_code is null then 'TikTok'
            when network_name like 'UA48736' and referral_code is null then 'ZoomD'
            when network_name like 'Unattributed' and referral_code is null then 'Facebook'
            when network_name like 'Unstrusted Devices' and referral_code is null then 'Digital Turbine'
            when network_name like 'tiktok' and referral_code is null then 'TikTok'
            when network_name like '%ZoomD%' and referral_code is null then 'ZoomD'
            when network_name like '%Zoomd%' and referral_code is null then 'ZoomD'
            when network_name is null and referral_code is null then 'Organic'
            else network_name
        end as Channel
from klar.dim_funnel_v2 as funnel
left join adjust_processed as adjust on funnel.user_id = adjust.klaruserid
left join growth.referral_info as referral on funnel.user_id = referree_user_id),



final_table as (select
f.*,
c.loan_amount,
T.total_number,
a.Channel
from final_table_tx f
left join total_number T on f.monthly_cohort = T.monthly_cohort
left join credit_info_final c on f.monthly_cohort = c.monthly_cohort and c.business_month = f.business_month and f.user_id = c.user_id
left join channel_final a on a.user_id = f.user_id
order by f.user_id asc, business_month asc),

users_channel as (
select
monthly_cohort,
channel,
count(DISTINCT user_id) as total_users_channel
from final_table
group by 1,2),

ft as (select
f.*,
u.total_users_channel
from final_table f 
left join users_channel u on u.monthly_cohort = f.monthly_cohort and u.channel = f.channel
order by f.user_id, f.monthly_cohort, business_month)

select
distinct business_month
from ft
'''
# Dataframe with business month information
business_months = pd.read_sql_query(sqlalchemy.text(max_business_month_query), cnx)
# Max Business Month
max_business_month = business_months.business_month.max()
# Query for Average Purchases in First Business month for Retained Users by Business Month
avg_purchases_by_month_query = '''
with all_fta_users as (
select f.user_id,
       f.email,
       f.phone_number,
       f.first_deposit,
       date_trunc('month', f.first_deposit ::date) as monthly_cohort
from klar.dim_funnel_v2 f
where f.first_deposit is not null
),


full_transaction_data as (
select tx.transaction_id,
	   tx.timestamp_mx_created_at,
	   tx.timestamp_mx,
       tx.amount,
       tx.currency_code,
       tx.provider_id,
       tx.type,
       tx.state,
       tx.short_description,
       tx.business_description,
       tx.data_science_description,
       tx.description,
       tx.source_type,
       tx.target_type,
       tx.card_type,
       tx.source_account_internal_id,
       cus.*,
	   datediff(days, cus.first_deposit::date, tx.timestamp_mx_created_at::date) as days_since_activation,
	   days_since_activation / 30 as business_month,
	   case when tx.type in ('PURCHASE', 'DEPOSIT', 'QUASI-CASH') and tx.provider_id = 'GALILEO' then 1 else 0 end as purchase,
	   case when tx.type in ('PURCHASE', 'DEPOSIT', 'QUASI-CASH') and tx.provider_id = 'GALILEO' then tx.amount else 0 end as purchase_amount,
	   case when tx.type = 'DISBURSEMENT' and tx.provider_id = 'GALILEO' then 1 else 0 end as withdrawal,
	   case when tx.type = 'DISBURSEMENT' and tx.provider_id = 'GALILEO' then tx.amount else 0 end as withdrawal_amount,
	   case when tx.type = 'FEE' and tx.provider_id = 'GALILEO' then 1 else 0 end as balance_check,
	   case when tx.type = 'FEE' and tx.provider_id = 'GALILEO' then tx.amount else 0 end as balance_check_amount,
	   case when tx.type in ('ADJUSTMENT', 'DEPOSIT', 'DISBURSEMENT', 'FEE', 'PURCHASE', 'QUASI-CASH') and tx.provider_id = 'GALILEO' then 1 else 0 end as card_transaction,
	   case when tx.type in ('ADJUSTMENT', 'DEPOSIT', 'DISBURSEMENT', 'FEE', 'PURCHASE', 'QUASI-CASH') and tx.provider_id = 'GALILEO' then tx.amount else 0 end as card_transaction_amount,
	   case when tx.type in ('DEPOSIT', 'REVERSAL') and tx.provider_id = 'OPEN_PAY' then 1 else 0 end as openpay_deposit,
	   case when tx.type in ('DEPOSIT', 'REVERSAL') and tx.provider_id = 'OPEN_PAY' then tx.amount else 0 end as openpay_deposit_amount,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' then 1 else 0 end as transfer,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' then tx.amount else 0 end as transfer_amount,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' and tx.data_science_description = 'CLABE_OUTGOING' then 1 else 0 end as transfer_out,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' and tx.data_science_description = 'CLABE_OUTGOING' then tx.amount else 0 end as transfer_out_amount,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' and tx.data_science_description = 'CLABE_INCOMING' then 1 else 0 end as transfer_in,
	   case when tx.type = 'TRANSFER' and tx.provider_id = 'STP' and tx.data_science_description = 'CLABE_INCOMING' then tx.amount else 0 end as transfer_in_amount,
	   case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Referidos', '+Referidos') then 1 else 0 end as rewards_referral,
	   case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Referidos', '+Referidos') then tx.amount else 0 end as rewards_referral_amount,
       case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Beneficios', '+Beneficios', 'Giveaway 14-02') then 1 else 0 end as rewards_mktcampaign,
       case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Beneficios', '+Beneficios', 'Giveaway 14-02') then tx.amount else 0 end as rewards_mktcampaign_amount,
       case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Reembolsos', '+Reembolsos', 'Reembolso max de $25 en hasta 3 retiros en cajero al mes') then 1 else 0 end as rewards_reembolso_fee,
       case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Reembolsos', '+Reembolsos', 'Reembolso max de $25 en hasta 3 retiros en cajero al mes') then tx.amount else 0 end as rewards_reembolso_fee_amount,
	   case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Cashback', '+Cashback', 'Recibe 1% en Cashback después de completar 10 compras') then 1 else 0 end as rewards_cashback,
	   case when tx.type = 'REWARD' and tx.provider_id <> 'KLAR' and tx.short_description in ('Cashback', '+Cashback', 'Recibe 1% en Cashback después de completar 10 compras') then tx.amount else 0 end as rewards_cashback_amount
from analytics_bi.transactions tx
left join all_fta_users as cus using(user_id)
),

total_number as (
select
full_transaction_data.monthly_cohort,
count(distinct full_transaction_data.user_id) as total_number
from full_transaction_data
group by 1 ),

final_table_tx as (
select user_id,
	   monthly_cohort,
       business_month,
       sum(purchase) as count_purchases,
       sum(purchase_amount) as purchases_amount,
       sum(withdrawal) as count_withdrawals,
       sum(withdrawal_amount) as withdrawals_amount,
       sum(balance_check) as balance_checks,
       sum(balance_check_amount) as balance_checks_amount,
       sum(card_transaction) as card_transactions,
       sum(card_transaction_amount) as card_transactions_amount,
       sum(openpay_deposit) as openpay_deposits,
       sum(openpay_deposit_amount) as openpay_deposits_amount,
       sum(transfer) as transfers,
       sum(transfer_amount) as transfers_amount,
       sum(transfer_out) as transfers_out,
       sum(transfer_out_amount) as transfers_out_amount,
       sum(transfer_in) as transfers_in,
       sum(transfer_in_amount) as transfers_in_amount,
       sum(rewards_referral) as rewards_referrals,
       sum(rewards_referral_amount) as rewards_referrals_amount,
       sum(rewards_mktcampaign) as rewards_mktcampaigns,
       sum(rewards_mktcampaign_amount) as rewards_mktcampaigns_amount,
       sum(rewards_reembolso_fee) as rewards_reembolsos_fees,
       sum(rewards_reembolso_fee_amount) as rewards_reembolsos_fees_amount,
       sum(rewards_cashback) as rewards_cashbacks,
       sum(rewards_cashback_amount) as rewards_cashbacks
from full_transaction_data as tx
where tx.state = 'SETTLED' and tx.source_account_internal_id <> '0000000000000000' AND tx.source_account_internal_id <> '00000000-0000-0000-0000-000000000000' and business_month >= 0
group by user_id, tx.monthly_cohort, business_month),

credit_info_1 as (select
cus.*,
t.loan_id,
t.loan_amount,
datediff(days, cus.first_deposit::date, t.disbursement_date::date) as days_since_disbursement,
	   days_since_disbursement / 30 as business_month
from credit.klar_loans t
left join all_fta_users as cus on cus.user_id = t.user_id),

credit_info_final as (select
ci.user_id,
ci.monthly_cohort,
ci.business_month,
sum(ci.loan_amount) as loan_amount
from credit_info_1 ci
group by 1,2,3),

adjust_processed as (
 select
         DISTINCT adjust.klaruserid ,network_name
     from public.adjust
         join (select
         klaruserid,
                max (last_session_time) as max_created
         from public.adjust
         group by klaruserid) as max_time_user
     on adjust.klaruserid = max_time_user.klaruserid and adjust.last_session_time = max_time_user.max_created
     where adjust.klaruserid notnull
     order by 1
 ),
 
 
channel_final as (select distinct funnel.user_id,
        funnel.first_deposit,
        adjust.network_name,
        referral.referral_code,
        case
            when referral_code notnull then 'Referral'
            when network_name = '5fow643x' and referral_code is null then 'ZoomD'
            when network_name = 'Apple Search Ads' and referral_code is null then 'Apple Search'
            when network_name = 'Carlos Estrada' and referral_code is null then 'Influencers'
            when network_name = 'Eduardo Rosas' and referral_code is null then 'Influencers'
            when network_name like '%Facebook%' and referral_code is null then 'Facebook'
            when network_name like '%Google%' and referral_code is null then 'Google'
            when network_name like '%Influencer%' and referral_code is null then 'Influencers'
            when network_name like 'Instagram Installs' and referral_code is null then 'Facebook'
            when network_name like 'Joel Video' and referral_code is null then 'Influencers'
            when network_name like 'Klar.mx' and referral_code is null then 'Klar'
            when network_name like '%Liftoff%' and referral_code is null then 'Liftoff'
            when network_name like 'New_Respaldo' and referral_code is null then 'Klar'
            when network_name like 'Snapchar Installs' and referral_code is null then 'Snapchat'
            when network_name like 'TikTok ZoomD' and referral_code is null then 'TikTok'
            when network_name like 'Tiktok Organic' and referral_code is null then 'TikTok'
            when network_name like 'UA48736' and referral_code is null then 'ZoomD'
            when network_name like 'Unattributed' and referral_code is null then 'Facebook'
            when network_name like 'Unstrusted Devices' and referral_code is null then 'Digital Turbine'
            when network_name like 'tiktok' and referral_code is null then 'TikTok'
            when network_name like '%ZoomD%' and referral_code is null then 'ZoomD'
            when network_name like '%Zoomd%' and referral_code is null then 'ZoomD'
            when network_name is null and referral_code is null then 'Organic'
            else network_name
        end as Channel
from klar.dim_funnel_v2 as funnel
left join adjust_processed as adjust on funnel.user_id = adjust.klaruserid
left join growth.referral_info as referral on funnel.user_id = referree_user_id),



final_table as (select
f.*,
c.loan_amount,
T.total_number,
a.Channel
from final_table_tx f
left join total_number T on f.monthly_cohort = T.monthly_cohort
left join credit_info_final c on f.monthly_cohort = c.monthly_cohort and c.business_month = f.business_month and f.user_id = c.user_id
left join channel_final a on a.user_id = f.user_id
order by f.user_id asc, business_month asc),

users_channel as (
select
monthly_cohort,
channel,
count(DISTINCT user_id) as total_users_channel
from final_table
group by 1,2),

ft as (select
f.*,
u.total_users_channel
from final_table f 
left join users_channel u on u.monthly_cohort = f.monthly_cohort and u.channel = f.channel
order by f.user_id, f.monthly_cohort, business_month)

select
monthly_cohort,
avg(count_purchases*1.0) as avg_purchases,
avg(purchases_amount*1.0)as avg_amount
from ft
where business_month = 0 and user_id in (
select
user_id
from ft
where (count_purchases > 0 or count_withdrawals > 0 or balance_checks > 0 or card_transactions > 0 or openpay_deposits > 0 or transfers > 0 or transfers_out > 0 or transfers_in> 0 or rewards_referrals > 0 or rewards_mktcampaigns > 0 or rewards_reembolsos_fees > 0)
and business_month = {}
)
group by 1
order by 1

'''

res = pd.DataFrame(columns=['Cohort'])

for i in tqdm(range(max_business_month)):
    query = avg_purchases_by_month_query.format(i+1)
    aux = pd.read_sql_query(sqlalchemy.text(query), cnx)
    aux.columns = ['Cohort', 'Avg_Purch_BM{}'.format(i+1), 'Avg_PurchAmount_BM{}'.format(i+1)]
    res = res.merge(aux, on='Cohort', how='outer')
