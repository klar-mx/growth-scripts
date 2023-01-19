import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from tqdm import tqdm
import time
from scipy import stats

# BD Connection
f = open("C:\\Users\\gabri\\Documents\\Queries\\db_klarprod_connection.txt", "r")
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)

clock = time.time()
query_info = '''
with galileo_info as (
    select c.user_id, r.*
        from analytics_bi.galileo_auth r
            left join (
                SELECT uare.external_id__card_number as cad, u.user_id
                FROM analytics.useraccountevent__accounts__references__external_id uare
                    INNER JOIN analytics.useraccountevent__accounts__references uar ON uar.__ID = uare.useraccountevent__accounts__references__id
                    INNER JOIN analytics.useraccountevent__accounts ua ON ua.__ID = uar.useraccountevent__accounts__id
                    INNER JOIN analytics.useraccountevent u ON u.__ID = ua.useraccountevent__id
                    LEFT OUTER JOIN public.db_kyc_public_user_kyc e ON e.id = u.user_id AND uare.external_id__card_number IS NOT NULL AND uare.external_id__card_number != '') c on c.cad::text = r.card_identifier::text
) ,
     specific_declines as (
         select user_id, coalesce(invalid_card, 0) as invalid_card, coalesce(insufficient_funds, 0) as insufficient_funds, coalesce(invalid_pin, 0) as invalid_pin
         from (
             select user_id, authorization_response, count(*) as count_attempts
             from galileo_info
             WHERE (authorization_response in ('14', '51', '55'))
             group by user_id, authorization_response
             order by user_id)
             PIVOT(
                 sum (count_attempts)
                     FOR authorization_response IN ('14' as invalid_card, '51' as insufficient_funds,'55' as invalid_pin)
                 )
),
     total_purchase_attempts as (
         select user_id,
                count(distinct transaction_timestamp) num_attempts
         from galileo_info
         group by user_id
         order by user_id
),
     total_accepted_attempts as (
        select user_id,
                count(distinct transaction_timestamp) accepted_attempts
         from galileo_info
        WHERE ((authorization_response = '00' or authorization_response = '85') and authorization_status != 'Z')
         group by user_id
         order by user_id
),
     total_declined_attempts as (
        select user_id,
                count(distinct transaction_timestamp) declined_attempts
         from galileo_info
        WHERE ((authorization_response != '00' and authorization_response != '85') or authorization_status = 'Z')
         group by user_id
         order by user_id
),
     rfm_layer_a_info as(
        select rfm_layer_a.user_id,
               max_user.max_date,
               rfm_layer_a.layer_a
        from growth.rfm_layers as rfm_layer_a,
             (select user_id,
                     max(date) as max_date
             from growth.rfm_layers
--            where date < '2021-12-01'::date
             group by user_id) max_user
        where rfm_layer_a.user_id = max_user.user_id 
        and rfm_layer_a.date = max_user.max_date
        order by 2
    ),
     credit_info as (
         select user_id,
                first_offer_created_at,
                first_loan_created_at,
                datediff(day, first_offer_created_at::date,first_loan_created_at::date) as days_for_acceptance,
                user_status
        from credit.credit_user
     ),
     overdraft_info as(
         select overdraft_info.user_id,
                first_overdraft_offer_timestamp,
                first_overdraft_accepted_offer_timestamp,
                datediff(day, first_overdraft_offer_timestamp::date, first_overdraft_accepted_offer_timestamp::date) as days_for_acceptance,
                last_overdraft.user_status,
                last_overdraft.lastest_overdraft as overdraft_num
         from ds_overdraft.overdraft_user_view as overdraft_info
             join (select o.user_id, loan_status as user_status, lastest_overdraft
             from ds_overdraft.overdraft_loanbook as o,
                  (select user_id,
                          max(sequence) as lastest_overdraft
                  from ds_overdraft.overdraft_loanbook
                  group by user_id) last_overdraft
             where o.user_id = last_overdraft.user_id and o.sequence = last_overdraft.lastest_overdraft
                 )  as last_overdraft
             on overdraft_info.user_id = last_overdraft.user_id
     )
select total_purchase_attempts.*,
       total_accepted_attempts.accepted_attempts,
       total_declined_attempts.declined_attempts,
       specific_declines.invalid_card,
       specific_declines.insufficient_funds,
       specific_declines.invalid_pin,
       rfm_layer_a_info.max_date,
       rfm_layer_a_info.layer_a,
       credit_info.user_status,
       overdraft_info.user_status
from total_purchase_attempts
left join total_accepted_attempts
on total_purchase_attempts.user_id = total_accepted_attempts.user_id
left join total_declined_attempts
on total_purchase_attempts.user_id = total_declined_attempts.user_id
left join specific_declines
on total_purchase_attempts.user_id = specific_declines.user_id
left join rfm_layer_a_info
on total_purchase_attempts.user_id = rfm_layer_a_info.user_id
left join credit_info
on total_purchase_attempts.user_id = credit_info.user_id
left join overdraft_info
on total_purchase_attempts.user_id = overdraft_info.user_id
'''

approval_rfm_df = pd.read_sql_query(sqlalchemy.text(query_info), cnx)

print("finished query in: {}".format(time.time() - clock))
# =================================================================================== OVERALL ===================================================================================================
approval_rfm_df.columns = ['user_id', 'num_attempts', 'accepted_attempts', 'declined_attempts', 'invalid_card', 'insufficient_funds', 'invalid_pin', 'max_date', 'layer_a', 'credit_user_status', 'overdraft_user_status']
# ========================================================================= APPROVAL RATE WITHOUT DELINQUENTS ===================================================================================================
approval_rate_without_delinquents = approval_rfm_df[(~approval_rfm_df.credit_user_status.isin(['CHURNED CREDIT', 'DELINQUENT'])) & (approval_rfm_df.overdraft_user_status != 'DEFAULTED')].groupby(['layer_a']).agg(
    {'user_id': 'count', 'num_attempts': 'sum', 'accepted_attempts': 'sum', 'declined_attempts': 'sum', 'invalid_card': 'sum', 'insufficient_funds': 'sum', 'invalid_pin': 'sum'})

# ========================================================================= DELINQUENT APPROVAL RATE ===================================================================================================
approval_rate_delinquents = approval_rfm_df[(approval_rfm_df.credit_user_status.isin(['CHURNED CREDIT', 'DELINQUENT'])) | (approval_rfm_df.overdraft_user_status == 'DEFAULTED')].groupby(['layer_a']).agg(
    {'user_id': 'count', 'num_attempts': 'sum', 'accepted_attempts': 'sum', 'declined_attempts': 'sum', 'invalid_card': 'sum', 'insufficient_funds': 'sum', 'invalid_pin': 'sum'})

# ========================================================================= CREDIT USERS APPROVAL RATE ===================================================================================================
approval_rate_credit = approval_rfm_df[~approval_rfm_df.credit_user_status.isna()].groupby(['layer_a', 'credit_user_status']).agg(
    {'user_id': 'count', 'num_attempts': 'sum', 'accepted_attempts': 'sum', 'declined_attempts': 'sum', 'invalid_card': 'sum', 'insufficient_funds': 'sum', 'invalid_pin': 'sum'})
# Unstack result
approval_rate_credit = approval_rate_credit.reset_index()
# ========================================================================= OVERDRAFT USERS APPROVAL RATE ===================================================================================================
approval_rate_overdraft = approval_rfm_df[~approval_rfm_df.overdraft_user_status.isna()].groupby(['layer_a', 'overdraft_user_status']).agg(
    {'user_id': 'count', 'num_attempts': 'sum', 'accepted_attempts': 'sum', 'declined_attempts': 'sum', 'invalid_card': 'sum', 'insufficient_funds': 'sum', 'invalid_pin': 'sum'})
# Unstack result
approval_rate_overdraft = approval_rate_overdraft.reset_index()
# ================================================================== APPROVAL RATE  WITH/WITHOUT OVERDRAFT-CREDIT===================================================================================================
# Df with no delinquents
approval_rfm_df_no_delinq = approval_rfm_df[(~approval_rfm_df.credit_user_status.isin(['CHURNED CREDIT', 'DELINQUENT'])) & (approval_rfm_df.overdraft_user_status != 'DEFAULTED')]
# Credit
approval_rate_with_credit = approval_rfm_df_no_delinq[(~approval_rfm_df_no_delinq.credit_user_status.isna()) & (approval_rfm_df_no_delinq.overdraft_user_status.isna())].agg(
    {'user_id': 'count', 'num_attempts': 'sum', 'accepted_attempts': 'sum', 'declined_attempts': 'sum', 'invalid_card': 'sum', 'insufficient_funds': 'sum', 'invalid_pin': 'sum'})
# With Overdraft
approval_rate_with_overdraft = approval_rfm_df_no_delinq[(approval_rfm_df_no_delinq.credit_user_status.isna()) & (~approval_rfm_df_no_delinq.overdraft_user_status.isna())].agg(
    {'user_id': 'count', 'num_attempts': 'sum', 'accepted_attempts': 'sum', 'declined_attempts': 'sum', 'invalid_card': 'sum', 'insufficient_funds': 'sum', 'invalid_pin': 'sum'})
# With Both
approval_rate_with_both = approval_rfm_df_no_delinq[(~approval_rfm_df_no_delinq.credit_user_status.isna()) & (~approval_rfm_df_no_delinq.overdraft_user_status.isna())].agg(
    {'user_id': 'count', 'num_attempts': 'sum', 'accepted_attempts': 'sum', 'declined_attempts': 'sum', 'invalid_card': 'sum', 'insufficient_funds': 'sum', 'invalid_pin': 'sum'})
# Without any
approval_rate_without = approval_rfm_df_no_delinq[(approval_rfm_df_no_delinq.credit_user_status.isna()) & (approval_rfm_df_no_delinq.overdraft_user_status.isna())].agg(
    {'user_id': 'count', 'num_attempts': 'sum', 'accepted_attempts': 'sum', 'declined_attempts': 'sum', 'invalid_card': 'sum', 'insufficient_funds': 'sum', 'invalid_pin': 'sum'})
# ========================================================================= NPS INTEGRATION ===================================================================================================
nps_raw = pd.read_csv(r'C:\Users\gabri\PycharmProjects\Klar\NPS\Data\NPS_Responses_24May.csv')
nps_emails = nps_raw.email.drop_duplicates().dropna()
email_list = tuple(nps_emails.to_list())
# Query for emails
email_query = '''
    select user_id,
           email
    from klar.dim_funnel_v3
    where email in {}
'''.format(email_list)
# Execute Query
user_ids_nps = pd.read_sql_query(email_query, cnx)
# NPS with user_id
nps_data = nps_raw[['Submission Date', 'source', 'email', 'En una escala del 0 al 10 ¿Qué tan probable es que recomiendes Klar a tus amigos o familiares?']]
nps_data.columns = ['date', 'source', 'email', 'nps']
nps_data = nps_data.merge(user_ids_nps, on='email')
# Integration with Overaal data
approval_rfm_df = approval_rfm_df.merge(nps_data, on = 'user_id', how = 'left')
# Calc acceptance rate per user
approval_rfm_df['acceptance_rate'] = approval_rfm_df.accepted_attempts.div(approval_rfm_df.num_attempts, fill_value=0)*100
# Rearrange columns
approval_rfm_df = approval_rfm_df[['user_id', 'num_attempts', 'accepted_attempts', 'declined_attempts','acceptance_rate',
       'invalid_card', 'insufficient_funds', 'invalid_pin', 'max_date',
       'layer_a', 'credit_user_status', 'overdraft_user_status', 'date',
       'source', 'email', 'nps']]
# Correlation between NPS score and acceptance rate
nps_acceptance = approval_rfm_df[~approval_rfm_df.nps.isna()][['nps', 'acceptance_rate']]
correlation = approval_rfm_df[~approval_rfm_df.nps.isna()]['nps'].corr(approval_rfm_df.acceptance_rate)
correlations = approval_rfm_df.corr()
# ANOVA
F, p = stats.f_oneway(approval_rfm_df[~approval_rfm_df.nps.isna()]['nps'],approval_rfm_df[~approval_rfm_df.nps.isna()]['acceptance_rate'])
# Dummies
nps_acceptance_dummy = pd.concat([nps_acceptance[['acceptance_rate']], pd.get_dummies(nps_acceptance['nps'])], axis=1)
dummies_correlations = nps_acceptance_dummy.corr()