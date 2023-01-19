import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from tqdm import tqdm

# BD Connection
f = open("C:\\Users\\gabri\\Documents\\Queries\\db_klarprod_connection.txt", "r")
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)

# Query for declines
decline_info_query = '''
with first_n_declines_timestamps as (select *
                                         from (with first_n_declines as (
                                             with number_of_declines as (select c.user_id,
                                                                                r.transaction_timestamp,
                                                                                r.authorization_response,
                                                                                r.authorization_status,
                                                                                rank() over (partition by c.user_id order by r.transaction_timestamp) as number_decline,
                                                                                r.local_amount,
                                                                                merchant_description
                                                                         from analytics_bi.galileo_auth r
                                                                                  left join (
                                                                             SELECT uare.external_id__card_number as cad, u.user_id
                                                                             FROM analytics.useraccountevent__accounts__references__external_id uare
                                                                                      INNER JOIN analytics.useraccountevent__accounts__references uar ON uar.__ID = uare.useraccountevent__accounts__references__id
                                                                                      INNER JOIN analytics.useraccountevent__accounts ua ON ua.__ID = uar.useraccountevent__accounts__id
                                                                                      INNER JOIN analytics.useraccountevent u ON u.__ID = ua.useraccountevent__id
                                                                                      LEFT OUTER JOIN public.db_kyc_public_user_kyc e ON e.id = u.user_id
                                                                                 AND uare.external_id__card_number IS NOT NULL
                                                                                 AND uare.external_id__card_number != '') c on c.cad::text = r.card_identifier::text
                                                                         WHERE ((r.authorization_response != '00' and r.authorization_response != '85') or r.authorization_status = 'Z') and r.authorization_response = '55'
                                                                         order by c.user_id
                                             )
                                             select *
                                             from number_of_declines
                                             where number_decline <= 5)
                                               select first_n_declines.user_id,
                                                      first_n_declines.transaction_timestamp,
                                                      first_n_declines.number_decline
                                               from first_n_declines) PIVOT(
                                                                            MAX(transaction_timestamp)
        FOR number_decline IN ('1' as one,'2' as two,'3' as three,'4' as four,'5' as five)
                                             )
),
 purchases_to_n_decline as (
    select user_info.user_id,
           count(t.*) as purchases_to_n_decline
    from first_n_declines_timestamps
             join klar.dim_funnel_v2 as user_info
                  on user_info.user_id = first_n_declines_timestamps.user_id
             join analytics_bi.transactions as t
                  on user_info.user_id = t.user_id
    where first_n_declines_timestamps.{n} notnull and (t.timestamp_mx_created_at BETWEEN user_info.first_deposit AND first_n_declines_timestamps.{n})
      AND t.type like 'PURCHASE'
    group by user_info.user_id
),
days_to_n_decline as (
    select user_info.user_id,
           datediff(days, user_info.first_deposit, first_n_declines_timestamps.{n}) as days_to_n_decline
    from first_n_declines_timestamps
    join klar.dim_funnel_v2 as user_info
    on user_info.user_id = first_n_declines_timestamps.user_id
    where first_n_declines_timestamps.{n} notnull

    ),
purchases_from_n_decline_to_today as (
    select first_n_declines_timestamps.user_id,
           count(t.*) as purchases_from_n_decline
    from first_n_declines_timestamps
    join analytics_bi.transactions as t
    on first_n_declines_timestamps.user_id = t.user_id
    where first_n_declines_timestamps.{n} notnull and (t.timestamp_mx_created_at BETWEEN first_n_declines_timestamps.{n} and current_date)
      AND t.type like 'PURCHASE'
    group by first_n_declines_timestamps.user_id
),
days_from_n_decline_to_today as (
    select user_info.user_id,
           datediff(days, first_n_declines_timestamps.{n}, current_date) as days_from_n_decline
    from first_n_declines_timestamps
    join klar.dim_funnel_v2 as user_info
    on user_info.user_id = first_n_declines_timestamps.user_id
    where first_n_declines_timestamps.{n} notnull
    )
select
    first_n_declines_timestamps.*,
    user_info.first_deposit,
    purchases_to_n_decline.purchases_to_n_decline,
    days_to_n_decline.days_to_n_decline,
    coalesce(purchases_from_n_decline_to_today.purchases_from_n_decline,0) as purchases_from_n_decline,
    days_from_n_decline_to_today.days_from_n_decline,
    coalesce(credit_info.user_status, 'NO CREDIT USER') as credit_status
from first_n_declines_timestamps
join purchases_to_n_decline
on purchases_to_n_decline.user_id = first_n_declines_timestamps.user_id
join klar.dim_funnel_v2 as user_info
on purchases_to_n_decline.user_id = user_info.user_id
join days_to_n_decline
on purchases_to_n_decline.user_id = days_to_n_decline.user_id
left join purchases_from_n_decline_to_today
on purchases_to_n_decline.user_id = purchases_from_n_decline_to_today.user_id
left join days_from_n_decline_to_today
on days_from_n_decline_to_today.user_id = purchases_to_n_decline.user_id
left join credit.credit_user as credit_info
on first_n_declines_timestamps.user_id = credit_info.user_id
'''
# List for number of decline
n_declines = ['one', 'two', 'three', 'four', 'five']
# List of dataframes
res = []
#Execution for queries
for i in tqdm(n_declines):
    query = decline_info_query.format(n=i)
    aux = pd.read_sql_query(sqlalchemy.text(query), cnx)
    res.append(aux)
# List of tables with new columns
calc_res = []
# For cycle for calculate information
for idx, df in enumerate(res):
    aux_df = df.copy()
    aux_df['days_p_before_decline'] = aux_df['days_to_n_decline'].div(aux_df['purchases_to_n_decline'])
    aux_df['days_p_after_decline'] = aux_df['days_from_n_decline'].div(aux_df['purchases_from_n_decline'])
    aux_df['perc_change'] = ((aux_df['days_p_after_decline'].div(aux_df['days_p_before_decline'])) - 1)
    calc_res.append(aux_df)

# General Table metrics with info from all users
general_resume = []
# Cycle to make the calculations
for idx, df in enumerate(calc_res):
    stats = []
    stats.append(n_declines[idx])
    aux = df[~df['credit_status'].isin(['DELINQUENT', 'CHURNED CREDIT'])]
    aux = aux[aux['purchases_from_n_decline'] > 0]
    aux = aux[aux['days_p_before_decline'] > 0]
    stats.append(round(aux.days_p_before_decline.mean(), 2))
    stats.append(round(aux.days_p_before_decline.std(), 2))
    stats.append(round(aux.days_p_after_decline.mean(), 2))
    stats.append(round(aux.days_p_after_decline.std(), 2))
    stats.append(round(aux.perc_change.mean(), 2))
    stats.append(round(aux.perc_change.std(), 2))
    general_resume.append(stats)
# Create the DataFrame with the information
general_df_stats = pd.DataFrame(general_resume, columns = ['Decline','Avg_days_p_before', 'STD_days_p_before','Avg_days_p_after', 'STD_days_p_after','Avg_perc_change_before', 'Avg_perc_change_after'])
del general_resume, stats, aux
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Cohort metrics
# Cycle to make the calculations
cohort_metrics = []
for idx, df in enumerate(calc_res):
    stats = []
    stats.append(n_declines[idx])
    aux = df[~df['credit_status'].isin(['DELINQUENT', 'CHURNED CREDIT'])]
    aux = aux[aux['purchases_from_n_decline'] > 0]
    aux = aux[aux['days_p_before_decline'] > 0]
    aux = aux[(aux.first_deposit > '2021-11-01') & (aux.first_deposit < '2022-03-01')]
    aux_means = aux.groupby(pd.Grouper(key='first_deposit',freq='M')).aggregate({'days_p_before_decline':'mean', 'days_p_after_decline':'mean',  'perc_change':'mean'})
    aux_means = aux_means.add_prefix('mean_')
    aux_stds = aux.groupby(pd.Grouper(key='first_deposit',freq='M')).aggregate({'days_p_before_decline':'std', 'days_p_after_decline': 'std', 'perc_change': 'std'})
    aux_stds = aux_stds.add_prefix('std_')
    aux_metrics = pd.concat([aux_means,aux_stds], axis=1)
    aux_metrics.index = aux_metrics.index.strftime('%Y-%m')
    aux_metrics = aux_metrics.set_index(n_declines[idx] + '_' +aux_metrics.index.astype(str))
    cohort_metrics.append(aux_metrics)
#General Metrics
cohort_metrics_df = pd.concat(cohort_metrics)

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
churned_general = {}
# Cycle to make the calculations
for idx, df in enumerate(calc_res):
    metrics = []
    aux = df[~df['credit_status'].isin(['DELINQUENT', 'CHURNED CREDIT'])]
    num_users = aux.shape[0]
    churned_users = aux[(aux['purchases_from_n_decline'] == 0) & (aux['days_from_n_decline'] > 30)].shape[0]
    perc = round((churned_users/num_users)*100,2)
    metrics.append(num_users)
    metrics.append(churned_users)
    metrics.append(perc)
    churned_general[n_declines[idx]] = metrics

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
general_purchases = calc_res[0][['user_id', 'one', 'two', 'three', 'four', 'five', 'first_deposit','purchases_from_n_decline', 'credit_status']]
general_purchases = general_purchases[~general_purchases['credit_status'].isin(['DELINQUENT', 'CHURNED CREDIT'])]
general_purchases.rename(columns = {'purchases_from_n_decline':'purchases_from_one_decline'}, inplace = True)

for idx, df in enumerate(calc_res[1:]):
    aux = df[~df['credit_status'].isin(['DELINQUENT', 'CHURNED CREDIT'])]
    aux = aux[['user_id', 'purchases_from_n_decline']]
    aux = aux.rename(columns={'purchases_from_n_decline': 'purchases_from_'+n_declines[idx+1] +'_decline'})
    general_purchases = general_purchases.merge(aux, on = 'user_id', how='left')

columns = ['purchases_from_'+ x +'_decline' for x in n_declines]
purchases_after_declines = general_purchases[['user_id', 'first_deposit'] + columns]

purchases_after_declines['num_declines'] = purchases_after_declines[['purchases_from_one_decline','purchases_from_two_decline', 'purchases_from_three_decline','purchases_from_four_decline', 'purchases_from_five_decline']].notnull().sum(axis=1)
purchases_after_declines['num_0'] = (purchases_after_declines[['purchases_from_one_decline','purchases_from_two_decline', 'purchases_from_three_decline','purchases_from_four_decline', 'purchases_from_five_decline']] == 0).sum(axis=1)
purchases_after_declines['Segment'] = purchases_after_declines.num_declines.astype(str) + ', ' + purchases_after_declines.num_0.astype(str)