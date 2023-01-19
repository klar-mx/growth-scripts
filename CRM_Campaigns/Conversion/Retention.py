import pandas as pd
import os
from sqlalchemy import create_engine
import sqlalchemy
from datetime import datetime
import numpy as np
from tqdm import tqdm
import time
import psycopg2

month_list = ['October2021', 'November2021', 'December2021', 'January2022', 'February2022', 'March2022', 'April2022', 'May2022', 'June2022', 'July2022']

# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# Redshift connection
f = open('C:\\Users\\gabri\\OneDrive\\Documentos\\Queries\\db_klarprod_connection.txt', 'r')
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# Read campaign dates
campaign_dates = pd.read_csv('Data/Conversion_campaign_dates.csv', parse_dates=['Initial_date', 'End_date'], dayfirst=True)
# Rewrite campaign dates
campaign_dates.Campaign = campaign_dates.apply(lambda row: row.Campaign.split('Conversion ')[1] + row.Initial_date.strftime('%Y'), axis=1)
campaign_dates = campaign_dates.set_index('Campaign')
# ========================================================================================================================================
# Number of days since the end of the campaign
campaign_dates['days_after_campaign'] = campaign_dates.End_date.apply(lambda x: (datetime.now() - x).days)
campaign_dates['intervals'] = campaign_dates['days_after_campaign'].apply(lambda x: [30] if x < 30 else np.arange(start=30, stop=((x // 30) + 1) * 30, step=30))
# ========================================================================================================================================
# Read db pickle
db_users_conv = pd.read_pickle('./Data/DB_Conversion.pkl')
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# Query for retention
query_retention = '''
with users as (
    select funnel.user_id
    from klar.dim_funnel_v3 as funnel
    where funnel.user_id in {}
),
active_flag as (
    select user_id,
           count(distinct transaction_id) as num_transactions
    from analytics_bi.transactions as tx
    where tx.type in ('PURCHASE', 'FEE', 'DISBURSEMENT', 'TRANSFER', 'DEPOSIT', 'QUASI_CASH')
        and tx.state = 'SETTLED'
        and tx.source_account_internal_id <> '0000000000000000'
        and tx.source_account_internal_id <> '00000000-0000-0000-0000-000000000000'
        and tx.provider_id <> 'KLAR'
        and tx.timestamp_mx_created_at BETWEEN dateadd(day,{},'{}'::date) AND dateadd(day,{},'{}'::date)
    group by user_id
)
select users.user_id,
       active_flag.num_transactions,
       case when active_flag.num_transactions notnull then 1 else 0 end as active_flag
from users
left join active_flag on active_flag.user_id = users.user_id
'''
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
res = []
start_time = time.time()
# First for : 1. All users in the experiment. 2. Users who received the incentive
for i in range(2):
    # Dictionary with results:
    month_dict = {}
    # Filter de Payments
    if i == 1:
        aux = db_users_conv[~db_users_conv.Payment.isna()]
    else:
        aux = db_users_conv.copy()
    # Second for. Filtering for each month
    for month in tqdm(db_users_conv.month.value_counts().index):
        # Printing information
        print("\n" + month)
        # Read the monthly campaign end date
        finish_date = campaign_dates.loc[month, 'End_date']
        # 30 days periods to run de query
        interval = campaign_dates.loc[month, 'intervals']
        # Filter by month
        aux_month = aux[aux.month == month]
        # = = = = = = = = = = = = = = = = = = = = = = = = = = =
        # Segment results
        segment_dict = {}
        # Third for. Filtering by Principal Segment
        for segment in tqdm(aux_month.principal_segment.value_counts().index):
            # Printing information
            print("\n" + segment)
            # Filter segment
            aux_segment = aux_month[aux_month.principal_segment == segment]
            # = = = = = = = = = = = = = = = = = = = = = = = = = = =
            # Intervals results
            interval_dict = {}
            # Fourth for. Retention by cohorts of 30 days
            for ret_days in interval:
                # Print interval
                print("\n" + str(ret_days))
                # Get the list of users for query
                users_list = aux_segment[aux_segment.user_id.notnull()].user_id.to_list()
                # If the number of users is too big, partition the query
                if len(users_list) > 100000:
                    # Number of steps for the query
                    steps = len(users_list) // 100000
                    # Result query
                    results_query = pd.DataFrame()
                    for j in range(steps + 1):
                        # = = = = = = = = = = = = = = = = = = = = = = = = = = =
                        # Print step of the query
                        print("\n" + "Step: " + str(j))
                        # Partition the list
                        users = tuple(users_list[100000 * j:100000 * (j + 1)])
                        # Add the users and end of date of the campaign to the query
                        query = query_retention.format(users, ret_days - 30, finish_date.strftime('%Y-%m-%d'), ret_days, finish_date.strftime('%Y-%m-%d'))
                        # Execute the query
                        results_query = pd.concat([results_query, pd.read_sql_query(sqlalchemy.text(query), cnx)])
                else:
                    # Get the list of users for query
                    users = tuple(users_list)
                    # Add the users and end of date of the campaign to the query
                    query = query_retention.format(users, ret_days - 30, finish_date.strftime('%Y-%m-%d'), ret_days, finish_date.strftime('%Y-%m-%d'))
                    # Execute the query
                    results_query = pd.read_sql_query(sqlalchemy.text(query), cnx)
                # = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
                retention = results_query.active_flag.sum() / results_query.user_id.count()
                # Add retention for the interval to the interval dict
                interval_dict[ret_days] = retention
            # = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
            # Add the interval_dict to the segment dict
            segment_dict[segment] = interval_dict
        # = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        # Add the segment_dict to the month dict
        month_dict[month] = segment_dict
    # = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    # Add the month_dict to the overall list
    res.append(month_dict)

print("Time needed: " + str((time.time() - start_time) / 60))
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# Generate tables for outputs
# General cohorts
all_users = res[0]
monthly_users = []
# Payment users
payment_users = res[1]
month_payment_users = []
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
for month in all_users.keys():
    monthly_users.append((month, pd.DataFrame(all_users[month])))
    month_payment_users.append((month, pd.DataFrame(payment_users[month])))
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
ret_overall = pd.DataFrame()
# = = = = = = = = = = = = = = = = =
for df in monthly_users:
    ret_overall = pd.merge(ret_overall,pd.DataFrame(data=df[1].mean(axis=1), columns=[df[0]]), how='outer',left_index=True, right_index=True)
# Reorder the table
ret_overall = ret_overall[month_list]
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
ret_payment = pd.DataFrame()
# = = = = = = = = = = = = = = = = =
for df in month_payment_users:
    ret_payment = pd.merge(ret_payment,pd.DataFrame(data=df[1].mean(axis=1), columns=[df[0]]), how='outer',left_index=True, right_index=True)
# Reorder the table
ret_payment = ret_payment[month_list]
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
monthly_users_campaigns = []
month_payment_users_campaigns = []
for segment in ['CSU no 1T nor 1P', 'CSU + 1T no 1P', 'No CSU']:
    # Segments for overall users
    segment_retention = pd.DataFrame()
    segment_paid_retention = pd.DataFrame()
    for idx in range(len(monthly_users)):
        if segment in monthly_users[idx][1].columns.to_list():
            segment_retention = pd.merge(segment_retention, pd.DataFrame(data=monthly_users[idx][1][segment].to_frame(name=monthly_users[idx][0])), how='outer', left_index=True, right_index=True)
    # Segments for paid users
        if segment in month_payment_users[idx][1].columns.to_list():
            segment_paid_retention = pd.merge(segment_paid_retention, pd.DataFrame(data=month_payment_users[idx][1][segment].to_frame(name=month_payment_users[idx][0])), how='outer', left_index=True, right_index=True)
    # Overall campaign users
    monthly_users_campaigns.append((segment, segment_retention[[x for x in month_list if x in segment_retention.columns.to_list()]]))
    # Paid campaign users
    month_payment_users_campaigns.append((segment,segment_paid_retention[[x for x in month_list if x in segment_paid_retention.columns.to_list()]]))
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
