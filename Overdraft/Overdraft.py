import pandas as pd
from sqlalchemy import create_engine
import time
import psycopg2
import matplotlib.pyplot as plt
import numpy as np


def execute_scripts_from_file(filename, cnx) -> list:
    # Open and read the file as a single buffer and returns a list of Dataframes with all the queries
    tables = []
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            print('Started query')
            start = time.time()
            tables.append(pd.read_sql_query(command, cnx))
            end = time.time()
            print('Finished query')
            print("Seconds elapsed: " + str(end - start))
        except psycopg2.OperationalError as msg:
            print("Command skipped: ", msg)

    return tables


# Open DWH connection
f = open('C:\\Users\\gabri\\Documents\\Queries\\db_klarprod_connection.txt', 'r')
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)
# Execute queries
tables = execute_scripts_from_file('Queries.sql', cnx)
# Separate tables into variables
channels, referral, funnel, overdraft, over_trans = tables
del tables, cnx, f, postgres_str
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Referrals manipulation
referral['channel'] = "Referral"
referrals_users = referral[['referree_user_id', "channel"]]
referrals_users.columns = ['user_id', 'referral']
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Adjust table treatment
# Replace unattributed values to Facebook
channels.network_name = channels.network_name.replace({'Unattributed': 'Facebook'})
# Filter the email_confirmed events
email_confirmed = channels[channels.event_name == 'email_confirmed']
# Read the channel dictionary
channel_df = pd.read_excel('C:\\Users\\gabri\PycharmProjects\\Klar\\User_Behavior\\Channels.xlsx', sheet_name='Channels')
# Create the channel dictionary
channel_dict = channel_df.set_index('channel').to_dict()['Section']
# Map the channel dict into a new column
email_confirmed['channel'] = email_confirmed['network_name'].map(channel_dict)
# Get the new channel
new_channels = email_confirmed[email_confirmed.channel.isna()]['network_name'].drop_duplicates()
if new_channels.shape[0] > 0:
    print("The number of new channels is: " + str(new_channels.shape[0]))
    new_channels.to_excel('C:\\Users\\gabri\PycharmProjects\\Klar\\User_Behavior\\New_channels.xlsx', index=False)
# Filter columns to use
adjust_info = email_confirmed[['klaruserid', 'channel']]
# Check for duplicated user_ids
adjust_info = adjust_info.sort_values(by=['klaruserid', 'channel'])
adjust_info = adjust_info.drop_duplicates('klaruserid')
adjust_info.columns = ['user_id', 'channel']

del channel_df, channel_dict, email_confirmed, new_channels
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Filter Funnel Table for adding channel tracking
start_tracking = '2021-10-01'
funnel_by_channel = funnel[funnel.account_created > start_tracking]
del start_tracking
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Create master table
funnel_by_channel = funnel_by_channel.merge(adjust_info, on='user_id', how='left')
# Aggreate referral info
funnel_by_channel = funnel_by_channel.merge(referrals_users, on='user_id', how='left')
# Channel info
funnel_by_channel['Channel'] = funnel_by_channel.referral.combine_first(funnel_by_channel.channel)
# Fill na on Channel column with Organic values
funnel_by_channel['Channel'] = funnel_by_channel['Channel'].fillna('Organic')
# Drop extra tables
funnel_by_channel = funnel_by_channel.drop(['channel', 'referral'], axis=1)
# Add General Channel
funnel_by_channel['General_Channel'] = funnel_by_channel['Channel'].apply(lambda x: 'Affiliates' if x not in ['Facebook', 'Google', 'Organic', 'Referral'] else x)
funnel_by_channel = funnel_by_channel.sort_values(by='account_created')
funnel_by_channel = funnel_by_channel.reset_index(drop=True)
# List of channels
channels = sorted(funnel_by_channel.Channel.unique().tolist())
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# ========================================================================== OVERDRAFT =====================================================================================
# ==========================================================================================================================================================================
overdraft.disbursement_date = pd.to_datetime(overdraft.disbursement_date)
most_recent_disbursed_offer = overdraft.iloc[overdraft.groupby('user_id')['disbursement_date'].agg(pd.Series.idxmax)].sort_values(by='first_accepted_offer_mx')
first_disbursed_offer = overdraft.iloc[overdraft.groupby('user_id')['disbursement_date'].agg(pd.Series.idxmin)].sort_values(by='first_accepted_offer_mx')
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# By channel by month
channel_overdraft_df = funnel_by_channel.merge(first_disbursed_offer, on='user_id')
# Grouped info
channel_overdraft_monthly = channel_overdraft_df.groupby([pd.Grouper(key='fta', freq='M'), 'General_Channel']).user_id.count()
channel_overdraft_monthly = channel_overdraft_monthly.unstack(level=0)
channel_overdraft_monthly.columns = [x.strftime('%Y-%m') for x in channel_overdraft_monthly.columns]
channel_overdraft_monthly.loc['Total'] = channel_overdraft_monthly.sum()
# Deliquent info
channel_overdraft_monthly_deliquent = channel_overdraft_df[channel_overdraft_df.loan_status.isin(['DELINQUENT', 'DEFAULTED'])].groupby([pd.Grouper(key='fta', freq='M'), 'General_Channel']).user_id.count()
channel_overdraft_monthly_deliquent = channel_overdraft_monthly_deliquent.unstack(level=0)
channel_overdraft_monthly_deliquent.columns = [x.strftime('%Y-%m') for x in channel_overdraft_monthly_deliquent.columns]
channel_overdraft_monthly_deliquent.loc['Total'] = channel_overdraft_monthly_deliquent.sum()
# Percentage
channel_overdraft_monthly_deliquent_perc = channel_overdraft_monthly_deliquent.div(channel_overdraft_monthly[channel_overdraft_monthly_deliquent.columns], axis=1)
channel_overdraft_monthly_deliquent_perc = channel_overdraft_monthly_deliquent_perc.applymap("{:.2%}".format)
# Aggregate info
channel_overdraft_monthly_deliquent = channel_overdraft_monthly_deliquent.add_prefix('del_')
channel_overdraft_monthly_deliquent_perc = channel_overdraft_monthly_deliquent_perc.add_prefix('perc_')
channel_overdraft_agg = pd.concat([channel_overdraft_monthly, channel_overdraft_monthly_deliquent,channel_overdraft_monthly_deliquent_perc], axis=1, join="inner")
channel_overdraft_agg = channel_overdraft_agg[list(sum(zip(channel_overdraft_monthly.columns, channel_overdraft_monthly_deliquent.columns,channel_overdraft_monthly_deliquent_perc.columns), ()))]
del channel_overdraft_monthly, channel_overdraft_monthly_deliquent
channel_overdraft_agg.fillna(0, inplace=True)
# ==========================================================================================================================================================================
# Avg time between FTA and first disbursment
channel_overdraft_df['fta_first_overdraft'] = (channel_overdraft_df['first_accepted_offer_mx'] - channel_overdraft_df['fta']).dt.days
# Historic Channel Mean
overdraft_delay_channel = channel_overdraft_df.groupby('General_Channel')['fta_first_overdraft'].mean()
# Historic Channel Mean by month
overdraft_delay_channel_monthly = channel_overdraft_df.groupby([pd.Grouper(key='fta',freq='M'),'General_Channel'])['fta_first_overdraft'].mean()
overdraft_delay_channel_monthly = overdraft_delay_channel_monthly.unstack(level=0)
overdraft_delay_channel_monthly.columns = [x.strftime('%Y-%m') for x in overdraft_delay_channel_monthly.columns]
# ==========================================================================================================================================================================
# ====================================================================== DELINQUENT ANALYSIS ===============================================================================
# ==========================================================================================================================================================================
delinquents_overdraft_df = channel_overdraft_df[channel_overdraft_df.loan_status.isin(['DELINQUENT', 'DEFAULTED'])]
# Historic delinquent Mean
delinquent_delay_monthly = delinquents_overdraft_df.groupby(pd.Grouper(key='fta', freq='M'))['fta_first_overdraft'].mean()
# Historic Delinquent by Channel Mean by month
delinquent_delay_channel_monthly = delinquents_overdraft_df.groupby([pd.Grouper(key='fta',freq='M'),'General_Channel'])['fta_first_overdraft'].mean()
delinquent_delay_channel_monthly = delinquent_delay_channel_monthly.unstack(level=0)
delinquent_delay_channel_monthly.columns = [x.strftime('%Y-%m') for x in delinquent_delay_channel_monthly.columns]
# ==========================================================================================================================================================================
# Difference between hard deliquent and casual delinquent
delinquents_overdraft_df['delinquent_type'] = delinquents_overdraft_df.apply(lambda row: 'Hard' if row.fta_first_overdraft < 5 and row.balance > 0 else 'Casual', axis=1)
# Delinquent type by month
delinquency_type_monthly = delinquents_overdraft_df.groupby([pd.Grouper(key='fta', freq='M'),'delinquent_type']).user_id.count()
delinquency_type_monthly = delinquency_type_monthly.unstack(level=0)
delinquency_type_monthly.columns = [x.strftime('%Y-%m') for x in delinquency_type_monthly.columns]
# ==========================================================================================================================================================================
# Overdraft debt amount by month
amount_debt_monthly_sum = delinquents_overdraft_df.groupby(pd.Grouper(key='fta', freq='M')).balance.sum()
amount_debt_monthly_sum = pd.DataFrame(amount_debt_monthly_sum).transpose()
amount_debt_monthly_sum.index = ['Amount_Total']
amount_debt_monthly_sum.columns = [x.strftime('%Y-%m') for x in amount_debt_monthly_sum.columns]
# Monthly debt average
amount_debt_monthly_avg = delinquents_overdraft_df[delinquents_overdraft_df.balance>0].groupby(pd.Grouper(key='fta', freq='M')).balance.mean()
amount_debt_monthly_avg = pd.DataFrame(amount_debt_monthly_avg).transpose()
amount_debt_monthly_avg.index = ['Avg_Amount']
amount_debt_monthly_avg.columns = [x.strftime('%Y-%m') for x in amount_debt_monthly_avg.columns]
# Agg Monthly debt
amount_debt_monthly = pd.concat([amount_debt_monthly_sum,amount_debt_monthly_avg],axis=0)
del amount_debt_monthly_sum,amount_debt_monthly_avg
# ==========================================================================================================================================================================
# ====================================================================== DELINQUENT GENERAL ================================================================================
# ==========================================================================================================================================================================
# Overdraft by channel
# Most recent overdraft status by channel
channel_recent_overdraft = funnel_by_channel.merge(most_recent_disbursed_offer, on='user_id')
# Overdraft usage by channel
# Ftas by month
ftas_monthly_channel = funnel_by_channel.groupby([pd.Grouper(key='fta', freq='M'),'General_Channel']).user_id.count()
ftas_monthly_channel = ftas_monthly_channel.unstack(level=0)
ftas_monthly_channel.columns = [x.strftime('%Y-%m') for x in ftas_monthly_channel.columns]
ftas_monthly_channel = ftas_monthly_channel.loc[:,'2021-10':]
ftas_monthly_channel.loc['Total'] = ftas_monthly_channel.sum()
# Current overdraft channel status
actual_overdraft_channel = channel_recent_overdraft.groupby([pd.Grouper(key='fta', freq='M'), 'General_Channel']).user_id.count()
actual_overdraft_channel = actual_overdraft_channel.unstack(level=0)
actual_overdraft_channel.columns = [x.strftime('%Y-%m') for x in actual_overdraft_channel.columns]
actual_overdraft_channel = actual_overdraft_channel.loc[:,'2021-10':]
actual_overdraft_channel.loc['Total'] = actual_overdraft_channel.sum()
# Current perc
channel_perc_overdraft_monthly = actual_overdraft_channel.div(ftas_monthly_channel.loc[actual_overdraft_channel.index])
channel_perc_overdraft_monthly = channel_perc_overdraft_monthly.applymap("{:.2%}".format)
# ==========================================================================================================================================================================
# Ftas by month
ftas_monthly = funnel.groupby(pd.Grouper(key='fta', freq='M')).user_id.count()
ftas_monthly.index = [x.strftime('%Y-%m') for x in ftas_monthly.index]
# First overdraft info
general_overdraft_df = funnel.merge(first_disbursed_offer, on='user_id')
# Overdraft users by month
overdraft_users_monthly = general_overdraft_df.groupby(pd.Grouper(key='fta', freq='M')).user_id.count()
overdraft_users_monthly.index = [x.strftime('%Y-%m') for x in overdraft_users_monthly.index]
# Delinquent by month
delinquents_monthly = general_overdraft_df[general_overdraft_df.loan_status.isin(['DELINQUENT', 'DEFAULTED'])].groupby(pd.Grouper(key='fta', freq='M')).user_id.count()
delinquents_monthly.index = [x.strftime('%Y-%m') for x in delinquents_monthly.index]
# Overdraft users percentage by month
overdraft_users_perc_monthly = overdraft_users_monthly.div(ftas_monthly.loc[overdraft_users_monthly.index])
overdraft_users_perc_monthly = overdraft_users_perc_monthly.map("{:.2%}".format)
# Delinquent percentage by month
delinquent_perc_monthly = delinquents_monthly.div(overdraft_users_monthly.loc[delinquents_monthly.index])
delinquent_perc_monthly = delinquent_perc_monthly.map("{:.2%}".format)
# Overall Metrics
overdraft_overall = pd.concat([ftas_monthly, overdraft_users_monthly, overdraft_users_perc_monthly, delinquents_monthly, delinquent_perc_monthly],axis=1,join="inner")
del ftas_monthly, overdraft_users_monthly, overdraft_users_perc_monthly, delinquents_monthly, delinquent_perc_monthly
overdraft_overall.columns = ['FTAs', 'Overdraft_Users', 'Overdraft/FTAs', 'Overdraft_Del', 'Del/Users']
