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

# Get weekday name
funnel['week_day'] = funnel.account_created.dt.day_name()

funnel_steps_first = ['account_created', 'fta', 'first_purchase', 'tenth_purchase']
# Days in the funnel
for idx, step in enumerate(['account_fta', 'fta_1P', '1P_10P']):
    funnel[step] = funnel.apply(lambda row: pd.NA if pd.isna(row[funnel_steps_first[idx]]) or pd.isna(row[funnel_steps_first[idx + 1]]) else (row[funnel_steps_first[idx + 1]] - row[funnel_steps_first[idx]]).days, axis=1)
del idx, step, funnel_steps_first
# Compared to FTA
funnel_steps_fta = ['completed_web_signup', 'sms_confirmed', 'fta']

for idx, step in enumerate(['signup_sms', 'sms_fta']):
    funnel[step] = funnel.apply(lambda row: pd.NA if pd.isna(row[funnel_steps_fta[idx]]) or pd.isna(row[funnel_steps_fta[idx + 1]]) else (row[funnel_steps_fta[idx + 1]] - row[funnel_steps_fta[idx]]).days, axis=1)
del idx, step, funnel_steps_fta
# Create the cohort etiquettes
funnel["cohort_account"] = funnel.account_created.dt.strftime('%Y-%m')
# Correct info account - fta
funnel.loc[funnel["account_fta"] < 0, "fta"] = funnel.loc[funnel["account_fta"] < 0, "account_created"]
funnel.loc[funnel["account_fta"] < 0, "account_fta"] = 0
# Correct info fta - 1P
funnel.loc[funnel["fta_1P"] < 0, "first_purchase"] = funnel.loc[funnel["fta_1P"] < 0, "fta"]
funnel.loc[funnel["fta_1P"] < 0, "fta_1P"] = 0
# Correct info account - fta
funnel.loc[funnel["signup_sms"] < 0, "sms_confirmed"] = funnel.loc[funnel["signup_sms"] < 0, "completed_web_signup"]
funnel.loc[funnel["signup_sms"] < 0, "signup_sms"] = 0
# Correct info fta - 1P
funnel.loc[funnel["sms_fta"] < 0, "fta"] = funnel.loc[funnel["sms_fta"] < 0, "sms_confirmed"]
funnel.loc[funnel["sms_fta"] < 0, "sms_fta"] = 0
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Create master table
master_table = funnel.merge(adjust_info, on='user_id', how='left')
# Aggreate referral info
master_table = master_table.merge(referrals_users, on='user_id', how='left')
# Channel info
master_table['Channel'] = master_table.referral.combine_first(master_table.channel)
# Fill na on Channel column with Organic values
master_table['Channel'] = master_table['Channel'].fillna('Organic')
# Drop extra tables
master_table = master_table.drop(['channel', 'referral'], axis=1)
# Add General Channel
master_table['General_Channel'] = master_table['Channel'].apply(lambda x: 'Affiliates' if x not in ['Facebook', 'Google', 'Organic', 'Referral'] else x)
master_table = master_table.sort_values(by='account_created')
master_table = master_table.reset_index(drop=True)
# List of the cohorts
cohorts = sorted(master_table.cohort_account.unique(dropna=True).tolist())
# List of channels
channels = sorted(master_table.Channel.unique().tolist())
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Filter Funnel Table
start_date = '2021-07-01'
cohort_analysis = master_table[~master_table.account_created.isna()]
cohort_analysis = cohort_analysis[cohort_analysis.account_created > start_date]
del start_date
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Historic Channel distribution
channel_dists = pd.DataFrame(columns=['Days'])
for channel in channels:
    aux = cohort_analysis[cohort_analysis.Channel == channel]
    aux_dist = aux.account_fta.value_counts()
    aux_dist = aux_dist.reset_index()
    aux_dist = aux_dist.sort_values(by='index')
    aux_dist['cpf'] = aux_dist.account_fta.cumsum() / aux_dist.account_fta.sum()
    aux_dist.columns = ['Days', channel, channel + '_cpf']
    channel_dists = channel_dists.merge(aux_dist, on='Days', how='outer')
del channel, aux, aux_dist
channel_dists = channel_dists.sort_values(by='Days')
# Channel modification
for channel in channels:
    channel_dists.loc[channel_dists[channel].isna(), channel] = 0
    col = channel + '_cpf'
    channel_dists[col] = channel_dists[col].fillna(method='ffill')
del col, channel
# Export
fta_delay_channel = pd.ExcelWriter('Channel_FTA_Delay.xlsx', engine='xlsxwriter')
channel_dists.to_excel(fta_delay_channel, 'Historic_Channel_Delay', index=False)
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Channel distribution by month
for channel in channels:
    aux = cohort_analysis[cohort_analysis.Channel == channel]
    hist_channel = pd.DataFrame(columns=['Days'])
    cohorts = sorted(aux.cohort_account.unique().tolist())
    for cohort in cohorts:
        aux_cohort = aux[aux.cohort_account == cohort]
        aux_dist = aux_cohort.account_fta.value_counts()
        aux_dist = aux_dist.reset_index()
        aux_dist = aux_dist.sort_values(by='index')
        aux_dist['cpf'] = aux_dist.account_fta.cumsum() / aux_dist.account_fta.sum()
        aux_dist.columns = ['Days', cohort, cohort + '_cpf']
        hist_channel = hist_channel.merge(aux_dist, on='Days', how='outer')
        hist_channel.loc[hist_channel[cohort].isna(), cohort] = 0
        hist_channel = hist_channel.sort_values(by='Days')
        col = cohort + '_cpf'
        hist_channel[col] = hist_channel[col].fillna(method='ffill')
    hist_channel.to_excel(fta_delay_channel, channel, index=False)
fta_delay_channel.save()
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Fta_distribution by month
hist_account = pd.DataFrame(columns=['Days'])
cohorts = sorted(cohort_analysis.cohort_account.unique().tolist())
for cohort in cohorts:
    aux_cohort = cohort_analysis[cohort_analysis.cohort_account == cohort]
    aux_dist = aux_cohort.account_fta.value_counts()
    aux_dist = aux_dist.reset_index()
    aux_dist = aux_dist.sort_values(by='index')
    aux_dist['cpf'] = aux_dist.account_fta.cumsum() / aux_dist.account_fta.sum()
    aux_dist.columns = ['Days', cohort, cohort + '_cpf']
    hist_account = hist_account.merge(aux_dist, on='Days', how='outer')
    hist_account.loc[hist_account[cohort].isna(), cohort] = 0
    hist_account = hist_account.sort_values(by='Days')
    col = cohort + '_cpf'
    hist_account[col] = hist_account[col].fillna(method='ffill')
hist_account.to_excel('Monthly_Account_Fta_Dist.xlsx', index=False)
# ==========================================================================================================================================================================
# ============================================================== DAILY FUNNEL CONVERSION BY CHANNEL ========================================================================
# Filter Funnel Table
daily_conv = pd.DataFrame(columns=['Day'])
adjust_date = '2021-10-01'
for step in ['started_web_signup', 'completed_web_signup','account_created', 'sms_confirmed', 'fta', 'first_purchase']:
    funnel_step = master_table[~master_table[step].isna()]
    funnel_step = funnel_step[funnel_step[step] > adjust_date]
    step_count = funnel_step.groupby([pd.Grouper(key=step,freq='d'), 'General_Channel'])['user_id'].count()
    step_count = step_count.unstack(level=1)
    step_columns = step_count.columns
    new_step_columns = [step + '.' + x for x in step_columns]
    step_count.columns = new_step_columns
    step_count = step_count.reset_index()
    step_count.rename({step: 'Day'}, axis=1, inplace=True)
    daily_conv = daily_conv.merge(step_count, on='Day', how='outer')
del adjust_date
daily_conv.Day = daily_conv.Day.dt.strftime('%m/%d/%Y')
