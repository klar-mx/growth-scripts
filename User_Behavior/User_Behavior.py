import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import time
from openpyxl import load_workbook
import seaborn as sns
import matplotlib.pyplot as plt


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
            print("Seconds elapsed: " + str(end-start))
        except psycopg2.OperationalError as msg:
            print("Command skipped: ", msg)

    return tables


# def combine_year_month(df: pd.DataFrame) -> pd.DataFrame:
#     df['month_date'] = df['year'] + '-' + df['month']
#     df = df.drop(['year','month'], axis=1)
#     return df


# Query engine creation
f = open('C:\\Users\\gabri\\Documents\\Queries\\db_klarprod_connection.txt', 'r')
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)
# Execute queries from file
# tables = execute_scripts_from_file('Queries_Filter.sql', cnx, '2021-10-01','2021-11-01')
tables = execute_scripts_from_file('Queries.sql', cnx)
# # Separate tables into dataframes
balance, deposits_transfers, rewards, purchase, creditKlayuda, crediKlar, funnel, channel, referrals, delinquent_Klayuda, delinquent_crediKlar, retention = tables
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Referrals manipulation
referrals['channel'] = "Referral"
referrals_users = referrals[['referree_user_id', "channel"]]
referrals_users.columns = ['user_id', 'referral']
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Adjust table treatment
# Replace unattributed values to Facebook
channel.network_name = channel.network_name.replace({'Unattributed': 'Facebook'})
# Filter the email_confirmed events
email_confirmed = channel[channel.event_name == 'email_confirmed']
# Read the channel dictionary
channel_df = pd.read_excel('Channels.xlsx', sheet_name='Channels')
# Create the channel dictionary
channel_dict = channel_df.set_index('channel').to_dict()['Section']
# Map the channel dict into a new column
email_confirmed['channel'] = email_confirmed['network_name'].map(channel_dict)
# Get the new channel
new_channels = email_confirmed[email_confirmed.channel.isna()]['network_name'].drop_duplicates()
if new_channels.shape[0] > 0:
    print("The number of new channels is: " + str(new_channels.shape[0]))
    new_channels.to_excel('New_channels.xlsx', index=False)
# Filter columns to use
adjust_info = email_confirmed[['klaruserid','channel']]
del email_confirmed
# Check for duplicated user_ids
adjust_info = adjust_info.sort_values(by=['klaruserid', 'channel'])
adjust_info = adjust_info.drop_duplicates('klaruserid')
adjust_info.columns = ['user_id', 'channel']
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Credit reconciliation
# Deliquent users
delinquent_crediKlar.deliquent = 'Delinquent'
deliquent = pd.merge(delinquent_Klayuda, delinquent_Klayuda, how='outer', on='user_id')
deliquent['deliquent'] = deliquent.deliquent_x.combine_first(deliquent.deliquent_y)
deliquent.drop(['deliquent_x','deliquent_y'], axis=1, inplace=True)
# Credit information
crediKlar = crediKlar.add_prefix('crediKlar_')
crediKlar.rename(columns = {'crediKlar_user_id':'user_id'},inplace=True)
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Filter Funnel Table
start_date = '2021-10-01'
end_date = '2021-11-01'
cohort_analysis = funnel[(funnel.cuenta > start_date) & (funnel.cuenta<end_date)]
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Create master table
master_table = cohort_analysis.merge(adjust_info, on='user_id', how='left')
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
master_table = master_table[['email', 'user_id', 'General_Channel', 'Channel', 'cuenta', 'fta', 'p1', 'p10']]
master_table = master_table.sort_values(by='fta')
master_table = master_table.reset_index(drop=True)
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Add balance information
master_table = master_table.merge(balance, on='user_id', how='left')
# Add transactions information
master_table = master_table.merge(deposits_transfers, on='user_id', how='left')
# Add transactions information
master_table = master_table.merge(rewards, on='user_id', how='left')
# Add purchases information
master_table = master_table.merge(purchase, on='user_id', how='left')
# Add credit information Klayuda
master_table = master_table.merge(creditKlayuda, on='user_id', how='left')
# Add credit information CrediKlar
master_table = master_table.merge(crediKlar, on='user_id', how='left')
# Add deliquent
master_table = master_table.merge(deliquent, on='user_id', how='left')
# Add retention
master_table = master_table.merge(retention, on='user_id', how='left')
# See master_table size
print(master_table.shape)
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
aggregation_dict = {'cuenta': 'count', 'fta': 'count', 'p1': 'count', 'p10': 'count', 'days30_avg_balance': 'mean', 'days60_avg_balance': 'mean', 'days90_avg_balance': 'mean','days30_count_trans': 'sum', 'days60_count_trans': 'sum',
                    'days90_count_trans': 'sum','days30_count_reward': 'sum', 'days60_count_reward': 'sum', 'days90_count_reward': 'sum','days30_vol_purch': 'sum', 'days30_count_purch': 'sum', 'days60_vol_purch': 'sum','days60_count_purch': 'sum',
                    'days90_vol_purch': 'sum', 'days90_count_purch': 'sum', 'retention_30_days': 'sum', 'retention_60_days': 'sum', 'retention_90_days': 'sum', 'deliquent': 'count', 'days30_num_offers': 'sum', 'days30_accepted': 'sum',
                    'amount30_days': 'sum', 'days60_num_offers': 'sum', 'days60_accepted': 'sum','amount60_days': 'sum', 'days90_num_offers': 'sum', 'days90_accepted': 'sum', 'amount90_days': 'sum', 'crediKlar_days30_num_offers': 'sum',
                    'crediKlar_days30_accepted': 'sum', 'crediKlar_amount30_days': 'sum', 'crediKlar_days60_num_offers': 'sum', 'crediKlar_days60_accepted': 'sum', 'crediKlar_amount60_days': 'sum', 'crediKlar_days90_num_offers': 'sum',
                    'crediKlar_days90_accepted': 'sum', 'crediKlar_amount90_days': 'sum'}
# Detail resume
detail_resume = master_table.groupby(['Channel']).agg(aggregation_dict)
# Complete resume
resume = master_table.groupby(['General_Channel']).agg(aggregation_dict)
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# Export
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('C:\\Users\\gabri\\PycharmProjects\\Klar\\User_Behavior\\Exports\\60days_October.xlsx', engine='xlsxwriter')
detail_resume.to_excel(writer,sheet_name='Detailed')
resume.to_excel(writer,sheet_name='General')
writer.save()