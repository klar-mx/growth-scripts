import pandas as pd
from sqlalchemy import create_engine
import time
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import gspread_dataframe as gd
import os
import json

affiliation_list = ['AdAction', 'Destacame', 'Digital Turbine', 'Influencers', 'Klar', 'Leadgenios', 'Lychee', 'Mapendo', 'MiQ', 'Oppizi', 'Snapchat', 'Taboola', 'Liftoff', 'Offline']
row_overall = 615
row_cck = 101
# =============================================================================================================================
# =============================================================================================================================
# ======================================================== DIRECTORIES ========================================================

# Google Sheets API
# Read and Load Credentials
# Google Credentials
credentials = ServiceAccountCredentials.from_json_keyfile_name(os.environ['directory_to_credentials'])
gc = gspread.authorize(credentials)


# =============================================================================================================================
# =============================================================================================================================
# ========================================================= FUNCTIONS =========================================================


def execute_scripts_from_file(filename, connection) -> list:
    """
    Open and read the file as a single buffer and returns a list of Dataframes with all the queries
    :param filename: Name of the file with the queries
    :param connection: Database connection
    :return: List with Dataframes of executed queries
    """
    tables = []
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # All SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            print('Started query')
            start = time.time()
            tables.append(pd.read_sql_query(command, connection))
            end = time.time()
            print('Finished query')
            print("Seconds elapsed: " + str(end - start))
        except psycopg2.OperationalError as msg:
            print("Command skipped: ", msg)
    print("The number of tables is: {}".format(len(tables)))
    return tables


def map_funnel(funnel: list, adjust_info: pd.DataFrame, referrals: pd.DataFrame) -> list:
    """
    Function to map adjust info to each user
    :param referrals:
    :param funnel: List containing the dataframes with user along every step of the funnel
    :param adjust_info: Dataframe with the adjust information
    :return:
    """
    # Create aux list
    aux = []
    # For loop to see each step of the funnel dataframe
    for step in funnel:
        # Name of the step
        step_name = step[0]
        # Dataframe with all the users ids
        step_data = step[1]
        # Add the adjust info
        funnel_step = step_data.merge(adjust_info, how='left', on='user_id')
        # Delete duplicates after the join
        funnel_step = funnel_step.drop_duplicates('user_id')
        # Merge the information from Referral
        funnel_step = funnel_step.merge(referrals, on='user_id', how='left')
        # Combine First Referrals info with adjust
        funnel_step['Mapped_Channels'] = funnel_step.referral.combine_first(funnel_step.Channel)
        # Fill Nans values with Organic
        funnel_step.Mapped_Channels = funnel_step.Mapped_Channels.fillna('Organic')
        # Add the mapped Dataframe to the res list
        aux.append((step_name, funnel_step))
    return aux


def funnel_channel_split(funnel_list: list, affiliation_channels: list) -> list:
    """
    Function to generation daily numbers from each mapped funnel step
    :param funnel_list: List containing each name of the funnel and the dataframe with attribution information
    :param affiliation_channels:  Channels that are part of the affiliation section
    :return: list of channel split dataframes with info of every step
    """
    # Overall Dataframes
    m_funnel_overall = pd.DataFrame()
    d_funnel_overall = pd.DataFrame()
    # CCK Dataframes
    d_cck_overall = pd.DataFrame()
    m_cck_overall = pd.DataFrame()
    # Debit Dataframes
    d_debit_overall = pd.DataFrame()
    m_debit_overall = pd.DataFrame()
    # Presented Dataframes
    m_funnel_overall_presented = pd.DataFrame()
    d_funnel_overall_presented = pd.DataFrame()
    # Presented CCK Dataframes
    d_cck_overall_presented = pd.DataFrame()
    m_cck_overall_presented = pd.DataFrame()
    # Presented Debit Dataframes
    d_debit_overall_presented = pd.DataFrame()
    m_debit_overall_presented = pd.DataFrame()
    # For loop for exploding each step of the funnel
    for step in funnel_list:
        # Separate de step name and the step dataframe
        step_name = step[0]
        step_data = step[1]
        # For loop for two frequencies [monthly and daily]
        for freq_ in ['m', 'd']:
            # Group by frequency and user_id
            step_aux = step_data.groupby([pd.Grouper(key=step_name, freq=freq_), 'Mapped_Channels'])['user_id'].count()
            # Unstack Dataframe at level 0 [Date level]
            step_aux = step_aux.unstack(level=0)
            # Create total column
            total_aux = step_aux.sum()
            # Rename the column
            total_aux.name = 'Total'
            # Convert total_aux to DataFrame
            total_aux = total_aux.to_frame()
            # Add column to aux Dataframe
            step_aux = pd.concat([step_aux, total_aux.transpose()], axis=0, join='outer')
            # Fill NaNs with 0
            step_aux = step_aux.fillna(0)
            # Format column names
            step_aux.columns = [x.strftime('%m/%d/%Y') for x in step_aux.columns]
            # Transpose Dataframe
            step_aux = step_aux.transpose()
            # Create a set of affiliation channels in order to extract the channels present in the data
            affiliation_channels = set(affiliation_channels)
            # Get the actual affiliation channels presented in the data
            affiliation_present_channels = list(affiliation_channels.intersection(set(step_aux.columns)))
            # Create an Affiliation column in order to summarize information
            step_aux['Affiliation'] = step_aux[affiliation_present_channels].sum(axis=1)
            # Create a presented view, the one present in CCK Tracker
            step_aux_presented = step_aux[['Total', 'Organic', 'Facebook', 'Google', 'Referral', 'Affiliation', 'Apple Search', 'TikTok', 'ZoomD', 'Twitter']]
            # Drop Affiliation column
            step_aux = step_aux.drop('Affiliation', axis=1)
            # Logic to put together each Dataframe
            if step_name == 'first_purchase':
                step_aux = step_aux.add_prefix('1P.')
                step_aux_presented = step_aux_presented.add_prefix('1P.')
            elif step_name == 'cck_offers':
                step_aux = step_aux.add_prefix('CCK.')
                step_aux_presented = step_aux_presented.add_prefix('CCK.')
            elif step_name == 'credit_first_p':
                step_aux = step_aux.add_prefix('1P_C')
                step_aux_presented = step_aux_presented.add_prefix('1P_C')
            else:
                step_aux = step_aux.add_prefix(step_name[0] + '.')
                step_aux_presented = step_aux_presented.add_prefix(step_name[0] + '.')
            if freq_ == 'm':
                if step_name in ['credit_first_p', 'cck_offers']:
                    m_cck_overall = pd.concat([m_cck_overall, step_aux], axis=1)
                    m_cck_overall_presented = pd.concat([m_cck_overall_presented, step_aux_presented], axis=1)
                elif step_name in ['debit_fta']:
                    m_debit_overall = pd.concat([m_debit_overall, step_aux], axis=1)
                    m_debit_overall_presented = pd.concat([m_debit_overall_presented, step_aux_presented], axis=1)
                else:
                    m_funnel_overall = pd.concat([m_funnel_overall, step_aux], axis=1)
                    m_funnel_overall_presented = pd.concat([m_funnel_overall_presented, step_aux_presented], axis=1)
            else:
                if step_name in ['credit_first_p', 'cck_offers']:
                    d_cck_overall = pd.concat([d_cck_overall, step_aux], axis=1)
                    d_cck_overall_presented = pd.concat([d_cck_overall_presented, step_aux_presented], axis=1)
                elif step_name in ['debit_fta']:
                    d_debit_overall = pd.concat([d_debit_overall, step_aux], axis=1)
                    d_debit_overall_presented = pd.concat([d_debit_overall_presented, step_aux_presented], axis=1)
                else:
                    d_funnel_overall = pd.concat([d_funnel_overall, step_aux], axis=1)
                    d_funnel_overall_presented = pd.concat([d_funnel_overall_presented, step_aux_presented], axis=1)
    return [m_funnel_overall, m_funnel_overall_presented, m_cck_overall, m_cck_overall_presented, m_debit_overall, m_debit_overall_presented,
            d_funnel_overall, d_funnel_overall_presented, d_cck_overall, d_cck_overall_presented, d_debit_overall, d_debit_overall_presented]


# =============================================================================================================================
# =============================================================================================================================
# cnx = create_engine(postgres_str % quote(postgres_str_pass))
cnx = create_engine(os.environ['POSTGRES_CRED'])
# Execute queries from file
tables = execute_scripts_from_file('CCk_Queries.sql', cnx)
# # Separate tables into dataframes
overall, cck, debit_path, sms_confirmed, fta, debit_fta, first_p, cck_offers, credit_first_p, adjust, referral = tables
# =============================================================================================================================
# Create a list of tuples with name and raw dataframe
funnel = [('sms_confirmed', sms_confirmed), ('fta', fta), ('debit_fta', debit_fta), ('first_purchase', first_p), ('cck_offers', cck_offers), ('credit_first_p', credit_first_p)]
# Overall metrics
funnel_info = [overall, cck, debit_path]
del tables, cnx
# =============================================================================================================================
# ========================================================= REFERRALS =========================================================
# Referrals manipulation
referral['channel'] = "Referral"
referrals_users = referral[['referree_user_id', "channel"]]
referrals_users.columns = ['user_id', 'referral']
# =============================================================================================================================
# ================================================ CHANNEL NAME MANIPULATION ==================================================
# =============================================================================================================================
# Channel names normalization
# Read the channel dictionary
channel_df = pd.read_csv('Channels.csv')
# Create the channel dictionary
channel_dict = channel_df.set_index('Tracker').to_dict()['Channel']
# Map the channel dict into a new column
adjust['Channel'] = adjust['channel'].map(channel_dict)
# Get the new channel
new_channels = adjust[adjust.Channel.isna()]['channel'].drop_duplicates()
if new_channels.shape[0] > 0:
    print("The number of new channels is: " + str(new_channels.shape[0]))
    print(new_channels)
    new_channels.to_csv('New_channels.csv', index=False)
else:
    del new_channels
del channel_df, channel_dict
# =============================================================================================================================
# ===================================================== FUNNEL METRICS ========================================================
# Add Adjust Info to the Funnel
mapped_funnel = map_funnel(funnel, adjust, referrals_users)
# Channel Split
overall_m, overall_m_presented, cck_overall_m, cck_overall_m_presented, debit_overall_m, debit_overall_m_presented, overall_d, overall_d_presented, cck_overall_d, cck_overall_d_presented, debit_overall_d, debit_overall_d_presented = funnel_channel_split(
    mapped_funnel, affiliation_list)
# Add signup info to the funnel info_
new_funnel_info = []
for idx, table in enumerate(funnel_info):
    table.day = pd.to_datetime(table.day)
    table.day = table.day.dt.strftime('%m/%d/%Y')
    table = table.set_index('day')
    new_funnel_info.append(table)

funnel_daily_info, funnel_daily_cck_info, funnel_daily_debit_info = new_funnel_info
# =============================================================================================================================
# Open the whole Google Sheet
cck_tracker = gc.open("[Growth] - CCK Tracker")
# =============================================================================================================================
last_two_mondays = (datetime.date.today() - datetime.timedelta(days=datetime.date.today().weekday()) - datetime.timedelta(days=7)).strftime('%m/%d/%Y')
# =============================================================================================================================
# Overall metrics
gd.set_with_dataframe(cck_tracker.worksheet("Tracker - Overall"), funnel_daily_info.loc[last_two_mondays:, :], row=row_overall, col=6, include_index=False, include_column_header=False)
gd.set_with_dataframe(cck_tracker.worksheet("Tracker - Overall"), overall_d_presented.loc[last_two_mondays:, :], row=row_overall, col=21, include_index=False, include_column_header=False)
# CCK metrics
gd.set_with_dataframe(cck_tracker.worksheet("Tracker - CCK"), funnel_daily_cck_info.loc[last_two_mondays:, :], row=row_cck, col=4, include_index=False, include_column_header=False)
gd.set_with_dataframe(cck_tracker.worksheet("Tracker - CCK"), cck_overall_d_presented.loc[last_two_mondays:, :], row=row_cck, col=13, include_index=False, include_column_header=False)
# Debit Path
gd.set_with_dataframe(cck_tracker.worksheet("Tracker - Debit Path"), funnel_daily_debit_info.loc[last_two_mondays:, :], row=row_cck, col=4, include_index=False, include_column_header=False)
gd.set_with_dataframe(cck_tracker.worksheet("Tracker - Debit Path"), debit_overall_d_presented.loc[last_two_mondays:, :], row=row_cck, col=7, include_index=False, include_column_header=False)
# =============================================================================================================================
# Affiliation Report
gd.set_with_dataframe(cck_tracker.worksheet("Tracker - Channel View"), overall_d, row=2, include_index=True)
