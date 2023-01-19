import pandas as pd
from sqlalchemy import create_engine
import time
import psycopg2


def execute_scripts_from_file(filename, connection) -> list:
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
            tables.append(pd.read_sql_query(command, connection))
            end = time.time()
            print('Finished query')
            print("Seconds elapsed: " + str(end - start))
        except psycopg2.OperationalError as msg:
            print("Command skipped: ", msg)
    print("The number of tables is: {}".format(len(tables)))
    return tables


def map_funnel(funnel: list, adjust_info: pd.DataFrame) -> list:
    aux = []
    for step in funnel:
        step_name = step[0]
        step_data = step[1]
        funnel_step = step_data.merge(adjust_info, how='left', on='user_id')
        funnel_step = funnel_step.drop_duplicates('user_id')
        funnel_step = funnel_step.merge(referrals_users, on='user_id', how='left')
        funnel_step['Mapped_Channels'] = funnel_step.referral.combine_first(funnel_step.Channel)
        funnel_step.Mapped_Channels = funnel_step.Mapped_Channels.fillna('Organic')
        aux.append((step_name, funnel_step))
    return aux


def funnel_channel_split(funnel_list: list) -> list:
    month_funnel_channel_split = pd.DataFrame()
    daily_funnel_channel_split = pd.DataFrame()
    month_funnel_channel_split_presented = pd.DataFrame()
    daily_funnel_channel_split_presented = pd.DataFrame()
    for step in funnel_list:
        step_name = step[0]
        step_data = step[1]
        for freq_ in ['m', 'd']:
            # Group by frequency and user_id
            step_aux = step_data.groupby([pd.Grouper(key=step_name, freq=freq_), 'Mapped_Channels'])['user_id'].count()
            step_aux = step_aux.unstack(level=0)
            total_aux = step_aux.sum()
            total_aux.name = 'Total'
            step_aux = step_aux.append(total_aux)
            step_aux = step_aux.fillna(0)
            step_aux.columns = [x.strftime('%m/%d/%Y') for x in step_aux.columns]
            step_aux = step_aux.transpose()
            step_aux['Affiliation'] = step_aux[['AdAction', 'Destacame', 'Digital Turbine', 'Influencers', 'Klar', 'Leadgenios', 'Lychee', 'Mapeando', 'MiQ', 'Oppizi', 'Snapchat', 'Taboola', 'TikTok']].sum(axis=1)
            step_aux_presented = step_aux[['Total', 'Organic', 'Facebook', 'Google', 'Referral', 'Affiliation', 'Apple Search', 'Liftoff', 'ZoomD']]
            if step_name == 'first_purchase':
                step_aux = step_aux.add_prefix('1P.')
                step_aux_presented = step_aux_presented.add_prefix('1P.')
            else:
                step_aux = step_aux.add_prefix(step_name[0] + '.')
                step_aux_presented = step_aux_presented.add_prefix(step_name[0] + '.')
            if freq_ == 'm':
                month_funnel_channel_split = pd.concat([month_funnel_channel_split, step_aux], axis=1)
                month_funnel_channel_split_presented = pd.concat([month_funnel_channel_split_presented, step_aux_presented], axis=1)
            else:
                daily_funnel_channel_split = pd.concat([daily_funnel_channel_split, step_aux], axis=1)
                daily_funnel_channel_split_presented = pd.concat([daily_funnel_channel_split_presented, step_aux_presented], axis=1)
    return [month_funnel_channel_split, daily_funnel_channel_split, daily_funnel_channel_split, daily_funnel_channel_split_presented]


def calc_step_delay_dist(df: pd.DataFrame, month_count: pd.DataFrame, list_months: list, funnel_step_name: str) -> pd.DataFrame:
    # Create the result dataframe
    res = pd.DataFrame(columns=['Days'])
    # Process each cohort
    for month in list_months:
        # Filter the month
        aux_cohort = df[df.cohort == month]
        # Filter the step
        aux = aux_cohort[funnel_step_name]
        # Value count the delay days
        aux_dist = aux.value_counts()
        # Reset index and sort by it
        aux_dist = aux_dist.reset_index()
        aux_dist = aux_dist.sort_values(by='index')
        # Month_Distr
        aux_dist['Distr'] = aux_dist[funnel_step_name].div(month_count.loc[month, 'user_id'])
        # Format columns
        aux_dist.columns = ['Days', funnel_step_name + '_' + month, funnel_step_name + '_' + month + '_distr']
        # Add cohort to general table
        res = res.merge(aux_dist, on='Days', how='outer')
        # Fill with 0 missing delays
        res = res.sort_values(by='Days')
    res = res.fillna(0)
    return res


# Read the database credentials and create database connection
f = open('C:\\Users\\gabri\\Documents\\Queries\\db_klarprod_connection.txt', 'r')
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)
# Execute queries from file
# tables = execute_scripts_from_file('Queries_Filter.sql', cnx, '2021-10-01','2021-11-01')
tables = execute_scripts_from_file('Queries_Input.sql', cnx)
signup, sms, adjust, funnel_table, referral, referral_cohorts = tables
channel_signup = signup[signup.signup >= '2021-10-01']
channel_sms = sms[sms.sms >= '2021-10-01']
funnel = [('signup', channel_signup), ('sms', channel_sms)]
del tables, f, cnx, postgres_str
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------ REFERRALS -------------------------------------------------------------------------------------------------------------------------
# Referrals manipulation
referral['channel'] = "Referral"
referrals_users = referral[['referree_user_id', "channel"]]
referrals_users.columns = ['user_id', 'referral']
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------ CHANNEL NAME MANIPULATION ---------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Channel names normalization
# Read the channel dictionary
channel_df = pd.read_excel('C:\\Users\\gabri\PycharmProjects\\Klar\\User_Behavior\\Channels.xlsx', sheet_name='Channels')
# Create the channel dictionary
channel_dict = channel_df.set_index('channel').to_dict()['Section']
# Map the channel dict into a new column
adjust['Channel'] = adjust['channel'].map(channel_dict)
# Get the new channel
new_channels = adjust[adjust.Channel.isna()]['Channel'].drop_duplicates()
if new_channels.shape[0] > 0:
    print("The number of new channels is: " + str(new_channels.shape[0]))
    new_channels.to_excel('C:\\Users\\gabri\PycharmProjects\\Klar\\User_Behavior\\New_channels.xlsx', index=False)
else:
    del new_channels
del channel_df, channel_dict
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------ FUNNEL METRICS --------------------------------------------------------------------------------------------------------------------------
# Add Adjust Info to the Funnel
mapped_funnel = map_funnel(funnel, adjust)
channel_split_month, channel_split_daily, channel_split_month_presented, channel_split_daily_presented = funnel_channel_split(mapped_funnel)
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------ FUNNEL METRICS --------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Get weekday name
funnel_table['week_day'] = funnel_table.account_created.dt.day_name()

funnel_steps = ['completed_web_signup', 'sms_confirmed', 'fta']
# Days in the funnel_table
for idx, step in enumerate(['signup_sms', 'sms_fta']):
    funnel_table[step] = funnel_table.apply(lambda row: pd.NA if pd.isna(row[funnel_steps[idx]]) or pd.isna(row[funnel_steps[idx + 1]]) else (row[funnel_steps[idx + 1]] - row[funnel_steps[idx]]).days, axis=1)
del idx, step, funnel_steps
# Create the cohort etiquettes
funnel_table["cohort_account"] = funnel_table.account_created.dt.strftime('%Y-%m')
# Correct info account - fta
funnel_table.loc[funnel_table["signup_sms"] < 0, "sms_confirmed"] = funnel_table.loc[funnel_table["signup_sms"] < 0, "completed_web_signup"]
funnel_table.loc[funnel_table["signup_sms"] < 0, "signup_sms"] = 0
# Correct info fta - 1P
funnel_table.loc[funnel_table["sms_fta"] < 0, "fta"] = funnel_table.loc[funnel_table["sms_fta"] < 0, "sms_confirmed"]
funnel_table.loc[funnel_table["sms_fta"] < 0, "sms_fta"] = 0
# ==========================================================================================================================================================================
# ================================================================== Sign up to sms confirm ================================================================================
# Filter date for tables
start_date = '2021-07-01'
# ==========================================================================================================================================================================
# Include the people who already confirmed their sms and started their signup after July 2021
signup_funnel = funnel_table[~funnel_table.sms_confirmed.isna()]
signup_funnel = signup_funnel[signup_funnel.started_web_signup > start_date]
# Number of started signups by month
started_signup_monthly = signup[signup.signup > start_date].groupby(pd.Grouper(freq='M', key='signup')).count()
started_signup_monthly.index = started_signup_monthly.index.strftime('%Y-%m')
# Number of sms_confirmed by month
sms_confirmed_monthly = sms[sms.sms > start_date].groupby(pd.Grouper(freq='M', key='sms')).count()
sms_confirmed_monthly.index = sms_confirmed_monthly.index.strftime('%Y-%m')
# ==========================================================================================================================================================================
# Signup to sms_confirmed delay dist
# Create the column signup_funnel in order to group values
signup_funnel["cohort"] = signup_funnel.started_web_signup.dt.strftime('%Y-%m')
# Create list with the months to form the cohorts
cohorts = sorted(signup_funnel.cohort.unique().tolist())
# Get the distribution
signup_sms_dist = calc_step_delay_dist(signup_funnel, started_signup_monthly, cohorts, 'signup_sms')
# ==========================================================================================================================================================================
# Include the people who already made FTA and confirmed sms July 2021
sms_funnel = funnel_table[~funnel_table.fta.isna()]
sms_funnel = sms_funnel[sms_funnel.sms_confirmed > start_date]
# Sms_confirmed to FTA delay dist
# Create the column signup_funnel in order to group values
sms_funnel["cohort"] = sms_funnel.sms_confirmed.dt.strftime('%Y-%m')
# Create list with the months to form the cohorts
cohorts = sorted(sms_funnel.cohort.unique().tolist())
# Get the distribution
sms_fta_dist = calc_step_delay_dist(sms_funnel, sms_confirmed_monthly, cohorts, 'sms_fta')
del start_date, cohorts
# Concretate table
step_funnel_dist = pd.merge(signup_sms_dist, sms_fta_dist, on='Days', how='outer')
# Set Days to index
step_funnel_dist = step_funnel_dist.set_index('Days')
# ==========================================================================================================================================================================
# ==========================================================================================================================================================================
# ======================================================================= Referral info ====================================================================================
# ==========================================================================================================================================================================
# Filter the referral cohort table
referral_cohorts = referral_cohorts[referral_cohorts.month_cohort >= '2021-07-01']
# Convert cohort month to string
referral_cohorts.month_cohort = referral_cohorts.month_cohort.dt.strftime('%Y-%m')
# Cohorts for Referral
cohorts_ref = sorted(referral_cohorts.month_cohort.unique().tolist())
# Referral table
referree_delay = calc_step_delay_dist(referral_cohorts, sms_confirmed_monthly, cohorts_ref, 'days')