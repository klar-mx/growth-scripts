import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# DB Information
db_credentials = open('/Users/gabrielreynoso/Documents/Queries/db_klarprod_connection.txt', 'r')
# =============================================================================================================================
# =============================================================================================================================
# Read file
postgres_str = db_credentials.read()
# Close file
db_credentials.close()
cnx = create_engine(postgres_str)


# =============================================================================================================================
# Read list of initial Users
initial_state = pd.read_csv('./Data/Debit_Users_WithCCK.csv')
initial_state.columns = ['user_id', 'email', 'initial_state', 'first_cck_timestamp_mx']
initial_state['first_state'] = 'debit'

# Query for actual state
actual_state_query = '''
select klar_user_id as user_id,
       o.current_state
from is_customer_io.segments as c
left join is_onboarding_service.onboarding_data as o on o.user_id = c.klar_user_id
where segment_id = 1586
'''

actual_state = pd.read_sql_query(actual_state_query,cnx)
actual_state.columns = ['user_id', 'current_state']
actual_state['actual_state'] = actual_state.current_state.apply(lambda x: 'debit' if x in ['DEBIT_ONLY_USER', 'NO_DEPOSIT'] else 'out_debit')
# =============================================================================================================================
# EDA
state = pd.merge(initial_state, actual_state, on='user_id')
# Detailed Movement
state['detailed_movement'] = state['initial_state'] + ' -> ' + state['current_state']
# Movement
state['movement'] = state['first_state'] + ' -> ' + state['actual_state']
# Numbers
print(state.groupby('movement')['user_id'].count())
print(state.groupby('detailed_movement')['user_id'].count())