import pandas as pd
from os import listdir
from sqlalchemy import create_engine
from tqdm import tqdm
import datetime

# BD Connection
f = open("C:\\Users\\gabri\\Documents\\Queries\\db_klarprod_connection.txt", "r")
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)

# Directory with the info
full_dir = 'C:\\Users\\gabri\\PycharmProjects\\Klar\\RFM\\CRM_Analytics\\Campaigns\\Data\\'
campaign = 'May_ChurnCampaign\\'
date_extracted = '2022-06-10'

# List of all the data available
user_cohort_data = []
name_data = []
for dir in listdir(full_dir + campaign):
    aux_df = pd.read_csv(full_dir + campaign + dir)
    col = [x for x in aux_df.columns if 'user' in x.lower()]
    user_cohort_data.append(aux_df[col])
    name_data.append(dir.split('.')[0])
del dir

# Get new data for new cluster
query_rfm = '''
        select rfm_layer.user_id,
               max_user.max_date as date,
               rfm_layer.cluster,
               rfm_layer.time,
               rfm_layer.monetary,
               rfm_layer.frequency,
               rfm_layer.recency
        from growth.rfm_history as rfm_layer,
             (select user_id,
                     max(date) as max_date
             from growth.rfm_history
             where date < '{}'::date
             group by user_id) max_user
        where rfm_layer.user_id = max_user.user_id
        and rfm_layer.date = max_user.max_date
        and rfm_layer.user_id in {}
'''

# List with the new data
data = []
after_30_days = []
after_60_days = []

# Run de queries for the new data
for df in tqdm(user_cohort_data):
    # List of users
    list_user_ids = df.iloc[:, 0].to_list()
    # Extracted date plus 30 days
    date_extracted_plus30 = (pd.to_datetime(date_extracted) + datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    # Extracted date plus 60 days
    date_extracted_plus60 = (pd.to_datetime(date_extracted) + datetime.timedelta(days=60)).strftime('%Y-%m-%d')
    # Run query
    aux_df = pd.read_sql_query(query_rfm.format(date_extracted, tuple(list_user_ids)), cnx)
    # Run query
    aux_df_30 = pd.read_sql_query(query_rfm.format(date_extracted_plus30, tuple(list_user_ids)), cnx)
    # Run query
    aux_df_60 = pd.read_sql_query(query_rfm.format(date_extracted_plus60, tuple(list_user_ids)), cnx)
    # Data at the beginning of extraction
    data.append(aux_df)
    # Add the new info the list after 30 days
    after_30_days.append(aux_df_30)
    # Add the new info the list after 60 days
    after_60_days.append(aux_df_60)

# Complete table
complete_data = []

for idx, df in enumerate(data):
    aux = df[['user_id', 'date', 'cluster', 'time', 'monetary', 'frequency', 'recency']]
    # Add after 30 days information
    aux_30 = after_30_days[idx]
    aux_30 = aux_30.add_suffix('_30')
    aux = aux.merge(aux_30, left_on='user_id', right_on='user_id_30')
    # Add after 60 days information
    aux_60 = after_60_days[idx]
    aux_60 = aux_60.add_suffix('_60')
    aux = aux.merge(aux_60, left_on='user_id', right_on='user_id_60')
    # Append data to list
    complete_data.append(aux)

# Cluster dictionary
cluster_names = pd.read_excel("C:\\Users\\gabri\\Documents\\RFM_LayerA_Dic.xlsx")
# Create the channel dictionary
cluster_dict = cluster_names.set_index('id').to_dict()['name']

for idx, df in enumerate(complete_data):
    for col in ['cluster', 'cluster_30', 'cluster_60']:
        # Map the channel dict into a new column
        df[col] = df[col].map(cluster_dict)
    df['movement_30'] = df['cluster'] + '->' + df['cluster_30']
    df['movement_60'] = df['cluster_30'] + '->' + df['cluster_60']
    complete_data[idx] = df
del idx,df

# Calc results
metrics = []
for df in complete_data:
    aux_metrics = []
    # First results
    aux_metrics.append(df.groupby('cluster').agg({'user_id':"count", 'time':"mean", 'monetary': "mean", 'frequency': "mean", 'recency': "mean"}))
    # After 30 days
    aux_metrics.append(df.groupby('cluster_30').agg({'user_id_30': "count", 'time_30': "mean", 'monetary_30': "mean", 'frequency_30': "mean", 'recency_30': "mean"}))
    # After 60 days
    aux_metrics.append(df.groupby('cluster_60').agg({'user_id_60': "count", 'time_60': "mean", 'monetary_60': "mean", 'frequency_60': "mean", 'recency_60': "mean"}))
    # Append the metrics
    metrics.append(aux_metrics)

# Export results
writer = pd.ExcelWriter('C:\\Users\\gabri\\PycharmProjects\\Klar\\RFM\\CRM_Analytics\\Campaigns\\Results\\'+campaign.split('\\')[0] + '_results.xlsx', engine='xlsxwriter')

for idx, metric_list in enumerate(metrics):
    row_count = 0
    for j, table in enumerate(metric_list):
        if j == 0:
            if len(name_data[idx].split('Import_ ')) > 1:
                table.to_excel(writer, sheet_name=name_data[idx].split('Import_ ')[1].replace(" ",""))
            else:
                table.to_excel(writer, sheet_name=name_data[idx].split('Import_ ')[0].replace(" ", ""))
            row_count = row_count + table.shape[0]
        else:
            if len(name_data[idx].split('Import_ ')) > 1:
                table.to_excel(writer, sheet_name=name_data[idx].split('Import_ ')[1].replace(" ",""), startrow = row_count + 4)
            else:
                table.to_excel(writer, sheet_name=name_data[idx].split('Import_ ')[0].replace(" ", ""), startrow=row_count + 4)
            row_count = row_count + table.shape[0] + 4

writer.save()