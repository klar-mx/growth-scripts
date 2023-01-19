import time
import pandas as pd
from os import listdir
from sqlalchemy import create_engine
from tqdm import tqdm
import datetime

dates = ['2022-08-25', '2022-09-26']
# Cluster dictionary
cluster_names = pd.read_excel("C:\\Users\\gabri\\OneDrive\\Documentos\\RFM_LayerA_Dic.xlsx")
# Create the channel dictionary
cluster_dict = cluster_names.set_index('id').to_dict()['name']
# Write the output
# Export results
writer = pd.ExcelWriter('./Results/August_ChurnCampaign_Results.xlsx', engine='xlsxwriter')
# ======================================================================================================================================================================
# ======================================================================================================================================================================
# Read the Excel File with the user_ids
churnedExcelFileMonth = pd.ExcelFile('./Data/August_ChurnCampaign/RFM_August.xlsx')
segment_sheets = churnedExcelFileMonth.sheet_names[:3]
segment_names = ['Churned', 'At_Risk_S1', 'At_Risk_S2']
# ======================================================================================================================================================================
# ======================================================================================================================================================================
# BD Connection
f = open("C:\\Users\\gabri\\OneDrive\\Documentos\\Queries\\db_klarprod_connection.txt", "r")
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)
# ======================================================================================================================================================================
# ======================================================================================================================================================================
# RFM Query for data on a specific date
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
# Purchases for the users in the specific date
query_p = '''
        select
               t.user_id,
               count(transaction_id) as purchases,
                -1*sum(amount) as total_amount
        from analytics_bi.transactions as t
        where t.type in ('PURCHASE')
          and t.timestamp_mx_created_at between '2022-08-25' and  '2022-09-26'
          and t.state = 'SETTLED'
          and t.source_account_internal_id <> '0000000000000000'
          and t.source_account_internal_id <> '00000000-0000-0000-0000-000000000000'
          and t.provider_id <> 'KLAR'
          and t.user_id in {}
        group by t.user_id
        
'''
# ======================================================================================================================================================================
# ======================================================================================================================================================================
# For loop for extracting cohort information
cohorts = {}
cohorts_results = {}
cohorts_movement = {}
for idx, x in enumerate(segment_names):
    # Get cohort info
    aux = pd.read_excel(churnedExcelFileMonth, sheet_name=segment_sheets[idx])
    aux = aux[~aux.klarUserId.isna()][['klarUserId', 'Segment']]
    # Dataframe and results dicts
    segments_dict = {}
    segments_results = {}
    segments_movement = {}
    # Print the cohort name
    print(x)
    start_time = time.time()
    overall_treatment = pd.DataFrame()
    i = 0
    for segment in tqdm(aux.Segment.unique()):
        print(segment)
        # Filter segment
        aux_segment = aux[aux.Segment == segment]
        # Start of the campaign
        aux_start_campaign = pd.read_sql_query(query_rfm.format(dates[0], tuple(aux_segment.klarUserId.to_list())), cnx)
        aux_start_campaign = aux_start_campaign.set_index('user_id')
        # Map cluster
        aux_start_campaign['cluster'] = aux_start_campaign['cluster'].map(cluster_dict)
        aux_start_campaign = aux_start_campaign.add_prefix('Pre_')
        # End of the campaign
        aux_end_campaign = pd.read_sql_query(query_rfm.format(dates[1], tuple(aux_segment.klarUserId.to_list())), cnx)
        aux_end_campaign = aux_end_campaign.set_index('user_id')
        # Map cluster
        aux_end_campaign['cluster'] = aux_end_campaign['cluster'].map(cluster_dict)
        aux_end_campaign = aux_end_campaign.add_prefix('Post_')
        # Merge Pre and Post campaign
        aux_result = pd.merge(aux_start_campaign, aux_end_campaign, left_index=True, right_index=True)
        aux_result = aux_result.reset_index()
        # Purchases
        aux_purchases = pd.read_sql_query(query_p.format(tuple(aux_segment.klarUserId.to_list())), cnx)
        # Add Purchases to result info
        aux_result = pd.merge(aux_result, aux_purchases, on='user_id', how='left')
        aux_result['movement'] = aux_result['Pre_cluster'] + '->' + aux_result['Post_cluster']
        # Add to dict of segments cohorts
        segments_dict[segment] = aux_result
        segments_results[segment] = aux_result.groupby('movement')[['purchases','total_amount','Pre_monetary','Pre_frequency', 'Pre_recency','Post_monetary', 'Post_frequency', 'Post_recency']].describe()
        segments_movement[segment] = pd.concat([aux_result.movement.value_counts(), aux_result.movement.value_counts(normalize=True).mul(100)],axis=1, keys=('counts','percentage'))
        # If not control then append to general treatment dataframe
        if segment != 'Control':
            overall_treatment = overall_treatment.append(aux_result)
        # Write the results into an excel
        segments_results[segment].to_excel(writer, startrow=i+3, startcol=0, sheet_name=x)
        segments_movement[segment].to_excel(writer, startrow=i+7+segments_results[segment].shape[0], startcol=0, sheet_name=x)
        worksheet = writer.sheets[x]
        worksheet.write(i, 0, segment)
        i = i+9+segments_results[segment].shape[0] + segments_movement[segment].shape[0]
    print(str(time.time()-start_time) + ' seconds')
    segments_dict['Overall_Treatment'] = overall_treatment
    segments_results['Overall_Treatment'] = overall_treatment.groupby('movement')[['purchases','total_amount','Pre_monetary','Pre_frequency', 'Pre_recency','Post_monetary', 'Post_frequency', 'Post_recency']].describe()
    segments_movement['Overall_Treatment'] = pd.concat([overall_treatment.movement.value_counts(), overall_treatment.movement.value_counts(normalize=True).mul(100)], axis=1, keys=('counts', 'percentage'))
    # Print the overall results table into excel
    segments_results['Overall_Treatment'].to_excel(writer, startrow=i+3, startcol=0, sheet_name=x)
    segments_movement['Overall_Treatment'].to_excel(writer, startrow=i+7+segments_results['Overall_Treatment'].shape[0], startcol=0, sheet_name=x)
    worksheet = writer.sheets[x]
    worksheet.write(i, 0, 'Overall_Treatment')
    # Add the segment dict to the cohorts dict
    cohorts[x] = segments_dict
    cohorts_results[x] = segments_results
writer.save()