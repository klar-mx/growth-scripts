import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

files = [x for x in os.listdir('/Users/gabrielreynoso/Downloads') if 'xlsx' in x]
month = (datetime.today() + relativedelta(months=-1)).strftime('%Y-%m')

results = pd.DataFrame()

for file in files:
    aux = pd.read_excel('/Users/gabrielreynoso/Downloads/' + file)
    aux.columns = aux.iloc[0]
    aux = aux.drop(aux.index[0])
    aux.index = aux.iloc[:, 0]
    aux = aux.drop(aux.columns.to_list()[0], axis=1)
    aux_index = [pd.to_datetime(x).strftime('%Y-%m') for x in aux.index.to_list() if x not in ['Total general']]
    aux_index.append('Total general')
    aux.index = aux_index
    if 'Total general' in aux.columns.to_list():
        total_flag = True
    else:
        total_flag = False
    aux = aux.drop(['Total general', datetime.today().strftime('%Y-%m')])
    aux_columns = [pd.to_datetime(x).strftime('%Y-%m') for x in aux.columns.to_list() if x not in ['Total general']]
    if total_flag:
        aux_columns.append('Total general')
        aux.columns = aux_columns
        aux = aux.drop(columns=['Total general'])
    else:
        aux.columns = aux_columns
    col_aux = aux[[month]]
    migrated_value = col_aux.iloc[:15].sum().to_list()[0]
    list_aux = col_aux.iloc[15:][month].values.tolist()
    list_aux.insert(0, migrated_value)
    list_index = col_aux.iloc[15:].index.to_list()
    list_index.insert(0, 'Migrated')
    df_aux = pd.DataFrame(data=list_aux, index=list_index, columns=[file.split('.')[0]])
    if results.shape[0] == 0:
        results = df_aux.copy()
    else:
        results = pd.merge(results, df_aux, left_index=True, right_index=True)

results.to_excel('./Data/' + month + '.xlsx', engine='xlsxwriter')