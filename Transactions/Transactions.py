import pandas as pd
from openpyxl import load_workbook
from sqlalchemy import create_engine
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta, MO
import numpy as np
import matplotlib.pyplot as plt


f = open('C:\\Users\\gabri\\Documents\\Queries\\db_klarprod_connection.txt', 'r')
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)

months = [['2021-11-01','2021-11-30','november'],['2021-12-01','2021-12-31','december'], ['2022-01-01','2022-01-31','January']]

queryNumTrans = '''
with transactions_by_user as (select t.user_id,
count(t.transaction_id) as num_transactions
from analytics_bi.transactions as t
where t.type = 'PURCHASE' and timestamp_mx_created_at between '{}'::date and '{}'::date
group by user_id)
select transactions_by_user.num_transactions,
       count (transactions_by_user.user_id) as count 
from transactions_by_user
group by transactions_by_user.num_transactions
order by 1
'''

queryPurchaseAmount = '''
select round(t.amount,0) as amount,
count(t.transaction_id) as num_transactions
from analytics_bi.transactions as t
where t.type = 'PURCHASE' and timestamp_mx_created_at between '{}'::date and '{}'::date
group by round(t.amount,0)
order by amount desc
'''

queryUserPurchaseVolume = '''
with transactions_by_user as (
    select t.user_id,
    round(sum(t.amount),0) as month_amount
    from analytics_bi.transactions as t
    where t.type = 'PURCHASE' and timestamp_mx_created_at between '{}'::date and '{}'::date
    group by user_id)
select transactions_by_user.month_amount as amount,
       count (transactions_by_user.user_id) as count
from transactions_by_user
where month_amount <0
group by transactions_by_user.month_amount
order by 1 desc;
'''

transByMonth = []
purchasesByMonth = []
userVolumeByMonth = []

for month in months:
    transByMonth.append(pd.read_sql_query(queryNumTrans.format(month[0], month[1]), cnx).rename(columns={'count': month[2]}))
    purchasesByMonth.append(pd.read_sql_query(queryPurchaseAmount.format(month[0], month[1]), cnx).rename(columns={'num_transactions': month[2]}))
    userVolumeByMonth.append(pd.read_sql_query(queryUserPurchaseVolume.format(month[0], month[1]), cnx).rename(columns={'count': month[2]}))

df_trans = transByMonth[0]
df_purch = purchasesByMonth[0]
df_userVol = userVolumeByMonth[0]

for table in transByMonth[1:]:
    df_trans = df_trans.merge(table, on='num_transactions',how='outer')
    df_trans = df_trans.fillna(0)

for table in purchasesByMonth[1:]:
    df_purch = df_purch.merge(table, on='amount',how='outer')
    df_purch = df_purch.fillna(0)

for table in userVolumeByMonth[1:]:
    df_userVol = df_userVol.merge(table, on='amount',how='outer')
    df_userVol = df_userVol.fillna(0)

df_trans = df_trans.sort_values(by='num_transactions')
df_purch = df_purch.sort_values(by='amount', ascending=False)
df_userVol = df_userVol.sort_values(by='amount', ascending=False)