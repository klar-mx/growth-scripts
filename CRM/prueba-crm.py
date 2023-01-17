#Import libraries and connect to DWH
import pandas as pd
import sheet_dev as sd
import importlib
importlib.reload(sd)
import psycopg2
import warnings
import time
from customer_io import Customer_io
warnings.filterwarnings('ignore', '.*pandas only support SQLAlchemy connectable.*')

#Conectarse al DWH no olviden poner su usuario y contraseña de redshift
conn = psycopg2.connect(
                        host='live-main.c56d9jndqmv2.us-east-2.redshift.amazonaws.com',
                        port=5439,
                        database='klarprod',
                        user='usuario_redshift',
                        password='pass_redshift'
                       )

#Definimos query, recordar que columna de email tiene que llamarse "email", y todas las demás columnas serán atributos
query = """with salary_advance_data as (
select user_id, days_past_due
from credit.salary_advance_loanbook
where loan_status = 'DELINQUENT'
)
select  kyc.email, sa.days_past_due as adelanto_days_past_due
from salary_advance_data sa
left join klar_pii.db_kyc_public_user_kyc kyc on sa.user_id=kyc.id
where adelanto_days_past_due<=7"""

#Línea para guardar el query en un dataframe df
df = pd.read_sql(query, conn)

#Cambiar formato para números enteros en texto (string)
df['adelanto_days_past_due'] = df['adelanto_days_past_due'].map(str)

print(df)

#Cerrar conexión porque están limitadas (máx dependiendo de lo que asigna data)
conn.close()

segment = 917
attributes = ['adelanto_days_past_due']

cio = Customer_io()
if len(df) > 0:
    old_users = cio.get_users_segment(segment)
    print(old_users)
    if len(old_users) > 0:
        cio.del_users_segment(segment, old_users)
#    time.sleep(70)
    cio.add_users_segment(segment, df, attributes)

    time.sleep(4)
    new_users = cio.get_users_segment(segment)
    print(len(new_users))
    if len(new_users) != len(df):
        print('Not all users are in the campaign, please verify')
else:
    print("No campaign")