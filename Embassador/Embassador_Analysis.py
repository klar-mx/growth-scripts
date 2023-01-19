import pandas as pd
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt

# Read Cohort info
# cohort = pd.read_csv('CustomerCohortEmbassador.csv')

# Results of the survey
results = pd.read_excel('BrandAmbassadorTesting.xlsx', sheet_name='Main DB')

results.columns = ['Submission Date', 'source', 'uuid', 'Simpatia', 'Confianza', 'Respeto', 'Accesible', 'Comunidad', 'Otras Opciones', 'Submission ID']

results = results[~results['uuid'].isna()]

otros = results['Otras Opciones'].value_counts().reset_index()

results[['Simpatia', 'Confianza', 'Respeto', 'Accesible', 'Comunidad']] = results[['Simpatia', 'Confianza', 'Respeto', 'Accesible', 'Comunidad']].apply(
    lambda x: x.apply(lambda y: y.split('\n') if not pd.isna(y) else y))

id_list = results.uuid.to_list()
id_tuple = tuple(id_list)

query = '''
WITH kyc_user AS(
SELECT
    id,
    DATEDIFF(hour,birth_date,GETDATE())/8766 AS age,
    gender,
    to_char (created, 'YYYY-MM') AS month_completed_signup
FROM db_kyc_public_user_kyc
),

kyc_address AS (
SELECT 
    id,
    user_id,
    state,
    city,
    postal_code
FROM db_kyc_public_user_kyc_address
),

funnel_status AS (
SELECT 
    user_id,
    CASE WHEN tenth_purchase IS NOT NULL then 4
        WHEN first_purchase is NOT NULL then 3
        WHEN first_deposit is NOT NULL then 2
        WHEN account_created is NOT NULL then 1
        ELSE 0 
        END as funnel_status
FROM klar.dim_funnel_v2
),

credit_status AS (
SELECT
user_id,
    CASE WHEN user_status = 'Delinquent' then 0
        WHEN user_status = 'New' then 2
        WHEN user_status = 'Returning' then 1
        WHEN user_status = 'Extension' then 1
        WHEN user_status = 'Renewal' then 1
        WHEN user_status = 'Late' then 1
        WHEN user_status = 'Frozen' then 1
        WHEN user_status = 'Churned' then 1
        ELSE 3 
        END as credit_status
from credit_ops.credit_user
),

credit_offer AS (
SELECT 
   distinct(user_id) AS credit_eligible
FROM db_loans_credit_batch_import_renewal
)

SELECT
    ku.id,
    ku.age,
    ku.gender,
    ku.month_completed_signup,
    ka.state,
    ka.city,
    ka.postal_code,
    fs.funnel_status,
    cs.credit_status,
    co.credit_eligible
FROM kyc_user ku
LEFT JOIN kyc_address ka ON ku.id = ka.user_id
LEFT JOIN funnel_status fs ON ku.id = fs.user_id
LEFT JOIN credit_status cs ON ku.id = cs.user_id
LEFT JOIN credit_offer co ON ku.id = co.credit_eligible
WHERE ku.id IN {}
'''.format(id_tuple)

POSTGRES_ADDRESS = 'live-main.c56d9jndqmv2.us-east-2.redshift.amazonaws.com'
POSTGRES_PORT = '5439'
POSTGRES_USERNAME = 'gabriel_reynoso'
POSTGRES_PASSWORD = 'qwh233-23$NSGMQ_QnqQ1A*^S'
POSTGRES_DBNAME = 'klarprod'
# A long string that contains the necessary Postgres login information
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME,
                                                                                        password=POSTGRES_PASSWORD,
                                                                                        ipaddress=POSTGRES_ADDRESS,
                                                                                        port=POSTGRES_PORT,
                                                                                        dbname=POSTGRES_DBNAME))
# Create the connection
cnx = create_engine(postgres_str)
del POSTGRES_ADDRESS, POSTGRES_PORT, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_DBNAME

users = pd.read_sql_query(query, con=cnx)

results_info = pd.merge(results, users, how='left', left_on= 'uuid',right_on='id')
del results, users

simpatia = results_info.drop(columns=['Confianza', 'Respeto', 'Accesible', 'Comunidad'])
confianza = results_info.drop(columns=['Simpatia', 'Respeto', 'Accesible', 'Comunidad'])
respeto = results_info.drop(columns=['Simpatia', 'Confianza', 'Accesible', 'Comunidad'])
accesible = results_info.drop(columns=['Simpatia', 'Confianza', 'Respeto', 'Comunidad'])
comunidad = results_info.drop(columns=['Simpatia', 'Confianza', 'Respeto', 'Accesible'])

simpatia = simpatia.explode('Simpatia')
confianza = confianza.explode('Confianza')
respeto = respeto.explode('Respeto')
accesible = accesible.explode('Accesible')
comunidad = comunidad.explode('Comunidad')

# General
data = [simpatia.Simpatia.value_counts().head(15),
confianza.Confianza.value_counts().head(15),
respeto.Respeto.value_counts().head(15),
accesible.Accesible.value_counts().head(15),
comunidad.Comunidad.value_counts().head(15)]

general_df = pd.concat(data, axis=1)

fig, ax = plt.subplots(figsize=general_df.shape)
sns.heatmap(general_df, cmap="Blues", vmin=general_df.min().min(), vmax=general_df.max().max(), linewidth=0.3, cbar_kws={"shrink": .8})
plt.title('General distribution preferences')
plt.show()

# Male
data_male = [simpatia[simpatia['gender']=='MALE'].Simpatia.value_counts().head(15),
confianza[confianza['gender']=='MALE'].Confianza.value_counts().head(15),
respeto[respeto['gender']=='MALE'].Respeto.value_counts().head(15),
accesible[accesible['gender']=='MALE'].Accesible.value_counts().head(15),
comunidad[comunidad['gender']=='MALE'].Comunidad.value_counts().head(15)]

male_df = pd.concat(data_male, axis=1)
male_df = male_df.reindex(general_df.index)

fig, ax = plt.subplots(figsize=general_df.shape)
sns.heatmap(male_df, cmap="Blues", vmin=male_df.min().min(), vmax=male_df.max().max(), linewidth=0.3, cbar_kws={"shrink": .8})
plt.title('Male distribution preferences')
plt.show()

# Female
data_female = [simpatia[simpatia['gender']=='FEMALE'].Simpatia.value_counts().head(15),
confianza[confianza['gender']=='FEMALE'].Confianza.value_counts().head(15),
respeto[respeto['gender']=='FEMALE'].Respeto.value_counts().head(15),
accesible[accesible['gender']=='FEMALE'].Accesible.value_counts().head(15),
comunidad[comunidad['gender']=='FEMALE'].Comunidad.value_counts().head(15)]

female_df = pd.concat(data_female, axis=1)
female_df = female_df.reindex(general_df.index)

fig, ax = plt.subplots(figsize=general_df.shape)
sns.heatmap(female_df, cmap="Blues", vmin=female_df.min().min(), vmax=female_df.max().max(), linewidth=0.3, cbar_kws={"shrink": .8})
plt.title('Female distribution preferences')
plt.show()

# ===================================================================================================================================================================
# ============================================================================== AGE ================================================================================
# ===================================================================================================================================================================

# Age Histogram
results_info.age.hist()
plt.title('Age Distribution')
plt.show()

# Between 18 and 30
data_until30 = [simpatia[(simpatia['age']>=18) & (simpatia['age']<30)].Simpatia.value_counts().head(15),
confianza[(confianza['age']>=18) & (confianza['age']<30)].Confianza.value_counts().head(15),
respeto[(respeto['age']>=18) & (respeto['age']<30)].Respeto.value_counts().head(15),
accesible[(accesible['age']>=18) & (accesible['age']<30)].Accesible.value_counts().head(15),
comunidad[(comunidad['age']>=18) &(comunidad['age']<30)].Comunidad.value_counts().head(15)]

until30_df = pd.concat(data_until30, axis=1)
until30_df = until30_df.reindex(general_df.index)

fig, ax = plt.subplots(figsize=until30_df.shape)
sns.heatmap(until30_df, cmap="Blues", vmin=until30_df.min().min(), vmax=until30_df.max().max(), linewidth=0.3, cbar_kws={"shrink": .8})
plt.title('Age 18-30 distribution preferences')
plt.show()

# Between 30 and 50
data_until50 = [simpatia[(simpatia['age']>=30) & (simpatia['age']<50)].Simpatia.value_counts().head(15),
confianza[(confianza['age']>=30) & (confianza['age']<50)].Confianza.value_counts().head(15),
respeto[(respeto['age']>=30) & (respeto['age']<50)].Respeto.value_counts().head(15),
accesible[(accesible['age']>=30) & (accesible['age']<50)].Accesible.value_counts().head(15),
comunidad[(comunidad['age']>=30) &(comunidad['age']<50)].Comunidad.value_counts().head(15)]

until50_df = pd.concat(data_until50, axis=1)
until50_df = until50_df.reindex(general_df.index)

fig, ax = plt.subplots(figsize=until50_df.shape)
sns.heatmap(until50_df, cmap="Blues", vmin=until50_df.min().min(), vmax=until50_df.max().max(), linewidth=0.3, cbar_kws={"shrink": .8})
plt.title('Age 30-50 distribution preferences')
plt.show()

