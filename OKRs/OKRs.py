import pandas as pd
from dateutil.relativedelta import relativedelta
import datetime
from tqdm import tqdm
from sqlalchemy import create_engine
import sqlalchemy

# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# Redshift connection
f = open('/Users/gabrielreynoso/Documents/Queries/db_klarprod_connection.txt', 'r')
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# Number of months to get the data
start_date = datetime.date(2020, 12, 1)
months = relativedelta(datetime.datetime.today().replace(day=1), start_date).months + relativedelta(datetime.datetime.today().replace(day=1), start_date).years * 12
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# = - = - = - = DATAFRAMES = - = - = - =
# =====================
# REFERRAL VIRALITY
# =====================
referral_virality = pd.DataFrame(columns=['referral_virality', 'referrees', 'sms_confirmed', 'date'])
# =====================
# MAUS
# =====================
mau_60 = pd.DataFrame(columns=['starting_date', 'end_date','active_users'])
mau_retained = pd.DataFrame(columns=['starting_date', 'end_date', 'active_retained_users'])
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# Queries
# =====================
# REFERRAL VIRALITY
# =====================
referral_virality_query = '''
with referees_count as (
    select
        sum(case when DATEDIFF(days,f.sms_confirmed_mx::date, r.referree_completed_signup_date::date) < 8 then 1 else 0 end) as referrees,
        current_date as date
    from growth.referral_info as r
    join (
        select user_id,
               sms_confirmed_mx
        from klar.cck_funnel
        where sms_confirmed_mx notnull
        and sms_confirmed_mx between '{start_date}' and '{end_date}'
    ) as f on f.user_id = r.referrer_user_id
),
    sms_confirmed_count as (
        select
            count(user_id) as sms_confirmed,
            current_date as date
        from klar.cck_funnel
        where sms_confirmed_mx notnull
          and sms_confirmed_mx between '{start_date}' and '{end_date}'
    )
select
    r.referrees*1.0/s.sms_confirmed as referral_virality,
    r.referrees as referrees,
    s.sms_confirmed as sms_confirmed,
    r.date
from referees_count as r
join sms_confirmed_count as s on s.date = r.date
'''
# =====================
# MAUS
# =====================
mau_60_query = '''
select
    '{start_date}' as starting_date,
    '{end_date}' as end_date,
    count(distinct t.user_id) as active_users
from analytics_bi.transactions as t
where timestamp_mx_created_at between '{start_date}' and '{end_date}'
'''
mau_retained_query = '''
select
    '{start_date}' as starting_date,
    '{end_date}' as end_date,
    count(distinct t.user_id) as active_retained_users
from analytics_bi.transactions as t
where timestamp_mx_created_at between '{start_date}' and '{end_date}'
and user_id in (
    select
        user_id
    from analytics_bi.transactions as t
    where timestamp_mx_created_at between '{start_date_2}' and '{end_date_2}'
    )
'''
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# Loop for executing the query in consecutive months
for i in tqdm(range(months)):
    # Calculate month as text
    reference_day = datetime.datetime.today().replace(day=1)
    end_date = (reference_day - relativedelta(months=i) - relativedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (reference_day - relativedelta(months=i + 1)).strftime('%Y-%m-%d')
    start_date_2 = (reference_day - relativedelta(months=i + 2)).strftime('%Y-%m-%d')
    end_date_2 = ((reference_day - relativedelta(months=i+1)) - relativedelta(days=1)).strftime('%Y-%m-%d')
    # ========================================================================================================================================
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ========================================================================================================================================
    # =====================
    # REFERRAL VIRALITY
    # =====================
    # Format query
    query_referral = referral_virality_query.format(start_date=start_date, end_date=end_date)
    # Execute query
    aux_referral = pd.read_sql_query(sqlalchemy.text(query_referral), cnx)
    # Append result
    referral_virality = pd.concat([referral_virality, aux_referral])
    # =====================
    # MAUS
    # =====================
    # Format query
    query_mau_60 = mau_60_query.format(start_date=start_date_2, end_date=end_date)
    query_mau_retained = mau_retained_query.format(start_date=start_date, end_date=end_date, start_date_2=start_date_2,end_date_2=end_date_2)
    # Execute query
    aux_mau_60 = pd.read_sql_query(sqlalchemy.text(query_mau_60), cnx)
    aux_mau_retained = pd.read_sql_query(sqlalchemy.text(query_mau_retained), cnx)
    # Append result
    mau_60 = pd.concat([mau_60, aux_mau_60])
    mau_retained = pd.concat([mau_retained, aux_mau_retained])

del aux_mau_60, aux_mau_retained, aux_referral
del end_date, f, i, mau_60_query, mau_retained_query, referral_virality_query
del start_date, start_date_2, end_date_2, reference_day, months, postgres_str, query_mau_60, query_mau_retained, query_referral