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
start_date = datetime.date(2022, 10, 1)
months = relativedelta(datetime.datetime.today().replace(day=1), start_date).months + relativedelta(datetime.datetime.today().replace(day=1), start_date).years * 12
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# = - = - = - = DATAFRAMES = - = - = - =
# =====================
# REFERRAL VIRALITY
# =====================
referral_virality = pd.DataFrame(columns=['referral_virality', 'referrees', 'sms_confirmed', 'date'])
referral_virality_to_credit = pd.DataFrame(columns=['referral_virality_to_credit', 'referrees_w_credit', 'sms_confirmed', 'date'])
referral_virality_to_debit = pd.DataFrame(columns=['referral_virality_to_debit', 'referrees_w_debit', 'sms_confirmed', 'date'])
referral_virality_from_credit = pd.DataFrame(columns=['referral_virality_from_credit', 'referrees_from_credit', 'sms_from_credit', 'date'])
referral_virality_from_debit = pd.DataFrame(columns=['referral_virality_from_debit', 'referrees_from_debit', 'sms_from_debit', 'date'])
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
    '{start_date}' as date
from referees_count as r
join sms_confirmed_count as s on s.date = r.date
'''
# =====================
# REFERRAL VIRALITY TO DEBIT/CREDIT
# =====================
referral_virality_to_credit_query = '''
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
        and first_cck_line_timestamp_mx notnull
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
    r.referrees*1.0/s.sms_confirmed as referral_virality_to_credit,
    r.referrees as referrees_w_credit,
    s.sms_confirmed as sms_confirmed,
    '{start_date}' as date
from referees_count as r
join sms_confirmed_count as s on s.date = r.date
'''
referral_virality_to_debit_query = '''
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
        and first_cck_line_timestamp_mx is null
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
    r.referrees*1.0/s.sms_confirmed as referral_virality_to_debit,
    r.referrees as referrees_w_debit,
    s.sms_confirmed as sms_confirmed,
    '{start_date}' as date
from referees_count as r
join sms_confirmed_count as s on s.date = r.date
'''
# =====================
# REFERRAL VIRALITY FROM DEBIT/CREDIT
# =====================
referral_virality_from_credit_query = '''
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
          and first_cck_line_timestamp_mx notnull 
    )
select
    r.referrees*1.0/s.sms_confirmed as referral_virality_from_credit,
    r.referrees as referrees_from_credit,
    s.sms_confirmed as sms_from_credit,
    '{start_date}' as date
from referees_count as r
join sms_confirmed_count as s on s.date = r.date
'''
referral_virality_from_debit_query = '''
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
          and first_cck_line_timestamp_mx is null
    )
select
    r.referrees*1.0/s.sms_confirmed as referral_virality_from_debit,
    r.referrees as referrees_from_debit,
    s.sms_confirmed as sms_from_debit,
    '{start_date}' as date
from referees_count as r
join sms_confirmed_count as s on s.date = r.date
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
    # ========================================================================================================================================
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ========================================================================================================================================
    # =====================
    # REFERRAL VIRALITY
    # =====================
    # Execute query
    aux_referral = pd.read_sql_query(sqlalchemy.text(referral_virality_query.format(start_date=start_date, end_date=end_date)), cnx)
    # Append result
    referral_virality = pd.concat([referral_virality, aux_referral])
    # =====================
    # REFERRAL VIRALITY
    # =====================
    # Execute query
    aux_referral_to_credit = pd.read_sql_query(sqlalchemy.text(referral_virality_to_credit_query.format(start_date=start_date, end_date=end_date)), cnx)
    aux_referral_to_debit = pd.read_sql_query(sqlalchemy.text(referral_virality_to_debit_query.format(start_date=start_date, end_date=end_date)), cnx)
    # Append result
    referral_virality_to_credit = pd.concat([referral_virality_to_credit, aux_referral_to_credit])
    referral_virality_to_debit = pd.concat([referral_virality_to_debit, aux_referral_to_debit])
    # =====================
    # REFERRAL VIRALITY
    # =====================
    # Execute query
    aux_referral_from_credit = pd.read_sql_query(sqlalchemy.text(referral_virality_from_credit_query.format(start_date=start_date, end_date=end_date)), cnx)
    aux_referral_from_debit = pd.read_sql_query(sqlalchemy.text(referral_virality_from_debit_query.format(start_date=start_date, end_date=end_date)), cnx)
    # Append result
    referral_virality_from_credit = pd.concat([referral_virality_from_credit, aux_referral_from_credit])
    referral_virality_from_debit = pd.concat([referral_virality_from_debit, aux_referral_from_debit])

del aux_referral, aux_referral_to_credit, aux_referral_to_debit, aux_referral_from_credit, aux_referral_from_debit
del referral_virality_query, referral_virality_to_credit_query, referral_virality_to_debit_query, referral_virality_from_credit_query, referral_virality_from_debit_query
del end_date, f, i,
del start_date, reference_day, months, postgres_str

# ==============================================================================================================================
# Referral Virality split
referral_virality = pd.merge(referral_virality, referral_virality_to_debit[['date', 'referral_virality_to_debit', 'referrees_w_debit']], on='date')
referral_virality = pd.merge(referral_virality, referral_virality_to_credit[['date', 'referral_virality_to_credit', 'referrees_w_credit']], on='date')
referral_virality_col_order = ['date', 'sms_confirmed', 'referrees', 'referral_virality', 'referrees_w_debit', 'referral_virality_to_debit', 'referrees_w_credit', 'referral_virality_to_credit']
referral_virality = referral_virality[referral_virality_col_order]
del referral_virality_col_order
# Referral Virality "From" split
referral_virality_from_split = pd.merge(referral_virality_from_credit, referral_virality_from_debit, on='date')
referral_virality_col_order = ['date', 'sms_from_debit', 'referrees_from_debit', 'referral_virality_from_debit', 'sms_from_credit', 'referrees_from_credit', 'referral_virality_from_credit']
referral_virality_from_split = referral_virality_from_split[referral_virality_col_order]
del referral_virality_col_order