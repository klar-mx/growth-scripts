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
# REVENUE GENERATING USERS
# =====================
mau_credit = pd.DataFrame(columns=['starting_date', 'end_date', 'credit_maus'])
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# Queries
# =====================
# REVENUE GENERATING USERS (LIGHT MAU)
# =====================
credit_mau_query = '''
with credit_mau as (select user_id
                    from analytics_bi.transactions t
                    where timestamp_mx_created_at between '{start_date}' and '{end_date}'
                      and t.type = 'PURCHASE'
                      and balance_category = 'CREDIT_CARD'
                      and t.source_account_internal_id <> '0000000000000000'
                      AND t.source_account_internal_id <> '00000000-0000-0000-0000-000000000000'
                      AND t.amount <> 0
                      and t.state = 'SETTLED'
                      AND t.transaction_id NOT LIKE 'TGT%'
                      AND t.transaction_id NOT LIKE 'SRC%'
                    UNION
                    select user_id
                    from loans.cck_loanbook
                    where product_type = 'CASH_INSTALLMENT'
                      and disbursement_date_mx between '{start_date}' and '{end_date}'
                    UNION
                    select cck_tasks.user_id
                    from loans.cck_tasks
                    where loan_task_type = 'REPAY'
                      and loan_task_status = 'COMPLETED'
                      and updated_tmp_mx between '{start_date}' and '{end_date}'
                    UNION
                    /*
                    CrediKlar
                    */
                    select user_id
                    from loans.klar_loans
                    where product_name = 'crediklar'
                      and disbursement_date between '{start_date}' and '{end_date}'
                    UNION
                    select user_id
                    from analytics_history.loanevent
                    where type = 'BULLET'
                      and loan_task_type in ('PROLONG', 'REPAY')
                      and convert_timezone('UTC', '1970-01-01 00:00:00'::timestamp without time zone + created_in_ms::double precision * '00:00:00.001'::interval)::date between '{start_date}' and '{end_date}'
                        /*
                        +Credito
                        */
                    UNION
                    select user_id
                    from loans.klar_loans
                    where product_name = '+credito'
                      and disbursement_date between '{start_date}' and '{end_date}'
                    UNION
                    select user_id
                    from db_loans_payments
                    where payment_agreement = 'INTEREST'
                      and payment_sequence_name like '%EXTENSION%'
                      and payment_date::date between '{start_date}' and '{end_date}'
                    UNION
                    select user_id
                    from db_loans_payments
                    where payment_agreement in ('PARTIAL', 'FULL')
                      and payment_date::date between '{start_date}' and '{end_date}'
                      )
select
    '{start_date}' as starting_date,
    '{end_date}' as end_date,
    count(user_id) as credit_maus
from credit_mau
'''
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# Loop for executing the query in consecutive months
for i in tqdm(range(months)):
    # Calculate month as text
    reference_day = datetime.datetime.today().replace(day=1)
    end_date = (reference_day - relativedelta(months=i) - relativedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (reference_day - relativedelta(months=i+1)).strftime('%Y-%m-%d')
    # month = (datetime.datetime.today().replace(day=1) - relativedelta(months=i)).strftime('%Y-%m-%d')
    # ========================================================================================================================================
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ========================================================================================================================================
    # =====================
    # REVENUE GENERATING USERS (LIGHT MAU)
    # =====================
    # Format query
    query_credit_maus = credit_mau_query.format(start_date=start_date, end_date=end_date)
    # Execute query
    aux_mau_credit = pd.read_sql_query(sqlalchemy.text(query_credit_maus), cnx)
    # Append result
    mau_credit = pd.concat([mau_credit, aux_mau_credit])