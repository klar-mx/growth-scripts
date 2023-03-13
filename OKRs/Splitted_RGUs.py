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
start_date = datetime.date(2022, 9, 1)
months = relativedelta(datetime.datetime.today().replace(day=1), start_date).months + relativedelta(datetime.datetime.today().replace(day=1), start_date).years * 12
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# = - = - = - = DATAFRAMES = - = - = - =
# =====================
# REVENUE GENERATING USERS
# =====================
splitted_rgus = []
# ========================================================================================================================================
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ========================================================================================================================================
# Queries
# =====================
# CREDIT ACTIVE USERS
# =====================
rgus_splitted_query = '''
 WITH r_users AS (
                /* Positive Balance at end of the day*/
                SELECT
                    b.user_id AS user_id
                FROM analytics_bi.balances_mex b
                where current_balance > 0
                and b.balance_date::timestamp between '{start_date_2}'::date and '{end_date_2}'::date
                UNION
                /* Any Transaction */
                SELECT t.user_id AS user_id
                FROM analytics_bi.transactions t
                where timestamp_mx_created_at between '{start_date}'::date and '{end_date}'::date
                UNION ALL
                /* CCK */
                   select
                        user_id
                   from loans.cck_loanbook_by_installment
                   where (loan_disbursement_date::date between '{start_date}'::date and '{end_date}'::date)
                    or (due_date::date between '{start_date}'::date and '{end_date}'::date
                     and balance > 0 and installment_status in ('DELINQUENT', 'OPENED'))
                UNION ALL
                /*
                 DELINQUENT CCK
                 */
                select
                    user_id
                from loans.cck_loanbook
                where (maturity_date_mx::date between '{start_date_3}'::date and '{end_date}'::date
                     and loan_balance > 0 and loan_status = 'DELINQUENT')
                UNION ALL
                /* Credi Klar*/
                    select
                        user_id
                from loans.crediklar_loanbook
                where (disbursement_date::date between '{start_date}'::date and '{end_date}'::date)
                    or (maturity_date::date between '{start_date}'::date and '{end_date}'::date
                    and balance > 0 and loan_status in ('DELINQUENT', 'OPENED'))
                    or (maturity_date::date between '{start_date_3}'::date and '{end_date}'::date
                     and balance > 0 and loan_status in ('DELINQUENT'))
                UNION ALL
                /*
                 +Credito
                 */
                select ls.user_id
                from db_loans_payment_sequence ls
                join credit.klar_loans as kl on ls.loan_id = kl.loan_id
                where (ls.disbursement_date::date between '{start_date}'::date and '{end_date}'::date)
                    or ((ls.acceptance_date::date between '{start_date}'::date and '{end_date}'::date) and ls.acceptance_date::date > ls.disbursement_date::date and ls.due_payment_sequence like '%EXTENSION%')
                    or ((ls.due_payment_date::date between '{start_date_3}'::date and '{end_date}'::date) and ls.due_payment_sequence not like '%EXTENSION%' and kl.loan_status not like 'CLOSED')
),
     mau as (
         /* Any Transaction */
                SELECT
                    distinct t.user_id AS user_id,
                    'MAU' as segment_mau
                FROM analytics_bi.transactions t
                where timestamp_mx_created_at between '{start_date}'::date and '{end_date}'::date
     ),
     balance as (
            SELECT
                distinct b.user_id AS user_id,
                         'BALANCE' as segment_balance
            FROM analytics_bi.balances_mex b
            where current_balance > 0
            and b.balance_date::timestamp between '{start_date_2}'::date and '{end_date_2}'::date
     ),
     interest as (
            /* CCK */
               select
                    distinct user_id,
                             'Interest' as segment_interest
               from loans.cck_loanbook_by_installment
               where (loan_disbursement_date::date between '{start_date}'::date and '{end_date}'::date)
                or (due_date::date between '{start_date}'::date and '{end_date}'::date
                 and balance > 0 and installment_status in ('DELINQUENT', 'OPENED'))
                UNION ALL
                /*
                 DELINQUENT CCK
                 */
                select
                    distinct user_id,
                             'Interest' as segment_interest
                from loans.cck_loanbook
                where (maturity_date_mx::date between '{start_date_3}'::date and '{end_date}'::date
                     and loan_balance > 0 and loan_status = 'DELINQUENT')
                UNION ALL
                /* Credi Klar*/
                    select
                        distinct user_id,
                                 'Interest' as segment_interest
                from loans.crediklar_loanbook
                where (disbursement_date::date between '{start_date}'::date and '{end_date}'::date)
                    or (maturity_date::date between '{start_date}'::date and '{end_date}'::date
                    and balance > 0 and loan_status in ('DELINQUENT', 'OPENED'))
                    or (maturity_date::date between '{start_date_3}'::date and '{end_date}'::date
                     and balance > 0 and loan_status in ('DELINQUENT'))
                UNION ALL
                /*
                 +Credito
                 */
                select
                    distinct ls.user_id,
                             'Interest' as segment_interest
                from db_loans_payment_sequence ls
                join credit.klar_loans as kl on ls.loan_id = kl.loan_id
                where (ls.disbursement_date::date between '{start_date}'::date and '{end_date}'::date)
                    or ((ls.acceptance_date::date between '{start_date}'::date and '{end_date}'::date) and ls.acceptance_date::date > ls.disbursement_date::date and ls.due_payment_sequence like '%EXTENSION%')
                    or ((ls.due_payment_date::date between '{start_date_3}'::date and '{end_date}'::date) and ls.due_payment_sequence not like '%EXTENSION%' and kl.loan_status not like 'CLOSED')
     ),
rgu_split as (
        select
            rgu.user_id,
            coalesce(m.segment_mau, '') as segment_m,
            coalesce(b.segment_balance, '') as segment_b,
            coalesce(i.segment_interest, '') as segment_i,
            concat(segment_m, concat('_', concat(segment_b, concat('_', segment_i)))) as segment
        from r_users rgu
        left join mau as m on m.user_id = rgu.user_id
        left join balance as b on b.user_id = rgu.user_id
        left join interest i on i.user_id = rgu.user_id
    )
select
    'total' as segment,
    count(distinct user_id) as total_rgus
from r_users
UNION ALL
select
    segment,
    count(distinct user_id) as total_rgus
from rgu_split
group by segment
having total_rgus > 0
order by segment desc
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
    start_date_2 = (reference_day - relativedelta(months=i + 1) - relativedelta(days=1)).strftime('%Y-%m-%d')
    end_date_2 = ((reference_day - relativedelta(months=i)) - relativedelta(days=2)).strftime('%Y-%m-%d')
    start_date_3 = (reference_day - relativedelta(months=i + 3)).strftime('%Y-%m-%d')
    # ========================================================================================================================================
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ========================================================================================================================================
    # =====================
    # REVENUE GENERATING USERS (LIGHT MAU)
    # =====================
    # Format query
    query_rgus_splitted = rgus_splitted_query.format(start_date=start_date, end_date=end_date, start_date_2=start_date_2,end_date_2 =end_date_2,start_date_3=start_date_3)
    # Execute query
    aux_rgus = pd.read_sql_query(sqlalchemy.text(query_rgus_splitted), cnx)
    # Append result
    splitted_rgus.append(aux_rgus)

splitted_rgus[0].to_clipboard(index=False)