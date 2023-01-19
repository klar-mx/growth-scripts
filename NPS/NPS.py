import pandas as pd
import re
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt

# Results of the survey
nps = pd.read_csv('Data/NPS_Responses_13Jun.csv')


def calc_nps(df: pd.DataFrame) -> pd.DataFrame:
    '''
    This function calculate de # of Detractors, Neutrals and Ambassadors based on a previous group by nps.
    :param df: DataFrame that contains the grouped NPS califications
    :return: Same Dataframe with additional calculated columns
    '''

    # Detractors
    df = df.merge(df.loc[:, [1, 2, 3, 4, 5,6]].sum(axis=1).to_frame(name='Detractors'), left_index=True, right_index=True)
    # Neutrals
    df = df.merge(df.loc[:, [7, 8]].sum(axis=1).to_frame(name='Neutrals'), left_index=True, right_index=True)
    # Ambassador
    df = df.merge(df.loc[:, [9, 10]].sum(axis=1).to_frame(name='Ambassador'), left_index=True, right_index=True)
    # %Ambassador - %Detractors
    for col in ['Ambassador', 'Detractors']:
        aux = df.loc[:, [col]].div(df.loc[:, ['Detractors', 'Neutrals', 'Ambassador']].sum(axis=1), axis=0)
        aux.columns = ['%' + col]
        df = df.merge(aux, left_index=True, right_index=True)
    # NPS Score
    df['NPS_score'] = df['%Ambassador'] - df['%Detractors']
    return df


# Rename columns
original_columns = nps.columns.to_list()
nps.columns = ['Date', 'Source', 'Email', 'NPS', 'Cambios', 'Like', 'Disappeared', 'Usage', 'Confidence', 'Benefits', 'Reliable', 'Aid', 'Subm_ID', 'Month', 'Year']
nps = nps[~nps.Date.isna()]
nps = nps[~nps.Email.isna()]
nps = nps[~nps.Email.str.contains('@klar')]
nps.Date = pd.to_datetime(nps.Date)
# ==========================================================================================================================================================================================
# ==========================================================================================================================================================================================
# ================================================================================  SINGLE SCORES  =========================================================================================
# ==========================================================================================================================================================================================
# ==========================================================================================================================================================================================

# Treatment to column: confidence
confidence = nps[['Date', 'Confidence']]
confidence = confidence[~confidence.Confidence.isna()]
confidence.Confidence = confidence['Confidence'].apply(lambda x: re.findall(r'^\D*(\d+)', x))
confidence.Confidence = confidence['Confidence'].apply(lambda x: x[0] if len(x) > 0 else np.nan)
confidence = confidence[~confidence.Confidence.isna()]
# Convert column to integer
confidence.Confidence = confidence.Confidence.astype(int)

# Average by month
monthly_resume = confidence.groupby(pd.Grouper(key='Date', freq='M'))['Confidence'].mean().to_frame(name='Confidence')

# Treatment to column: benefits
benefits = nps[['Date', 'Benefits']]
benefits = benefits[~benefits.Benefits.isna()]
benefits.Benefits = benefits['Benefits'].apply(lambda x: re.findall(r'^\D*(\d+)', x))
benefits.Benefits = benefits['Benefits'].apply(lambda x: x[0] if len(x) > 0 else np.nan)
benefits = benefits[~benefits.Benefits.isna()]
# Convert column to integer
benefits.Benefits = benefits.Benefits.astype(int)

# Average by month
monthly_resume = monthly_resume.merge(benefits.groupby(pd.Grouper(key='Date', freq='M'))['Benefits'].mean(), left_index=True, right_index=True)

# Treatment to column: reliable
reliable = nps[['Date', 'Reliable']]
reliable = reliable[~reliable.Reliable.isna()]
reliable.Reliable = reliable['Reliable'].apply(lambda x: re.findall(r'^\D*(\d+)', x))
reliable.Reliable = reliable['Reliable'].apply(lambda x: x[0] if len(x) > 0 else np.nan)
reliable = reliable[~reliable.Reliable.isna()]
# Convert column to integer
reliable.Reliable = reliable.Reliable.astype(int)

# Average by month
monthly_resume = monthly_resume.merge(reliable.groupby(pd.Grouper(key='Date', freq='M'))['Reliable'].mean(), left_index=True, right_index=True)

# Treatment to column: aid
aid = nps[['Date', 'Aid']]
aid = aid[~aid.Aid.isna()]
aid.Aid = aid['Aid'].apply(lambda x: re.findall(r'^\D*(\d+)', x))
aid.Aid = aid['Aid'].apply(lambda x: x[0] if len(x) > 0 else np.nan)
aid = aid[~aid.Aid.isna()]
# Convert column to integer
aid.Aid = aid.Aid.astype(int)

# Average by month
monthly_resume = monthly_resume.merge(aid.groupby(pd.Grouper(key='Date', freq='M'))['Aid'].mean(), left_index=True, right_index=True)
monthly_resume.index = monthly_resume.index.strftime('%b-%y')

# ==========================================================================================================================================================================================
# ==========================================================================================================================================================================================
# =================================================================================  NPS SCORES  ===========================================================================================
# ==========================================================================================================================================================================================
# ==========================================================================================================================================================================================
# Sub table with de NPS score and source
nps_analysis = nps[['Date','Email', 'Source', 'NPS']]
nps_analysis = nps_analysis[(~nps_analysis.Source.isna()) & (nps_analysis.Source != 'test') & (~nps_analysis.NPS.isna())]
nps_analysis.NPS = nps_analysis.NPS.astype(int)
nps_analysis = nps_analysis.drop_duplicates(['Email','Source'])
# Divide the table of NPS
month_nps = nps_analysis[['Date', 'Source', 'NPS']]
del nps_analysis
# ==========================================================================================================================================================================================
# ==========================================================================================================================================================================================
# NPS Score
# General
general_month_nps = month_nps.groupby([pd.Grouper(key='Date', freq='M'), 'NPS']).size().unstack(level=1).fillna(0)
# NPS Calc
general_month_nps = calc_nps(general_month_nps)
# Format month-year
general_month_nps.index = general_month_nps.index.strftime('%b-%y')
# Metrics
NPS_metrics = general_month_nps[['Detractors','Neutrals','Ambassador','NPS_score']]
# ==========================================================================================================================================================================================
# Opened by Source
source_month_nps = month_nps.groupby([pd.Grouper(key='Date', freq='M'), 'Source', 'NPS']).size().unstack(level=2).fillna(0)
# NPS Calc
source_month_nps = calc_nps(source_month_nps)
# Reset index
source_month_nps = source_month_nps.reset_index()
# Set index
source_month_nps = source_month_nps.set_index('Date')
# Format month-year
source_month_nps.index = source_month_nps.index.strftime('%b-%y')
# ==========================================================================================================================================================================================
# ===========================================================           SOURCE NPS           ===============================================================================================
# ==========================================================================================================================================================================================
# DAILY
# Daily Source NPS
source_daily_responses = month_nps.groupby([pd.Grouper(key='Date', freq='d'), 'Source', 'NPS']).size().unstack(level=2).fillna(0)
# NPS Calc
source_daily_responses = calc_nps(source_daily_responses)
# Reset index
source_daily_responses = source_daily_responses.reset_index()
# Set index
source_daily_responses = source_daily_responses.set_index('Date')
# Format day-month-year
source_daily_responses.index = source_daily_responses.index.strftime('%d-%m-%y')
# ==========================================================================================================================================================================================
# MONTHLY
# Monthly Source NPS
source_monthly_responses = month_nps.groupby([pd.Grouper(key='Date', freq='M'), 'Source', 'NPS']).size().unstack(level=2).fillna(0)
# NPS Calc
source_monthly_responses = calc_nps(source_monthly_responses)
# Reset index
source_monthly_responses = source_monthly_responses.reset_index()
# Set index
source_monthly_responses = source_monthly_responses.set_index('Date')
# Format day-month-year
source_monthly_responses.index = source_monthly_responses.index.strftime('%d-%m-%y')
# ==========================================================================================================================================================================================
# Count Daily Responses
daily_count = month_nps.groupby([pd.Grouper(key='Date', freq='d')]).size().fillna(0)

# daily_count.to_clipboard(header=False)
# source_daily_responses.to_clipboard()
# source_monthly_responses.to_clipboard()
# NPS_metrics.to_clipboard()