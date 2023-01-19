import pandas as pd
import re
import numpy as np

# Results of the survey
nps = pd.read_csv('Data/NPS_Responses_23May.csv')

# Rename columns
original_columns = nps.columns.to_list()
nps.columns = ['Date', 'Source', 'Email', 'NPS', 'Cambios', 'Like', 'Disappeared', 'Usage', 'Confidence', 'Benefits', 'Reliable', 'Aid', 'Subm_ID']
nps = nps[~nps.Date.isna()]
nps = nps[~nps.Email.isna()]
nps = nps[~nps.Email.str.contains('@klar')]
nps.Date = pd.to_datetime(nps.Date)
# ==========================================================================================================================================================================================
# ==========================================================================================================================================================================================
# ================================================================================  SINGLE SCORES  =========================================================================================
# ==========================================================================================================================================================================================
# ==========================================================================================================================================================================================

for col in ['Confidence', 'Benefits', 'Reliable', 'Aid']:
    nps[col] = nps[col].apply(lambda x: re.findall(r'^\D*(\d+)', x) if pd.notnull(x) else x)
    nps[col] = nps[col].apply(lambda x: (x[0] if len(x) > 0 else np.nan)if pd.notnull(x) else x)