import time
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
from tqdm import tqdm
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# Read and Load Credentials
credentials = ServiceAccountCredentials.from_json_keyfile_name('/Users/gabrielreynoso/Documents/GoogleCredentials/gabo_credentials.json')
gc = gspread.authorize(credentials)

# Payment db
payment_db = pd.DataFrame()

# List of payment sheets
sheets_payments = {
    '2022-06-01': '[Growth] RFM segmentation - June',
    '2022-07-01': '[Growth] RFM segmentation - July',
    '2022-08-01': '[Growth] RFM segmentation - August',
    '2022-10-01': '[Growth] RFM segmentation - October',
    '2022-11-01': '[Growth22] RFM segmentation - November (til jan)',
    '2023-01-01': '[Growth23] RFM segmentation - January'
}

# Get each month payments
for month in sheets_payments.keys():
    # Open the whole Google Sheet
    gsheet = gc.open(sheets_payments[month])
    # Get values to a dataframe
    month_payments = pd.DataFrame(gsheet.worksheet("Cumulative payments").get_all_records())
    # Filter columns
    month_payments = month_payments[['uuid','fee']]
    # Add month column
    month_payments['month'] = pd.to_datetime(month)
    # Append dataframe
    payment_db = pd.concat([payment_db, month_payments])

# Drop duplicates by user_id and month
payment_db = payment_db.drop_duplicates(['uuid','month'])
# Save the DB
payment_db.to_pickle('RFM_CHURNED_PAYMENTS.pkl')


