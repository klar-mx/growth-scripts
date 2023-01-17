import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe as gd

# Read and Load Credentials
credentials = ServiceAccountCredentials.from_json_keyfile_name('/Users/gabrielreynoso/Documents/GoogleCredentials/gabo_credentials.json')
gc = gspread.authorize(credentials)

# Email: gabriel-klar@testgabriel.iam.gserviceaccount.com

# Open the whole Google Sheet
gsheet = gc.open("[Growth] Waterfall-Weekly")

# Read a worksheet into a Dataframe
values = pd.DataFrame(gsheet.worksheet("Test").get_all_records())

# Append the dataframe again
gd.set_with_dataframe(gsheet.worksheet("Test"), values, row=4, col=1)

