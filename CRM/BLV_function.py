import pandas as pd
import requests
import json
import base64
import hashlib
import time
from sqlalchemy import create_engine
import sqlalchemy
import os


def run_campaign(source: pd.DataFrame, segment_str: str, attributes: str) -> str:
    """
    Function to add a users to a segment
    :param source: users info dataframe
    :param segment: segment to upload
    :param attributes: list of segment attributes
    """
    # API Credentials from Customeri.IO
    # Should be enviromental variables in the BLV
    api_auth = '1c03f7a99821ad19037c6726689fce29'
    track_api = '998e14fb966d15171526'
    track_cred = '4007a0eacc29eaf377f0'
    # api_auth = os.environ.get('CUSTOMER_IO_API_AUTH', '')
    # track_api = os.environ.get('CUSTOMER_IO_TRACK_API', '')
    # track_cred = os.environ.get('CUSTOMER_IO_TRACK_CRED', '')
    track_auth = '{0}:{1}'.format(track_api, track_cred)

    # Cast to the correct type the params
    # Converting string to list
    attr_list = attributes.strip('][').split(', ')
    # Convert str to int
    segment_int = int(segment_str)
    # ==================================================================
    # Get users from segment
    # ==================================================================
    print(api_auth)
    # Url for users in the segment
    url = 'https://api.customer.io/v1/segments/{0}/membership'.format(segment_int)
    # Headers for the request
    headers = {'Authorization': "Bearer {0}".format(api_auth)}
    # Response from API
    response = requests.request("GET", url, headers=headers).json()
    print(response)
    # Save users as a list
    old_users = response['ids']
    # ==================================================================
    # Delete users from segment
    # ==================================================================
    # Remove api command
    url = "https://track.customer.io/api/v1/segments/{0}/remove_customers?id_type=id".format(segment_int)
    # Headers
    headers = {
        'Authorization': 'Basic {0}'.format(base64.b64encode(track_auth.encode()).decode('utf-8')),
        # 'Authorization': 'Basic {0}'.format(track_auth),
        'content-type': 'application/json'
    }
    # Try catch response
    try:
        r = requests.request("POST", url, headers=headers, data=json.dumps({"ids": old_users}))
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    # ==================================================================
    # Wait 1 minute
    # ==================================================================
    # Sleep 60 seconds
    print('Sleeping 60 seconds')
    time.sleep(2)
    print('Im awake')
    # ==================================================================
    # Add users to a segment
    # ==================================================================
    # Drop duplicates emails
    df = source.drop_duplicates(subset=['email'], keep='first')
    # Decode emails
    df['md5em'] = df.email.apply(lambda x: (hashlib.md5(x.encode())).hexdigest())
    # Headers for request
    headers = {
        'Authorization': 'Basic {0}'.format(base64.b64encode(track_auth.encode()).decode('utf-8')),
        # 'Authorization': 'Basic {0}'.format(track_auth),
        'content-type': 'application/json'
    }
    # Check for attributes
    if len(attr_list) > 0:
        # Add attributes
        payload = dict.fromkeys(attr_list)
        # print(payload)
        # Add each user
        for i in range(len(df)):
            # Get user email
            url = "https://track.customer.io/api/v1/customers/{0}".format(df.md5em.iloc[i])
            # Upload each attribute
            for j in payload:
                payload[j] = str(df[j].iloc[i])
            try:
                # print(payload)
                r = requests.request("PUT", url, headers=headers, data=json.dumps(payload))
                r.raise_for_status()
                # print(r.status_code)
            except requests.exceptions.HTTPError as err:
                raise SystemExit(err)
    # Url for adding customers to segment
    url = "https://track.customer.io/api/v1/segments/{0}/add_customers".format(segment_int)
    # Try-catch
    try:
        r = requests.request("POST", url, headers=headers, data=json.dumps({"ids": df.md5em.to_list()}))
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    # print("I ended")
    return df.shape[0]


# ==============================================================================================================================
# ==============================================================================================================================
# ==============================================================================================================================
# DB Information
db_credentials = open('/Users/gabrielreynoso/Documents/Queries/db_klarprod_connection.txt', 'r')
# Read file
postgres_str = db_credentials.read()
# Close file
db_credentials.close()
# Create engine
cnx = create_engine(postgres_str)
# Query test
test = '''
with salary_advance_data as (
    select 
        user_id,
        days_past_due
    from credit.salary_advance_loanbook
    where loan_status = 'DELINQUENT'
)
select 
    sa.user_id,
    kyc.email,
    sa.days_past_due as adelanto_days_past_due
from salary_advance_data sa
left join klar_pii.db_kyc_public_user_kyc kyc on sa.user_id=kyc.id
where adelanto_days_past_due <= 7
order by kyc.email
'''
# Run query
source = pd.read_sql_query(sqlalchemy.text(test), cnx)
# Segment
segment = '1512'
# Attributes
attributes_list = "[adelanto_days_past_due]"

# Run the function
run_campaign(source, segment, attributes_list)

result = " ".join(line.strip() for line in test.splitlines())
print(repr(result))
