# Send campaign
import requests
import json
import base64
import hashlib
import pandas as pd


class Customer_io():
    def __init__(self):
        self.api_auth = '1c03f7a99821ad19037c6726689fce29'
        self.track_api = '998e14fb966d15171526'
        self.track_cred = '4007a0eacc29eaf377f0'
        self.track_auth = '{0}:{1}'.format(self.track_api, self.track_cred)

    def get_users_segment(self, segment: int) -> list:
        url = 'https://api.customer.io/v1/segments/{0}/membership'.format(segment)

        headers = {'Authorization': "Bearer {0}".format(self.api_auth)}

        response = requests.request("GET", url, headers=headers).json()
        return response['ids']

    def del_users_segment(self, segment: int, users: list) -> bool:
        url = "https://track.customer.io/api/v1/segments/{0}/remove_customers?id_type=id".format(segment)
        headers = {
            'Authorization': 'Basic {0}'.format(base64.b64encode(self.track_auth.encode()).decode('utf-8')),
            'content-type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=json.dumps({"ids": users}))
        if response.ok:
            return True
        else:
            print(response.text)
            return False

    def add_users_segment(self, segment: int, df: pd.DataFrame, attributes: list = []) -> bool:

        df.drop_duplicates(subset=['email'], keep='first')

        df['md5em'] = df.email.apply(lambda x: (hashlib.md5(x.encode())).hexdigest())
        headers = {
            'Authorization': 'Basic {0}'.format(base64.b64encode(self.track_auth.encode()).decode('utf-8')),
            'content-type': 'application/json'
        }
        if len(attributes) > 0:
            # Add attributes
            payload = dict.fromkeys(attributes)
            for i in range(len(df)):
                url = "https://track.customer.io/api/v1/customers/{0}".format(df.md5em.iloc[i])
                print(df.email.iloc[i])
                print(df.md5em.iloc[i])
                for j in payload:
                    payload[j] = df[j].iloc[i]
                print(payload)
                response = requests.request("PUT", url, headers=headers, data=json.dumps(payload))
                if not response.ok:
                    print('Something went wrong when adding attributes, please check: {0}'.format(response.text))
                    return False

        url = "https://track.customer.io/api/v1/segments/{0}/add_customers".format(segment)
        response = requests.request("POST", url, headers=headers, data=json.dumps({"ids": df.md5em.to_list()}))
        if response.ok:
            print('Users uploaded')
            return True
        else:
            print('Something went wrong when adding users, please check:{0}'.format(response.text))
            return False

