import pandas as pd

month = 'November'

# Read both csv for cashback
cashback_sql1 = pd.read_csv('/Users/gabrielreynoso/PycharmProjects/klar/Rewards/Cashback/Data/' + month + '_SQL1.csv')
cashback_sql2 = pd.read_csv('/Users/gabrielreynoso/PycharmProjects/klar/Rewards/Cashback/Data/' + month + '_SQL2.csv')
# Generate the raw cashback file
cashback_raw = pd.concat([cashback_sql1,cashback_sql2])
# Delete the separate files
del cashback_sql1, cashback_sql2
# Group by user_id the purchase and adjustments transactions counting items and sumint amounts
cashback_calculation = cashback_raw[cashback_raw.type.isin(['PURCHASE','ADJUSTMENT'])].groupby(['user_id','type']).agg({'amount':'sum','transaction_id':'count'})
# Format the table
cashback_calculation = cashback_calculation.unstack(level=1).fillna(0)
# Rename the columns
cashback_calculation.columns = ['adjustments','amount_purch','num_adjust','num_purchases']
# Raw Export
cashback_calculation.to_csv('./Cashback_Payments/Raw_' + month + '.csv')
# Filter users with less than 10 purchases
cashback_users = cashback_calculation[cashback_calculation.amount_purch > 9]
# Calculate the right amount of spend for each user
# cashback_users = cashback_calculation.copy()
cashback_users['cashback_amount'] = -1*cashback_users.amount_purch - cashback_users.adjustments
# cashback_users = cashback_users[cashback_users.cashback_amount >= 2000]
# Filter out the negative amount
cashback_users = cashback_users[cashback_users.cashback_amount >= 0]
# Calculate the cashback
cashback_users['cashback'] = cashback_users.cashback_amount* 0.01
# Top the cashback for max 1000
cashback_users['cashback'] = cashback_users['cashback'].apply(lambda x: 1000 if x > 1000 else round(x, 1))
# Filter the less than 1
cashback_users = cashback_users[cashback_users['cashback'] >= 1]
# Format the table
cashback_users = cashback_users.reset_index()
# Export the result
# cashback_users.to_csv('./Cashback_Payments/' + month + '.csv')
