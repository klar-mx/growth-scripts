import pandas as pd

names_file = pd.ExcelFile('/Users/gabrielreynoso/Downloads/Names_matching.xlsx')

list1 = pd.read_excel(names_file, sheet_name='Lista 1')
list2 = pd.read_excel(names_file, sheet_name='Lista 1.2')
names = pd.read_excel(names_file, sheet_name='Nombres')

list2['Combined'] = list2['first_name'].astype(str) +' '+ list2['last_name'].astype(str)

list1['user_email'] = list1.apply(lambda x: (x.email,x.klarUserId), axis=1)
list2['user_email'] = list2.apply(lambda x: (x.email,x.klarUserId), axis=1)

users1 = list1.groupby('Combined')['user_email'].apply(list).reset_index(name='user_emails')
users2 = list2.groupby('Combined')['user_email'].apply(list).reset_index(name='user_emails')

users = pd.concat([users1,users2])

users['len'] = users.apply(lambda x: len(x.user_emails), axis = 1)

names.columns = ['Combined']

users_names = pd.merge(names, users, on = 'Combined', how = 'left')

user_names = users_names[['Combined', 'len', 'user_emails']]

