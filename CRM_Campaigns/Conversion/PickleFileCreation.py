import pandas as pd
import os

# Get path to directory
dirname = os.path.dirname(__file__)

month = 'October2021'
conversionExcelFileMonth = pd.ExcelFile(os.path.join(dirname, 'Data/' + month + '.xlsx'))

# Print sheet names from Excel File
for sheet_name in conversionExcelFileMonth.sheet_names:
    print(sheet_name)

sheets_users = conversionExcelFileMonth.sheet_names[:15]

for not_segment in ['Import no csu control', 'import no csu treatment']:
    sheets_users.remove(not_segment)

# Print columns for the sheets
for segment in sheets_users:
    aux = pd.read_excel(conversionExcelFileMonth, sheet_name=segment)
    print(segment)
    print(aux.columns)

# Create all users dataframe
users = pd.DataFrame()
for segment in sheets_users:
    aux = pd.read_excel(conversionExcelFileMonth, sheet_name=segment)
    segment_name = segment.replace('- ', '')
    segment_name = segment_name.replace('C', 'control')
    segment_name = segment_name.replace(' ', '_')
    aux['segment'] = segment + '_' + month
    if 'uuid' in aux.columns:
        aux = aux[['uuid', 'segment']]
    else:
        aux = aux[['klarUserId', 'segment']]
    aux.columns = ['user_id','segment']
    users = pd.concat([users,aux])
    print(users.shape[0])

# Payments
payments_list = [x for x in conversionExcelFileMonth.sheet_names if 'Payments' in x]

# Create payments dataframe
payments = pd.DataFrame()
for pay_list in payments_list:
    aux = pd.read_excel(conversionExcelFileMonth, sheet_name=pay_list)
    aux['Payment'] = True
    aux = aux[['uuid', 'Payment']]
    aux.columns = ['user_id','Payment']
    payments = pd.concat([payments,aux])
    print(payments.shape[0])

# Create final db
users_resume = pd.merge(users, payments, how='left', on = 'user_id')

# Create pickle
# users_resume.to_pickle("./Data/" + month + ".pkl")