import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# July transactions
july_transactions = pd.read_csv('../Cashback_Payments/Raw_July.csv')
# August transactions
august_transactions = pd.read_csv('../Cashback_Payments/Raw_August.csv')
# Experiment DB
experiment_db = pd.read_pickle('Experiment_August_DB.pkl')

# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# Calculate the right amount of spend for each user
july_transactions['cashback_amount'] = -1 * july_transactions.amount_purch - july_transactions.adjustments
august_transactions['cashback_amount'] = -1 * august_transactions.amount_purch - august_transactions.adjustments
# Calculate the cashback
july_transactions['cashback'] = july_transactions.cashback_amount * 0.01
august_transactions['cashback'] = august_transactions.cashback_amount * 0.01
# Top the cashback for max 1000
july_transactions['cashback'] = july_transactions['cashback'].apply(lambda x: 1000 if x > 1000 else round(x, 1))
august_transactions['cashback'] = august_transactions['cashback'].apply(lambda x: 1000 if x > 1000 else round(x, 1))
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
july = july_transactions[['user_id', 'amount_purch', 'num_purchases', 'cashback']]
aug = august_transactions[['user_id', 'amount_purch', 'num_purchases', 'cashback']]
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
july = july.set_index('user_id')
aug = aug.set_index('user_id')
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
july = july.add_prefix('july_')
aug = aug.add_prefix('aug_')
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
july = july.reset_index()
aug = aug.reset_index()
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
user_info = pd.merge(july, aug, how='outer', on='user_id')
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# Experiment
experiment_db_encoded = pd.concat([experiment_db, pd.get_dummies(experiment_db.segment)], axis=1)
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# Requirements
experiment_db_encoded['pur_req'] = experiment_db_encoded.segment.apply(lambda x: (5 if x == 'Test 2 Treatment 5p 1_ Segment' else 8) if x in ['Test 2 Treatment 8p 1_ Segment', 'Test 2 Treatment 5p 1_ Segment'] else pd.NA)
experiment_db_encoded['amount_req'] = experiment_db_encoded.segment.apply(
    lambda x: (1000 if x == 'Test 2 Treatment 1k 1_ Segment' else 2000) if x in ['Test 2 Treatment 2k 1_ Segment', 'Test 2 Treatment 2k $20 Segment', 'Test 2 Treatment 1k 1_ Segment'] else pd.NA)
# Normal Cashback
experiment_db_encoded.loc[experiment_db_encoded['segment'] == 'Normal']['pur_req'] = 10
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# Add experiment information
user_info = pd.merge(user_info, experiment_db_encoded, on='user_id', how='outer')
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# Fill na values
user_info[['july_amount_purch', 'july_num_purchases', 'july_cashback', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback']] = user_info[
    ['july_amount_purch', 'july_num_purchases', 'july_cashback', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback']].fillna(0)
# User with cashback in august not involved in the experiment
cashback_aug = user_info[(~user_info.segment.isna()) | (user_info['aug_cashback'] >= 1)]
# Fill na's from segment
cashback_aug['segment'] = cashback_aug['segment'].fillna('NoExp')
# Purchases filter
cashback_aug = cashback_aug[((cashback_aug.aug_num_purchases > 9) & (cashback_aug.segment == 'NoExp')) | (cashback_aug.segment != 'NoExp')]
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
noExp_Cashback = cashback_aug[cashback_aug.segment == 'NoExp']
noExp_Cashback = noExp_Cashback[['user_id', 'july_amount_purch', 'july_num_purchases', 'july_cashback', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback', 'segment']]
noExp_Cashback['Pay_Cashback'] = True
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
optIn_Cashback = cashback_aug[cashback_aug['Test 1 Opt in Segment'] == 1]
optIn_Cashback['Pay_Cashback'] = optIn_Cashback.aug_num_purchases > 9
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
test_8p_Cashback = cashback_aug[cashback_aug['Test 2 Treatment 8p 1_ Segment'] == 1]
test_8p_Cashback['Pay_Cashback'] = test_8p_Cashback.aug_num_purchases > test_8p_Cashback.pur_req
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
test_1k_Cashback = cashback_aug[cashback_aug['Test 2 Treatment 1k 1_ Segment'] == 1]
test_1k_Cashback['Pay_Cashback'] = -1 * test_1k_Cashback.aug_amount_purch > test_1k_Cashback.amount_req
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
test_2k_20_Cashback = cashback_aug[cashback_aug['Test 2 Treatment 2k $20 Segment'] == 1]
test_2k_20_Cashback['Pay_Cashback'] = -1 * test_2k_20_Cashback.aug_amount_purch > test_2k_20_Cashback.amount_req
test_2k_20_Cashback['aug_cashback'] = 20
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
test_2k_Cashback = cashback_aug[cashback_aug['Test 2 Treatment 2k 1_ Segment'] == 1]
test_2k_Cashback['Pay_Cashback'] = -1 * test_2k_Cashback.aug_amount_purch > test_2k_Cashback.amount_req
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
test_5p_Cashback = cashback_aug[cashback_aug['Test 2 Treatment 5p 1_ Segment'] == 1]
test_5p_Cashback['Pay_Cashback'] = test_5p_Cashback.aug_num_purchases > test_5p_Cashback.pur_req
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
test_normal_Cashback = cashback_aug[cashback_aug['Normal'] == 1]
test_normal_Cashback['Pay_Cashback'] = test_normal_Cashback.aug_num_purchases > 9
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
test_control_Cashback = cashback_aug[cashback_aug['Test 2 Control Segment'] == 1]
test_control_Cashback['Pay_Cashback'] = test_control_Cashback.aug_num_purchases > 9
test_control_Cashback['aug_cashback'] = 0
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# List of cashback for users
cashback_payment = pd.concat([
    noExp_Cashback[['user_id', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback', 'Pay_Cashback', 'segment']],
    test_8p_Cashback[['user_id', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback', 'Pay_Cashback', 'segment']],
    test_1k_Cashback[['user_id', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback', 'Pay_Cashback', 'segment']],
    test_2k_20_Cashback[['user_id', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback', 'Pay_Cashback', 'segment']],
    test_2k_Cashback[['user_id', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback', 'Pay_Cashback', 'segment']],
    test_5p_Cashback[['user_id', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback', 'Pay_Cashback', 'segment']],
    test_normal_Cashback[['user_id', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback', 'Pay_Cashback', 'segment']],
    test_control_Cashback[['user_id', 'aug_amount_purch', 'aug_num_purchases', 'aug_cashback', 'Pay_Cashback', 'segment']]
])
# Export
cashback_payment.to_excel('./August2022_Experiment_Outputs/CashbackPayments.xlsx')

# Cohorts
cohort_list = [noExp_Cashback, optIn_Cashback, test_8p_Cashback, test_1k_Cashback, test_2k_20_Cashback, test_2k_Cashback, test_5p_Cashback, test_normal_Cashback, test_control_Cashback]
