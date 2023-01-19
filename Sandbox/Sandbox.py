import pandas as pd

merchants = pd.read_csv('C:\\Users\\gabri\\Desktop\\Result_1.csv',names = ['avg_purchase','merchant'])
examples = merchants[~merchants.merchant.isna()]
soriana = examples[examples.merchant.str.contains('Soriana',case=False)]
chedraui = examples[examples.merchant.str.contains('Chedraui',case=False)]
bodega_aurrera = examples[(examples.merchant.str.contains('Bodega Aurrera',case=False))&~(examples.merchant.str.contains('Bodega Aurrera Express',case=False))]
ba_express = examples[examples.merchant.str.contains('Bodega Aurrera Express',case=False)]
sams = examples[examples.merchant.str.contains('SAMS',case=False)]
walmart = examples[examples.merchant.str.contains('Walmart',case=False)]

means = [soriana.avg_purchase.mean(),
chedraui.avg_purchase.mean(),
bodega_aurrera.avg_purchase.mean(),
ba_express.avg_purchase.mean(),
sams.avg_purchase.mean(),
walmart.avg_purchase.mean()]
