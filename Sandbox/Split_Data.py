import pandas as pd


df = pd.read_csv('/Users/gabrielreynoso/Downloads/deliveries.csv')
parts = 8
chunk = round(df.shape[0]/parts,0)
for i in range(parts):
    aux = df.iloc[int(chunk*i):int(chunk*(i+1)),:]
    aux.to_csv('./Chunks/Chunk_{}.csv'.format(i))