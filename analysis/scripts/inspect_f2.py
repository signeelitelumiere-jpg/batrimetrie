import pandas as pd
p='analysis/output_new/merged_data_with_owendo_cols.csv'
df=pd.read_csv(p, dtype=str)
print('dtype f2:', df['f2'].dtype)
print('unique (first 20):', df['f2'].unique()[:20])
print('isnull count:', df['f2'].isna().sum(), 'empty strings:', (df['f2']=='').sum())
