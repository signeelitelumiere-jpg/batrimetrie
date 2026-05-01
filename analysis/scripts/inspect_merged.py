import pandas as pd
p='analysis/output_new/merged_data_with_owendo_cols.csv'
df=pd.read_csv(p)
print('total',len(df))
print('Lat non-null', df['Lat'].notna().sum())
print('\n--- sample with Lat ---')
print(df[df['Lat'].notna()].head(5).to_string())
print('\n--- sample testbaty ---')
print(df[df.get('src_file','').astype(str).str.contains('testbaty', na=False)].head(5).to_string())
