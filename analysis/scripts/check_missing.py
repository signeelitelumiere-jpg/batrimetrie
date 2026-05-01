import pandas as pd, os
files=[
    r"analysis/output_new/merged_data_with_owendo_cols.csv",
    r"analysis/output_new/merged_data.csv",
    r"owendo-05-04-26-4-Outcome data_uzf/Output/OWENDO-BATHY-SURVEY-generated.csv",
]
for f in files:
    print('\n--- FILE:',f,'---')
    if not os.path.exists(f):
        print('MISSING:',f)
        continue
    try:
        df=pd.read_csv(f)
    except Exception as e:
        print('ERROR reading',f,e)
        continue
    n=len(df)
    print('rows=',n)
    na=df.isna().sum()
    na=na[na>0].sort_values(ascending=False)
    if na.empty:
        print('No missing values')
    else:
        print('Columns with missing values (count, pct):')
        for col,cnt in na.items():
            print(f' - {col}: {cnt} ({cnt/ n:.2%})')
    print('\nSample rows (first 5):')
    print(df.head(5).to_string(index=False))
