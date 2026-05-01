import pandas as pd, glob, os
p='analysis/output_new'
files=glob.glob(os.path.join(p,'*.csv'))
print('Found',len(files),'csv files in',p)
for f in files:
    try:
        df=pd.read_csv(f)
    except Exception as e:
        print('ERR reading',f,e); continue
    n=len(df)
    missing_counts={}
    emptystr_counts={}
    for col in df.columns:
        na=int(df[col].isna().sum())
        empty=0
        if df[col].dtype==object:
            # treat strings that are empty or 'None' as empty
            s = df[col].astype(str).str.strip()
            empty = int((s=="").sum())
        if na>0:
            missing_counts[col]=na
        if empty>0:
            emptystr_counts[col]=empty
    print('\nFile:',f,'rows=',n)
    if not missing_counts and not emptystr_counts:
        print('  No empty/missing columns')
    else:
        if missing_counts:
            print('  Missing (NaN) columns:')
            for c,v in missing_counts.items():
                print('   -',c,':',v,'({:.2%})'.format(v/n if n else 0))
        if emptystr_counts:
            print('  Empty-string columns:')
            for c,v in emptystr_counts.items():
                print('   -',c,':',v,'({:.2%})'.format(v/n if n else 0))
