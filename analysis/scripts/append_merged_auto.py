import pandas as pd, glob, os
from pathlib import Path
OUT='analysis/output_new'
merged_target = Path(OUT) / 'merged_data_with_owendo_cols.csv'
merged_simple = Path(OUT) / 'merged_data.csv'
files = glob.glob(os.path.join(OUT,'*_merged_auto.csv'))
print('Found merged_auto files:', files)

def ensure_common_columns(df):
    import pandas as _pd
    for c in ('Lat','latitude'):
        if c in df.columns:
            df['Lat'] = _pd.to_numeric(df[c], errors='coerce')
            break
    for c in ('Lon','longitude'):
        if c in df.columns:
            df['Lon'] = _pd.to_numeric(df[c], errors='coerce')
            break
    if 'GroundH(H)' not in df.columns:
        if 'z_water' in df.columns:
            df['GroundH(H)'] = df['z_water']
        else:
            df['GroundH(H)'] = 0.0
    if 'datetime' not in df.columns and 'utcTime' in df.columns:
        try:
            df['datetime'] = _pd.to_datetime(df['utcTime'], unit='ms', errors='coerce')
            if df['datetime'].isna().all():
                df['datetime'] = _pd.to_datetime(df['utcTime'], unit='s', errors='coerce')
        except Exception:
            df['datetime'] = _pd.NaT
    elif 'datetime' not in df.columns:
        df['datetime'] = _pd.NaT
    if 'Locked' not in df.columns:
        df['Locked'] = 0
    if 'Sats' not in df.columns:
        df['Sats'] = 0
    if 'Status' not in df.columns:
        df['Status'] = ''
    for i in range(2,9):
        cname = f'f{i}'
        numc = f'f{i}_num'
        if cname not in df.columns:
            df[cname] = _pd.NA
        if numc not in df.columns:
            df[numc] = _pd.to_numeric(df.get(cname), errors='coerce')
    if 'z_water' not in df.columns:
        df['z_water'] = _pd.to_numeric(df['GroundH(H)'], errors='coerce').fillna(0.0)
    if 'z_bed' not in df.columns:
        if 'h' in df.columns:
            try:
                hnum = _pd.to_numeric(df['h'], errors='coerce')
                if (hnum.dropna() < 0).mean() > 0.5:
                    df['z_bed'] = df['z_water'] - hnum
                else:
                    df['z_bed'] = df['z_water'] - hnum.abs()
            except Exception:
                df['z_bed'] = df['z_water']
        else:
            df['z_bed'] = df['z_water']
    return df

all_added = 0
for f in files:
    try:
        df = pd.read_csv(f)
        df = ensure_common_columns(df)
        # append to merged_target
        if merged_target.exists():
            df_exist = pd.read_csv(merged_target)
            combined = pd.concat([df_exist, df], ignore_index=True, sort=False)
            combined.to_csv(merged_target, index=False)
        else:
            df.to_csv(merged_target, index=False)
        # append subset to merged_simple
        cols_subset = ['src_file','ping','datetime','datetime_parsed','easting','northing','depth','f2','f3','f4']
        subset = df[[c for c in cols_subset if c in df.columns]]
        if merged_simple.exists():
            dfm = pd.read_csv(merged_simple)
            dfm_comb = pd.concat([dfm, subset], ignore_index=True, sort=False)
            dfm_comb.to_csv(merged_simple, index=False)
        else:
            subset.to_csv(merged_simple, index=False)
        all_added += len(df)
        print('Appended',f,'rows=',len(df))
    except Exception as e:
        print('Failed to append',f,e)
print('Total appended rows:', all_added)
