from pathlib import Path
import pandas as pd
OUT_DIR = Path('analysis/output_new')
ow_path = OUT_DIR / 'merged_data_with_owendo_cols.csv'
if not ow_path.exists():
    print('Source merged_data_with_owendo_cols.csv not found')
    raise SystemExit(1)
df = pd.read_csv(ow_path)
# subset columns
cols_subset = ['src_file','ping','datetime','datetime_parsed','easting','northing','depth','f2','f3','f4']
subset = [c for c in cols_subset if c in df.columns]
df_subset = df[subset].copy()
# write merged_data.csv
merged_path = OUT_DIR / 'merged_data.csv'
df_subset.to_csv(merged_path, index=False)
# write previews
try:
    df.head(100).to_csv(OUT_DIR / 'preview_merged_new_head100.csv', index=False)
except Exception:
    pass
try:
    df.head(100).to_csv(OUT_DIR / 'preview_owendo_head100.csv', index=False)
except Exception:
    pass
try:
    df[['Lat','Lon']].head(100).to_csv(OUT_DIR / 'preview_latlon_head100.csv', index=False)
except Exception:
    pass
print('Regenerated merged_data.csv and previews')
