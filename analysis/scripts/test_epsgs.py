from pyproj import Transformer
import pandas as pd
import numpy as np
f='analysis/output_new/merged_data_with_owendo_cols.csv'
df=pd.read_csv(f)
false_east=500000.0
xs=df['easting'].values+false_east
ys=df['northing'].values
cands=[32628,32629,32630,32631,32632,32633]
for c in cands:
    try:
        t=Transformer.from_crs(f'EPSG:{c}','EPSG:4326',always_xy=True)
        lon,lat=t.transform(xs,ys)
        print(c, 'mean lon,lat:', float(np.mean(lon)), float(np.mean(lat)))
    except Exception as e:
        print('err',c,e)
