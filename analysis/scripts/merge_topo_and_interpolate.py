"""Search for topo files (CSV/TXT) with X/Y/Z and interpolate z to ping locations.
Outputs augmented CSV with 'z_topo_nn' (nearest neighbor) and 'z_topo_idw' (IDW interpolation).
"""
import os
import pandas as pd
import numpy as np
from scipy.spatial import cKDTree, Delaunay

BASE='owendo-05-04-26-4-Outcome data_uzf'
# naive search for topo files
cands=[]
for root,dirs,files in os.walk(BASE):
    for f in files:
        if f.lower().endswith(('.csv','.txt')) and ('topo' in f.lower() or 'leve' in f.lower() or 'levee' in f.lower() or 'lev' in f.lower()):
            cands.append(os.path.join(root,f))
print('Found topo candidate files:',cands)
if not cands:
    print('No topo candidates found. Exiting.')
    raise SystemExit(0)

# pick first and try to load
topo = None
for p in cands:
    try:
        df_top=pd.read_csv(p)
        cols=[c.lower() for c in df_top.columns]
        if any(x in cols for x in ('x','easting','east','lon','longitude')) and any(x in cols for x in ('y','northing','north','lat','latitude')) and any(x in cols for x in ('z','height','elevation','alt')):
            print('Using topo file',p)
            # find col names
            def find(cols,opts):
                for o in opts:
                    if o in cols:
                        return cols[cols.index(o)]
                return None
            xcol=find(cols,['easting','east','x','lon','longitude'])
            ycol=find(cols,['northing','north','y','lat','latitude'])
            zcol=find(cols,['z','height','elevation','alt'])
            topo = df_top[[xcol,ycol,zcol]].rename(columns={xcol:'x',ycol:'y',zcol:'z'})
            break
    except Exception as e:
        print('failed to read',p,e)

if topo is None:
    print('Could not locate X/Y/Z columns in candidates.')
    raise SystemExit(0)

# load pings
PING_CSV='analysis/output_new/merged_data_with_owendo_cols.csv'
if not os.path.exists(PING_CSV):
    print('Ping CSV missing:',PING_CSV)
    raise SystemExit(1)

pings=pd.read_csv(PING_CSV)
# assume pings have 'easting' and 'northing'
px=pings['easting'].values
py=pings['northing'].values

# build KDTree
tree=cKDTree(topo[['x','y']].values)
dists, idx = tree.query(np.column_stack([px,py]), k=1)

pings['z_topo_nn']=topo['z'].values[idx]

# simple IDW with k nearest
k=6
dists_k, idxs = tree.query(np.column_stack([px,py]), k=k)
ids = np.array(idxs)

def idw(zs, ds, eps=1e-6):
    w = 1.0 / (ds+eps)
    return (w*zs).sum()/w.sum()

z_idw=[]
for i in range(len(px)):
    zs = topo['z'].values[idxs[i]]
    ds = dists_k[i]
    z_idw.append(idw(zs, ds))
pings['z_topo_idw']=z_idw

OUT='analysis/output_new/merged_data_with_topo.csv'
pings.to_csv(OUT,index=False)
print('Wrote',OUT)
