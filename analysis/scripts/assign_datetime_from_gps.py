"""Assign datetime and GNSS lat/lon to OWENDO merged rows using nearest GPS samples.

Algorithm:
- Load `analysis/output_new/merged_data_with_owendo_cols.csv`.
- Load all GPS CSVs under `analysis/output_new` (files matching '*_gps_data.csv' or '*_gps.csv').
- Merge GPS rows into one DF; parse utcTime to datetime (ms or s); ensure numeric nez_x/nez_y and latitude/longitude.
- Build KDTree on GPS easting/northing (use `nez_x`/`nez_y` if available), fall back to projecting lat/lon if not.
- For each OWENDO ping row with CoordinateX/CoordinateY, find nearest GPS sample and assign `datetime` and `Lat`/`Lon` where missing.
- Write back updated `merged_data_with_owendo_cols.csv` and regenerate `merged_data_with_owendo_cols_z.csv` (recompute z_water/z_bed).
"""
from pathlib import Path
import pandas as pd
import numpy as np

OUT = Path('analysis/output_new/merged_data_with_owendo_cols.csv')
OUT_Z = Path('analysis/output_new/merged_data_with_owendo_cols_z.csv')
GPS_GLOB = list(Path('analysis/output_new').glob('*_gps_data.csv')) + list(Path('analysis/output_new').glob('*_gps.csv'))

if not OUT.exists():
    print('No OWENDO merged CSV found at', OUT)
    raise SystemExit(1)

if not GPS_GLOB:
    print('No GPS CSVs found under analysis/output_new; aborting assignment')
    raise SystemExit(0)

print('Found GPS files:', GPS_GLOB)
# load and concat GPS files
gps_frames = []
for p in GPS_GLOB:
    try:
        g = pd.read_csv(p)
        g['_srcfile'] = p.name
        gps_frames.append(g)
    except Exception as e:
        print('Failed to read', p, e)
if not gps_frames:
    print('No readable GPS frames')
    raise SystemExit(0)

gps = pd.concat(gps_frames, ignore_index=True, sort=False)
# detect coordinate columns
cols = [c.lower() for c in gps.columns]
lat_col = None
lon_col = None
for opt in ('latitude','lat'):
    if opt in cols:
        lat_col = gps.columns[cols.index(opt)]
        break
for opt in ('longitude','lon','lng'):
    if opt in cols:
        lon_col = gps.columns[cols.index(opt)]
        break
# detect easting/northing
nezx = None
nezy = None
for opt in ('nez_x','easting','east','x'):
    if opt in cols:
        nezx = gps.columns[cols.index(opt)]
        break
for opt in ('nez_y','northing','north','y'):
    if opt in cols:
        nezy = gps.columns[cols.index(opt)]
        break

# parse utcTime if present
time_col = None
for opt in ('utctime','utcTime','time','timestamp'):
    if opt in gps.columns or opt in [c.lower() for c in gps.columns]:
        # find actual casing
        for c in gps.columns:
            if c.lower() == opt:
                time_col = c
                break
        if time_col:
            break

# try parsing time as ms or s
if time_col is not None:
    try:
        gps['__t_ms'] = pd.to_datetime(gps[time_col], unit='ms', errors='coerce')
        if gps['__t_ms'].isna().all():
            gps['__t_ms'] = pd.to_datetime(gps[time_col], unit='s', errors='coerce')
    except Exception:
        gps['__t_ms'] = pd.NaT
else:
    gps['__t_ms'] = pd.NaT

# ensure numeric easting/northing; if not, but lat/lon present we won't build KDTree on them
if nezx and nezy:
    gps['__ex'] = pd.to_numeric(gps[nezx], errors='coerce')
    gps['__ey'] = pd.to_numeric(gps[nezy], errors='coerce')
else:
    gps['__ex'] = np.nan
    gps['__ey'] = np.nan

# prepare OWENDO merged
ow = pd.read_csv(OUT)
# reset index so positional indexing below matches 0..N-1
ow = ow.reset_index(drop=True)
# ensure CoordinateX/Y exist
if 'CoordinateX' not in ow.columns or 'CoordinateY' not in ow.columns:
    print('OWENDO merged file missing CoordinateX/CoordinateY')
    raise SystemExit(1)
ow['CoordinateX'] = pd.to_numeric(ow['CoordinateX'], errors='coerce')
ow['CoordinateY'] = pd.to_numeric(ow['CoordinateY'], errors='coerce')

# Build KDTree if possible
use_spatial = gps['__ex'].notna().any()

from scipy.spatial import cKDTree
from pyproj import Transformer

assigned = 0
if use_spatial:
    pts_g = np.column_stack([gps['__ex'].fillna(0).values, gps['__ey'].fillna(0).values])
    tree = cKDTree(pts_g)
    pts_ow = np.column_stack([ow['CoordinateX'].fillna(0).values, ow['CoordinateY'].fillna(0).values])
    dists, idxs = tree.query(pts_ow, k=1)
    # assign datetime if gps has __t_ms
    for i_pos, idx in enumerate(idxs):
        if idx is None or np.isnan(idx):
            continue
        grow = gps.iloc[int(idx)]
        if pd.notna(grow.get('__t_ms')) and (('datetime' not in ow.columns) or pd.isna(ow.at[i_pos,'datetime']) or ow.at[i_pos,'datetime']==''):
            ow.at[i_pos,'datetime'] = pd.to_datetime(grow['__t_ms'])
            assigned += 1
        # assign Lat/Lon if missing and gps has them
        if (('Lat' not in ow.columns) or pd.isna(ow.at[i_pos,'Lat']) or ow.at[i_pos,'Lat']=='') and lat_col:
            try:
                ow.at[i_pos,'Lat'] = float(grow[lat_col])
                ow.at[i_pos,'Lon'] = float(grow[lon_col])
            except Exception:
                pass
else:
    # try projecting gps lat/lon to EPSG:32632 and build tree
    if lat_col and lon_col:
        transformer = Transformer.from_crs('EPSG:4326','EPSG:32632', always_xy=True)
        lons = pd.to_numeric(gps[lon_col], errors='coerce').fillna(0).values
        lats = pd.to_numeric(gps[lat_col], errors='coerce').fillna(0).values
        gx, gy = transformer.transform(lons, lats)
        pts_g = np.column_stack([gx, gy])
        tree = cKDTree(pts_g)
        pts_ow = np.column_stack([ow['CoordinateX'].fillna(0).values, ow['CoordinateY'].fillna(0).values])
        dists, idxs = tree.query(pts_ow, k=1)
        for i_pos, idx in enumerate(idxs):
            grow = gps.iloc[int(idx)]
            if pd.notna(grow.get('__t_ms')) and (('datetime' not in ow.columns) or pd.isna(ow.at[i_pos,'datetime']) or ow.at[i_pos,'datetime']==''):
                ow.at[i_pos,'datetime'] = pd.to_datetime(grow['__t_ms'])
                assigned += 1
            if (('Lat' not in ow.columns) or pd.isna(ow.at[i_pos,'Lat']) or ow.at[i_pos,'Lat']=='') and lat_col:
                try:
                    ow.at[i_pos,'Lat'] = float(grow[lat_col])
                    ow.at[i_pos,'Lon'] = float(grow[lon_col])
                except Exception:
                    pass

print('Assigned datetime to', assigned, 'rows from GPS nearest neighbor')

# normalize z columns
if 'GroundH(H)' in ow.columns:
    ow['z_water'] = pd.to_numeric(ow['GroundH(H)'], errors='coerce').fillna(0.0)
else:
    ow['z_water'] = 0.0
if 'h' in ow.columns:
    try:
        hnum = pd.to_numeric(ow['h'], errors='coerce')
        if (hnum.dropna() < 0).mean() > 0.5:
            ow['z_bed'] = ow['z_water'] - hnum
        else:
            ow['z_bed'] = ow['z_water'] - hnum.abs()
    except Exception:
        ow['z_bed'] = ow['z_water']
else:
    ow['z_bed'] = ow['z_water']

# write back
ow.to_csv(OUT, index=False)
ow.to_csv(OUT_Z, index=False)
print('Wrote updated OWENDO merged files:', OUT, OUT_Z)
