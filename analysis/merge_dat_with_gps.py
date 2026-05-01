from pathlib import Path
import pandas as pd
import numpy as np
import sqlite3

uzf_gps_csv = Path('analysis/output_new/testbaty-Outcome data_gps_data.csv')
dat_path = Path(r'bord de mer bathy/testbaty-南方数据.dat')
out_csv = Path('analysis/output_new/testbaty-Outcome data_merged_auto.csv')
owarn = False

# Read dat file (CSV-like)
if not dat_path.exists():
    raise SystemExit(f"DAT file not found: {dat_path}")

# try common encodings
raw = pd.read_csv(dat_path, header=None, dtype=str, encoding='utf-8', engine='python')
# normalize columns: expect at least 3-5 columns
cols = raw.shape[1]
if cols >= 5:
    raw = raw.iloc[:, :5]
    raw.columns = ['depth','_skip','easting','northing','alt_raw']
elif cols == 4:
    raw.columns = ['depth','_skip','easting','northing']
    raw['alt_raw'] = np.nan
elif cols == 3:
    raw.columns = ['depth','easting','northing']
    raw['_skip'] = np.nan
    raw['alt_raw'] = np.nan
else:
    # fallback: try splitting by whitespace
    s = dat_path.read_text(encoding='utf-8', errors='replace')
    lines = [ln.strip().split() for ln in s.splitlines() if ln.strip()]
    raw = pd.DataFrame(lines)
    if raw.shape[1] >= 3:
        raw = raw.iloc[:, :5]
        raw.columns = ['depth','_skip','easting','northing','alt_raw']

# coerce numeric
for c in ['depth','easting','northing','alt_raw']:
    if c in raw.columns:
        raw[c] = pd.to_numeric(raw[c], errors='coerce')

# Build output base
m = pd.DataFrame()
if 'easting' in raw.columns:
    m['easting'] = raw['easting']
if 'northing' in raw.columns:
    m['northing'] = raw['northing']
if 'depth' in raw.columns:
    m['depth'] = raw['depth']

# Try to enrich from gps CSV if available
if uzf_gps_csv.exists():
    gps = pd.read_csv(uzf_gps_csv)
    # gps file columns include 'nez_x','nez_y','latitude','longitude','utcTime','altitude','annerHigh'
    # choose coordinate fields
    if 'nez_x' in gps.columns and 'nez_y' in gps.columns:
        gps_coords = gps[['nez_x','nez_y']].astype(float)
        dat_coords = m[['easting','northing']].astype(float)
        try:
            from scipy.spatial import cKDTree as KDTree
            tree = KDTree(gps_coords.values)
            dists, idxs = tree.query(dat_coords.values, k=1)
            nearest = gps.iloc[idxs].reset_index(drop=True)
        except Exception:
            # fallback: brute-force nearest (slower)
            nearest_rows = []
            gvals = gps_coords.values
            for px,py in dat_coords.values:
                d2 = np.sum((gvals - np.array([px,py]))**2, axis=1)
                nearest_rows.append(gps.iloc[int(np.argmin(d2))])
            nearest = pd.DataFrame(nearest_rows).reset_index(drop=True)
        # attach fields
        if 'latitude' in nearest.columns:
            m['Lat'] = pd.to_numeric(nearest['latitude'], errors='coerce')
        if 'longitude' in nearest.columns:
            m['Lon'] = pd.to_numeric(nearest['longitude'], errors='coerce')
        # datetime from utcTime
        if 'utcTime' in nearest.columns:
            try:
                m['datetime'] = pd.to_datetime(nearest['utcTime'], unit='ms', errors='coerce')
                if m['datetime'].isna().all():
                    m['datetime'] = pd.to_datetime(nearest['utcTime'], unit='s', errors='coerce')
            except Exception:
                m['datetime'] = pd.NaT
        # GroundH
        gh = None
        if 'altitude' in nearest.columns:
            gh = pd.to_numeric(nearest['altitude'], errors='coerce')
        elif 'annerHigh' in nearest.columns:
            gh = pd.to_numeric(nearest['annerHigh'], errors='coerce')
        if gh is not None:
            m['GroundH(H)'] = gh

# compute z_water and z_bed
if 'GroundH(H)' in m.columns:
    m['z_water'] = m['GroundH(H)']
else:
    m['z_water'] = 0.0
if 'depth' in m.columns:
    m['z_bed'] = m['z_water'] - m['depth']

# save merged into the UZF merged_auto path (overwrite)
out_csv.parent.mkdir(parents=True, exist_ok=True)
m.to_csv(out_csv, index=False)

# write OWENDO txt header
ow_txt = out_csv.with_name(out_csv.stem + '_OWENDO-BATHY-SURVEY-generated.txt')
header = 'CoordinateY CoordinateX h GroundH(H) Lat Lon Locked Sats Status'
lines = [header]
for _, rr in m.iterrows():
    cy = rr.get('northing','')
    cx = rr.get('easting','')
    h = rr.get('depth','')
    ghv = rr.get('GroundH(H)','')
    lat = rr.get('Lat','')
    lon = rr.get('Lon','')
    lines.append(f"{cy} {cx} {h} {ghv} {lat} {lon} 0 0 0")
ow_txt.write_text('\n'.join(lines), encoding='utf-8')

print('Wrote', out_csv)
print('Rows:', len(m))
