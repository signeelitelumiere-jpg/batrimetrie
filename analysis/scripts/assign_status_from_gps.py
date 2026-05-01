from pathlib import Path
import pandas as pd
import numpy as np
try:
    from scipy.spatial import cKDTree as KDTree
except Exception:
    KDTree = None

GPS_CAND = Path('analysis/output_new/testbaty-Outcome data_gps_data.csv')
TARGETS = [
    Path('analysis/output_new/merged_data.csv'),
    Path('analysis/output_new/testbaty-Outcome data_merged_auto.csv'),
]

def read_gps(path: Path):
    if not path.exists():
        return None
    df = pd.read_csv(path)
    # prefer Lat/Lon columns
    latc = next((c for c in df.columns if c.lower().startswith('lat')), None)
    lonc = next((c for c in df.columns if c.lower().startswith('lon')), None)
    if latc and lonc:
        df['__lat'] = pd.to_numeric(df[latc], errors='coerce')
        df['__lon'] = pd.to_numeric(df[lonc], errors='coerce')
    else:
        # try nez_x / nez_y
        if 'nez_x' in df.columns and 'nez_y' in df.columns:
            df['__lon'] = pd.to_numeric(df['nez_x'], errors='coerce')
            df['__lat'] = pd.to_numeric(df['nez_y'], errors='coerce')
    # sats
    if 'satellite_visible' in df.columns:
        df['__sats'] = pd.to_numeric(df['satellite_visible'], errors='coerce')
    elif 'Sats' in df.columns:
        df['__sats'] = pd.to_numeric(df['Sats'], errors='coerce')
    else:
        df['__sats'] = pd.NA
    # status raw
    if 'solution_type' in df.columns:
        df['__status_raw'] = df['solution_type'].astype(str)
    else:
        df['__status_raw'] = ''
    return df.dropna(subset=['__lat','__lon'])

def map_status_from_sats(s):
    try:
        s = int(s)
    except Exception:
        return ''
    if s >= 8:
        return 'RTK'
    if s >= 5:
        return 'DGPS'
    if s > 0:
        return 'SPS'
    return ''

def assign(gps_df, target_path: Path):
    if not gps_df is not None and not target_path.exists():
        return
    df = pd.read_csv(target_path)
    # obtain target coords (prefer Lat/Lon else easting/northing projected)
    if 'Lat' in df.columns and 'Lon' in df.columns and df['Lat'].notna().any():
        tgt_lat = pd.to_numeric(df['Lat'], errors='coerce')
        tgt_lon = pd.to_numeric(df['Lon'], errors='coerce')
    elif 'easting' in df.columns and 'northing' in df.columns:
        tgt_lon = pd.to_numeric(df['easting'], errors='coerce')
        tgt_lat = pd.to_numeric(df['northing'], errors='coerce')
    else:
        # nothing to map
        return

    # prepare points
    pts = np.vstack([tgt_lon.fillna(0).values, tgt_lat.fillna(0).values]).T
    gpts = np.vstack([gps_df['__lon'].values, gps_df['__lat'].values]).T
    if KDTree is None:
        # brute force
        idxs = []
        for p in pts:
            d2 = np.sum((gpts - p) ** 2, axis=1)
            idxs.append(int(np.argmin(d2)))
        idxs = np.array(idxs)
    else:
        tree = KDTree(gpts)
        dists, idxs = tree.query(pts, k=1)

    # lookup and assign
    sats_list = gps_df['__sats'].fillna(0).astype(float).values
    status_raw = gps_df['__status_raw'].astype(str).values

    assigned_sats = []
    assigned_status = []
    for i in idxs:
        try:
            s = sats_list[i]
        except Exception:
            s = 0
        raw = status_raw[i] if i < len(status_raw) else ''
        if raw and raw.strip():
            st = raw
        else:
            st = map_status_from_sats(s)
        assigned_sats.append(int(s) if not pd.isna(s) else 0)
        assigned_status.append(st)

    df['Sats'] = assigned_sats
    df['Status'] = assigned_status
    df['Locked'] = df['Sats'].apply(lambda x: 1 if (pd.notna(x) and int(x) > 0) else 0)

    df.to_csv(target_path, index=False)
    print('Assigned Status/Sats to', target_path)

def main():
    gps = read_gps(GPS_CAND)
    if gps is None or gps.empty:
        print('GPS canonical file not found or empty:', GPS_CAND)
        return 2
    for t in TARGETS:
        if t.exists():
            assign(gps, t)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
