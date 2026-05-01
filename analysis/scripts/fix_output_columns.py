from pathlib import Path
import pandas as pd
from pyproj import Transformer

FILES = [
    Path('analysis/output_new/merged_data.csv'),
    Path('analysis/output_new/testbaty-Outcome data_merged_auto.csv'),
]

def ensure_cols(p: Path):
    if not p.exists():
        return
    df = pd.read_csv(p)
    # easting/northing -> CoordinateX/Y
    if 'easting' in df.columns:
        df['CoordinateX'] = pd.to_numeric(df['easting'], errors='coerce')
    if 'northing' in df.columns:
        df['CoordinateY'] = pd.to_numeric(df['northing'], errors='coerce')
    # h
    if 'depth' in df.columns and 'h' not in df.columns:
        df['h'] = pd.to_numeric(df['depth'], errors='coerce')
    # GroundH(H)
    if 'GroundH(H)' not in df.columns:
        df['GroundH(H)'] = df.get('h')
    # Lat Lon: if not present, try to project from easting/northing (EPSG:32632)
    if ('Lat' not in df.columns or 'Lon' not in df.columns) and ('easting' in df.columns and 'northing' in df.columns):
        try:
            transformer = Transformer.from_crs('EPSG:32632', 'EPSG:4326', always_xy=True)
            xs = pd.to_numeric(df['easting'], errors='coerce').fillna(0).values
            ys = pd.to_numeric(df['northing'], errors='coerce').fillna(0).values
            lon, lat = transformer.transform(xs, ys)
            df['Lon'] = lon
            df['Lat'] = lat
        except Exception:
            df['Lon'] = df.get('Lon','')
            df['Lat'] = df.get('Lat','')
    # Locked/Sats/Status defaults
    if 'Locked' not in df.columns:
        df['Locked'] = 0
    if 'Sats' not in df.columns:
        df['Sats'] = 0
    if 'Status' not in df.columns:
        df['Status'] = ''
    # z_water / z_bed
    try:
        df['z_water'] = pd.to_numeric(df['GroundH(H)'], errors='coerce').fillna(0.0)
    except Exception:
        df['z_water'] = 0.0
    try:
        hnum = pd.to_numeric(df['h'], errors='coerce')
        if (hnum.dropna() < 0).mean() > 0.5:
            df['z_bed'] = df['z_water'] - hnum
        else:
            df['z_bed'] = df['z_water'] - hnum.abs()
    except Exception:
        df['z_bed'] = df['z_water']

    # reorder: put canonical columns first
    cols = list(df.columns)
    pref = ['CoordinateY','CoordinateX','h','GroundH(H)','Lat','Lon','Locked','Sats','Status','z_water','z_bed']
    newcols = [c for c in pref if c in df.columns] + [c for c in cols if c not in pref]
    df.to_csv(p, index=False, columns=newcols)
    print('Fixed', p)

def main():
    for p in FILES:
        ensure_cols(Path(p))

if __name__ == '__main__':
    main()
