from pathlib import Path
import pandas as pd
import numpy as np
import re

OUT_DIR = Path('analysis/output_new')

def extract_num(s):
    if pd.isna(s):
        return np.nan
    try:
        return float(s)
    except Exception:
        m = re.search(r"[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?", str(s))
        if m:
            try:
                return float(m.group(0))
            except Exception:
                return np.nan
    return np.nan

def ensure_columns(df: pd.DataFrame):
    # CoordinateX/Y
    if 'CoordinateX' not in df.columns and 'easting' in df.columns:
        df['CoordinateX'] = pd.to_numeric(df['easting'], errors='coerce')
    if 'CoordinateY' not in df.columns and 'northing' in df.columns:
        df['CoordinateY'] = pd.to_numeric(df['northing'], errors='coerce')
    # h from depth
    if 'h' not in df.columns:
        if 'depth' in df.columns:
            df['h'] = pd.to_numeric(df['depth'], errors='coerce')
        else:
            df['h'] = pd.NA
    # GroundH(H)
    if 'GroundH(H)' not in df.columns:
        if 'z_water' in df.columns:
            df['GroundH(H)'] = df['z_water']
        else:
            df['GroundH(H)'] = df['h']
    # Lat/Lon: attempt to compute if missing and easting/northing exist
    if ('Lat' not in df.columns or 'Lon' not in df.columns) and 'easting' in df.columns and 'northing' in df.columns:
        try:
            from pyproj import Transformer
            xs = pd.to_numeric(df['easting'], errors='coerce').fillna(0.0).values
            ys = pd.to_numeric(df['northing'], errors='coerce').fillna(0.0).values
            if xs.max() > 180 or ys.max() > 90:
                t = Transformer.from_crs('EPSG:32632', 'EPSG:4326', always_xy=True)
                lon, lat = t.transform(xs, ys)
                df['Lon'] = lon
                df['Lat'] = lat
        except Exception:
            if 'Lat' not in df.columns:
                df['Lat'] = ''
            if 'Lon' not in df.columns:
                df['Lon'] = ''
    else:
        if 'Lat' not in df.columns:
            df['Lat'] = ''
        if 'Lon' not in df.columns:
            df['Lon'] = ''
    # Locked / Sats / Status defaults
    if 'Sats' not in df.columns:
        df['Sats'] = 0
    else:
        df['Sats'] = pd.to_numeric(df['Sats'], errors='coerce').fillna(0).astype(int)
    if 'Locked' not in df.columns:
        df['Locked'] = df['Sats'].apply(lambda x: 1 if x>0 else 0)
    if 'Status' not in df.columns:
        df['Status'] = ''
    # z_water / z_bed
    if 'z_water' not in df.columns:
        df['z_water'] = pd.to_numeric(df.get('GroundH(H)'), errors='coerce').fillna(0.0)
    if 'z_bed' not in df.columns:
        try:
            hnum = pd.to_numeric(df['h'], errors='coerce')
            if (hnum.dropna() < 0).mean() > 0.5:
                df['z_bed'] = df['z_water'] + hnum
            else:
                df['z_bed'] = df['z_water'] - hnum.abs()
        except Exception:
            df['z_bed'] = df['z_water']
    # f*_num extraction
    for i in range(2,9):
        fn = f'f{i}'
        nm = f'f{i}_num'
        if fn in df.columns and nm not in df.columns:
            df[nm] = df[fn].apply(extract_num)
        if fn not in df.columns:
            df[fn] = pd.NA
            if nm not in df.columns:
                df[nm] = np.nan
    # datetime_parsed
    if 'datetime_parsed' not in df.columns:
        if 'datetime' in df.columns:
            def try_dt(v):
                for fmt in ("%Y-%m-%d %H:%M:%S.%f","%Y-%m-%d %H:%M:%S,%f","%Y-%m-%d %H:%M:%S"):
                    try:
                        return pd.to_datetime(v, format=fmt)
                    except Exception:
                        pass
                try:
                    return pd.to_datetime(v, errors='coerce')
                except Exception:
                    return pd.NaT
            df['datetime_parsed'] = df['datetime'].apply(try_dt)
        else:
            df['datetime_parsed'] = pd.NaT
    return df

def main(force: bool = False):
    if not OUT_DIR.exists():
        print('No output dir')
        return 1
    csvs = list(OUT_DIR.glob('*.csv'))
    for p in csvs:
        marker = p.with_name(p.name + '.normalized')
        if marker.exists() and not force:
            print(f'Skipping {p.name}: already normalized')
            continue
        try:
            df = pd.read_csv(p)
            before = set(df.columns)
            df2 = ensure_columns(df)
            after = set(df2.columns)
            df2.to_csv(p, index=False)
            # create marker file to indicate normalization done
            try:
                marker.write_text(f'normalized: {time.asctime()}')
            except Exception:
                pass
            added = sorted(list(after - before))
            if added:
                print(f'Updated {p.name}: added {added}')
            else:
                print(f'Checked {p.name}: no columns added')
        except Exception as e:
            print('Failed', p.name, e)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
