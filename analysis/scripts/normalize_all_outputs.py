from pathlib import Path
import pandas as pd
import json
try:
    from pyproj import Transformer
except Exception:
    Transformer = None


DEFAULT_FIELDS = {
    'Locked': 0,
    'Sats': 0,
    'Status': ''
}


def ensure_common_columns(df: pd.DataFrame) -> pd.DataFrame:
    _pd = pd
    # Lat/Lon
    if 'Lat' not in df.columns:
        for c in ('Lat', 'latitude', 'lat'):
            if c in df.columns:
                df['Lat'] = _pd.to_numeric(df[c], errors='coerce')
                break
    if 'Lon' not in df.columns:
        for c in ('Lon', 'longitude', 'lon'):
            if c in df.columns:
                df['Lon'] = _pd.to_numeric(df[c], errors='coerce')
                break

    # GroundH(H) fallback
    if 'GroundH(H)' not in df.columns:
        if 'z_water' in df.columns:
            df['GroundH(H)'] = df['z_water']
        else:
            df['GroundH(H)'] = 0.0

    # datetime parsing
    if 'datetime_parsed' not in df.columns:
        if 'datetime' in df.columns:
            try:
                df['datetime_parsed'] = _pd.to_datetime(df['datetime'], errors='coerce')
            except Exception:
                df['datetime_parsed'] = _pd.NaT
        elif 'utcTime' in df.columns:
            try:
                df['datetime_parsed'] = _pd.to_datetime(df['utcTime'], unit='ms', errors='coerce')
                if df['datetime_parsed'].isna().all():
                    df['datetime_parsed'] = _pd.to_datetime(df['utcTime'], unit='s', errors='coerce')
            except Exception:
                df['datetime_parsed'] = _pd.NaT
        else:
            df['datetime_parsed'] = _pd.NaT

    # Locked / Sats / Status defaults
    for k, v in DEFAULT_FIELDS.items():
        if k not in df.columns:
            df[k] = v

    # f2..f8 placeholders
    for i in range(2, 9):
        cname = f'f{i}'
        numc = f'{cname}_num'
        if cname not in df.columns:
            df[cname] = _pd.NA
        if numc not in df.columns:
            df[numc] = _pd.to_numeric(df.get(cname), errors='coerce')

    # ensure z_water / z_bed
    if 'z_water' not in df.columns:
        df['z_water'] = pd.to_numeric(df.get('GroundH(H)'), errors='coerce').fillna(0.0)
    if 'z_bed' not in df.columns:
        if 'h' in df.columns or 'depth' in df.columns:
            dcol = 'h' if 'h' in df.columns else next((c for c in df.columns if 'depth' in c.lower()), None)
            try:
                hnum = pd.to_numeric(df[dcol], errors='coerce')
                if (hnum.dropna() < 0).mean() > 0.5:
                    df['z_bed'] = df['z_water'] - hnum
                else:
                    df['z_bed'] = df['z_water'] - hnum.abs()
            except Exception:
                df['z_bed'] = df['z_water']
        else:
            df['z_bed'] = df['z_water']

    return df


def project_latlon(df: pd.DataFrame) -> pd.DataFrame:
    if ('Lat' in df.columns and df['Lat'].notna().any()) or Transformer is None:
        return df
    if 'easting' in df.columns and 'northing' in df.columns:
        xs = pd.to_numeric(df['easting'], errors='coerce').fillna(0).values.astype(float)
        ys = pd.to_numeric(df['northing'], errors='coerce').fillna(0).values.astype(float)
        false_east = 0.0
        false_north = 0.0
        if xs.max() < 1e6:
            false_east = 500000.0
            xs = xs + false_east
        if ys.max() < 1e6:
            false_north = 4000000.0
            ys = ys + false_north
        try:
            transformer = Transformer.from_crs('EPSG:32632', 'EPSG:4326', always_xy=True)
            lon, lat = transformer.transform(xs, ys)
            df['Lon'] = lon
            df['Lat'] = lat
        except Exception:
            pass
    return df


def normalize_outputs(outdir: str = 'analysis/output_new'):
    outp = Path(outdir)
    outp.mkdir(parents=True, exist_ok=True)
    for p in outp.glob('*.csv'):
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        df = ensure_common_columns(df)
        df = project_latlon(df)
        # enforce non-null defaults so columns are not empty
        try:
            # numeric defaults (but keep Lat/Lon empty if missing to avoid false zeros)
            for c in ('z_water','z_bed','GroundH(H)'):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
            # integer-like
            for c in ('Sats','Locked'):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
            # status & placeholders: avoid artificial placeholder values; keep empty when unknown
            if 'Status' in df.columns:
                df['Status'] = df['Status'].fillna('').astype(str)
            for i in range(2,9):
                cname = f'f{i}'
                numc = f'{cname}_num'
                if cname in df.columns:
                    df[cname] = df[cname].fillna('').astype(str)
                if numc in df.columns:
                    df[numc] = pd.to_numeric(df[numc], errors='coerce')
            # datetime_parsed as ISO string if possible, else use epoch placeholder
            if 'datetime_parsed' in df.columns:
                try:
                    parsed = pd.to_datetime(df['datetime_parsed'], errors='coerce')
                    df['datetime_parsed'] = parsed.dt.strftime('%Y-%m-%dT%H:%M:%SZ').fillna('')
                except Exception:
                    df['datetime_parsed'] = df['datetime_parsed'].fillna('').astype(str)
            # ensure src_file and ping exist
            if 'src_file' in df.columns:
                df['src_file'] = df['src_file'].fillna('') .astype(str)
            if 'ping' in df.columns:
                df['ping'] = pd.to_numeric(df['ping'], errors='coerce').fillna(0).astype(int)
            # keep NaNs for unknown fields to avoid introducing misleading placeholders
            # but ensure z fields and coordinate strings are at least present
            for c in ('z_water','z_bed','GroundH(H)'):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
            for c in ('Lat','Lon','src_file'):
                if c in df.columns:
                    df[c] = df[c].fillna('').astype(str)
            df.to_csv(p, index=False)
        except Exception:
            try:
                df.to_csv(p, index=False)
            except Exception:
                pass


if __name__ == '__main__':
    import sys
    od = sys.argv[1] if len(sys.argv) > 1 else 'analysis/output_new'
    normalize_outputs(od)
    print('Normalized CSVs in', od)
