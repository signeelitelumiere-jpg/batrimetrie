#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import numpy as np
import sys

def check(path: Path):
    if not path.exists():
        print('File not found:', path)
        return
    df = pd.read_csv(path)
    n = len(df)
    print('File:', path)
    print('Rows:', n)
    print('\nFirst rows:')
    print(df.head(5).to_string(index=False))

    def stats(col):
        s = df[col].dropna()
        if s.empty:
            return 'no values'
        try:
            arr = pd.to_numeric(s, errors='coerce').dropna()
            return f'count={len(arr)}, min={arr.min():.6g}, max={arr.max():.6g}, mean={arr.mean():.6g}'
        except Exception:
            return f'unique={s.nunique()}'

    print('\nLat/Lon stats:')
    for c in ('latitude','longitude','Lat','Lon'):
        if c in df.columns:
            print(' ', c, stats(c))

    print('\nEasting/Northing stats:')
    for c in ('easting','nez_x','nez_y','northing'):
        if c in df.columns:
            print(' ', c, stats(c))

    print('\nAltitude / depth stats:')
    for c in ('altitude','annerHigh','high_depth','low_depth','depth','h'):
        if c in df.columns:
            print(' ', c, stats(c))

    # timestamps
    if 'utcTime' in df.columns:
        ut = df['utcTime'].dropna()
        print('\nutcTime sample (first 10):')
        print(ut.head(10).to_list())
        # try ms then s
        def try_conv(unit):
            try:
                conv = pd.to_datetime(ut.astype('int64'), unit=unit, origin='unix', errors='coerce')
                valid = conv.notna().sum()
                return valid
            except Exception:
                return 0
        vms = try_conv('ms')
        vs = try_conv('s')
        print('utcTime parsed as ms valid count:', vms, 'as s valid count:', vs)

    # quality indicators
    for c in ('hdop','vdop','satellite_visible','satellites','Locked','Sats'):
        if c in df.columns:
            print('\n', c, 'unique values sample:', df[c].dropna().unique()[:10])

    # duplicates
    dup = df.duplicated().mean()
    print('\nDuplicate rows fraction:', f'{dup:.3f}')

    # null ratio
    nulls = df.isna().mean().sort_values()
    print('\nTop columns with null fraction < 1.0:')
    print(nulls[nulls < 1.0].head(10))

    # simple verdict heuristics
    score = 0
    # lat/lon present and within plausible ranges
    if any(c in df.columns for c in ('latitude','longitude','Lat','Lon')):
        latc = 'latitude' if 'latitude' in df.columns else ('Lat' if 'Lat' in df.columns else None)
        lonc = 'longitude' if 'longitude' in df.columns else ('Lon' if 'Lon' in df.columns else None)
        if latc and lonc:
            lat = pd.to_numeric(df[latc], errors='coerce')
            lon = pd.to_numeric(df[lonc], errors='coerce')
            if lat.between(-90,90).any() and lon.between(-180,180).any():
                score += 1
    # timestamps
    if 'utcTime' in df.columns:
        try:
            if vms+vs > 0:
                score += 1
        except Exception:
            pass
    # non-trivial depth values
    depth_cols = [c for c in ('high_depth','low_depth','depth','h') if c in df.columns]
    if depth_cols:
        for c in depth_cols:
            arr = pd.to_numeric(df[c], errors='coerce').dropna()
            if (arr.abs() > 0).any():
                score += 1
                break

    print('\nSimple reality score (0-3):', score)
    if score >= 2:
        print('Verdict: ces données semblent plausibles / réelles (consistent values).')
    else:
        print('Verdict: les données semblent incomplètes ou générées (manquent champs essentiels ou valeurs plausibles).')

if __name__ == '__main__':
    p = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('analysis/output_new/testbaty-Outcome data_gps_data.csv')
    check(p)
