#!/usr/bin/env python3
"""Add z_water and z_bed columns to canonical GPS CSV files.

Usage:
    python add_z_to_gps.py <csv-path>
If no path provided, processes all *_gps_data.csv files under analysis/output_new.
"""
from pathlib import Path
import sys
import pandas as pd

def compute_z_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # try to infer z_water from common altitude-like fields
    if 'z_water' not in df.columns:
        if 'GroundH(H)' in df.columns:
            df['z_water'] = pd.to_numeric(df['GroundH(H)'], errors='coerce')
        elif 'altitude' in df.columns:
            df['z_water'] = pd.to_numeric(df['altitude'], errors='coerce')
        elif 'annerHigh' in df.columns:
            df['z_water'] = pd.to_numeric(df['annerHigh'], errors='coerce')
        else:
            df['z_water'] = 0.0

    # find depth column candidates
    depth_col = None
    for cand in ('depth','Depth','DEPTH','h','high_depth','low_depth','point_depth','depth_m'):
        if cand in df.columns:
            depth_col = cand
            break

    # compute z_bed: z_water - depth (assumes depth positive downwards; preserves sign if negative)
    if 'z_bed' not in df.columns:
        if depth_col is not None:
            df['z_bed'] = pd.to_numeric(df['z_water'], errors='coerce').fillna(0.0) - pd.to_numeric(df[depth_col], errors='coerce').fillna(0.0)
        else:
            df['z_bed'] = df['z_water']

    return df

def process_file(p: Path):
    if not p.exists():
        print('Not found:', p)
        return
    print('Processing', p)
    df = pd.read_csv(p)
    df2 = compute_z_columns(df)
    # backup
    bak = p.with_suffix(p.suffix + '.bak')
    try:
        if not bak.exists():
            p.replace(bak)
            # write new file
            df2.to_csv(p, index=False)
            print('Wrote', p, ' (original moved to', bak, ')')
        else:
            # if bak exists, don't overwrite backup: write new file and also write a timestamped backup
            from datetime import datetime
            ts = datetime.now().strftime('%Y%m%d%H%M%S')
            bak2 = p.with_name(p.stem + '_' + ts + p.suffix + '.bak')
            p.replace(bak2)
            df2.to_csv(p, index=False)
            print('Wrote', p, ' (original moved to', bak2, ')')
    except Exception as e:
        # fallback: write to a .new file
        print('Backup move failed:', e)
        newp = p.with_suffix(p.suffix + '.new')
        df2.to_csv(newp, index=False)
        print('Wrote new file', newp)

def main():
    args = sys.argv[1:]
    if args:
        targets = [Path(args[0])]
    else:
        targets = list(Path('analysis/output_new').glob('*_gps_data.csv'))
    if not targets:
        print('No target GPS CSVs found.')
        return
    for t in targets:
        process_file(t)

if __name__ == '__main__':
    main()
