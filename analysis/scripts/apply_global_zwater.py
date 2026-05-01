#!/usr/bin/env python3
"""
Apply a global z_water value to a merged OWENDO CSV and compute z_bed.
Usage: apply_global_zwater.py --merged path/to/merged.csv --zw -0.5
Writes output to same folder as merged file with suffix `_zw.csv`.
"""
import argparse
from pathlib import Path
import pandas as pd
import sys
import json


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--merged', required=True, help='Path to merged_data_with_owendo_cols.csv')
    p.add_argument('--zw', required=True, type=float, help='Global z_water value to apply (meters)')
    args = p.parse_args()

    merged = Path(args.merged)
    if not merged.exists():
        print('Merged file not found:', merged, file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(merged)
    df['z_water'] = float(args.zw)
    if 'h' in df.columns:
        df['z_bed'] = pd.to_numeric(df['z_water'], errors='coerce').fillna(0.0) + pd.to_numeric(df['h'], errors='coerce').fillna(0.0)
    else:
        df['z_bed'] = df['z_water']

    out = merged.with_name(merged.stem + '_zw' + merged.suffix)
    df.to_csv(out, index=False)
    print('Wrote', out)


if __name__ == '__main__':
    main()
