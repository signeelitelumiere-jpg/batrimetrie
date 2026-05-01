#!/usr/bin/env python3
"""Add z_water and z_bed to any CSV in analysis/output_new that contains depth-like columns.
"""
from pathlib import Path
import pandas as pd
import sys

OUT = Path('analysis') / 'output_new'

def has_depth(cols):
    for c in cols:
        if any(k in c.lower() for k in ('depth','h','prof')):
            return True
    return False

def add_z(p: Path):
    try:
        df = pd.read_csv(p)
    except Exception as e:
        print('Skip (read error):', p, e)
        return
    changed = False
    if 'z_water' not in df.columns:
        # try common fields
        if 'GroundH(H)' in df.columns:
            df['z_water'] = pd.to_numeric(df['GroundH(H)'], errors='coerce')
        elif 'altitude' in df.columns:
            df['z_water'] = pd.to_numeric(df['altitude'], errors='coerce')
        elif 'annerHigh' in df.columns:
            df['z_water'] = pd.to_numeric(df['annerHigh'], errors='coerce')
        else:
            df['z_water'] = 0.0
        changed = True
    if 'z_bed' not in df.columns and has_depth(df.columns):
        # find depth column
        depth_col = None
        for cand in df.columns:
            if any(k in cand.lower() for k in ('depth','h','prof')):
                depth_col = cand
                break
        if depth_col is not None:
            try:
                hnum = pd.to_numeric(df[depth_col], errors='coerce')
                if (hnum.dropna() < 0).mean() > 0.5:
                    df['z_bed'] = pd.to_numeric(df['z_water'], errors='coerce').fillna(0.0) - hnum
                else:
                    df['z_bed'] = pd.to_numeric(df['z_water'], errors='coerce').fillna(0.0) - hnum.abs()
            except Exception:
                df['z_bed'] = df['z_water']
        else:
            df['z_bed'] = df['z_water']
        changed = True

    if changed:
        bak = p.with_suffix(p.suffix + '.bak')
        try:
            if not bak.exists():
                p.replace(bak)
                df.to_csv(p, index=False)
                print('Updated and backed up:', p)
            else:
                from datetime import datetime
                ts = datetime.now().strftime('%Y%m%d%H%M%S')
                bak2 = p.with_name(p.stem + '_' + ts + p.suffix + '.bak')
                p.replace(bak2)
                df.to_csv(p, index=False)
                print('Updated and backed up as:', bak2)
        except Exception as e:
            print('Write failed for', p, e)
    else:
        print('No change needed:', p)

def main():
    targets = list(OUT.glob('*.csv'))
    if not targets:
        print('No CSVs found in', OUT)
        return
    for t in sorted(targets):
        add_z(t)

if __name__ == '__main__':
    main()
