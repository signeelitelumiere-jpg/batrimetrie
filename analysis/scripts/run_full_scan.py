"""
Run the full pre-scan: import .dat topo/sonar files and generate the canonical CSV outputs:
 - analysis/output_new/merged_data.csv
 - analysis/output_new/merged_data_with_owendo_cols.csv
 - analysis/output_new/merged_data_with_latlon.csv
 - analysis/output_new/merged_data_latlon_epsg32632.csv

This script is conservative: it appends imported rows from any found `.dat` under the OWENDO folder,
mapping columns as: col3 -> northing, col4 -> easting, col5 -> depth.
"""
import os
import glob
import pandas as pd
from pathlib import Path
import subprocess
import sys

BASE_DIR = Path('owendo-05-04-26-4-Outcome data_uzf')
OUT_DIR = Path('analysis') / 'output_new'
OUT_DIR.mkdir(parents=True, exist_ok=True)

# find .dat files (e.g., the file with Chinese name)
dat_files = list(BASE_DIR.glob('*.dat')) + list(BASE_DIR.glob('**/*.dat'))
# also check top-level workspace for any .dat (some files are at repo root)
from pathlib import Path as _P
dat_files += list(_P('.').glob('*.dat'))
# ensure stdout can emit utf-8 on Windows terminals
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

if not dat_files:
    print(f'No .dat files found under {BASE_DIR}')
else:
    print('Found .dat files:')
    for p in dat_files:
        try:
            print(' -', str(p))
        except Exception:
            # fallback to repr
            print(' -', repr(p))

rows = []
for p in dat_files:
    try:
        with open(p, 'r', encoding='utf-8', errors='ignore') as fh:
            for i,ln in enumerate(fh, start=1):
                parts = [s.strip() for s in ln.strip().split(',')]
                if len(parts) < 5:
                    continue
                # mapping: parts[2]=northing, parts[3]=easting, parts[4]=depth
                north = float(parts[2])
                east = float(parts[3])
                depth = float(parts[4])
                row = {
                    'CoordinateY': north,
                    'CoordinateX': east,
                    'h': depth,
                    'GroundH(H)': depth,
                    'Lat': None,
                    'Lon': None,
                    'Locked': 0,
                    'Sats': None,
                    'Status': None,
                    'src_file': p.name,
                    'ping': i,
                    'datetime': None,
                    'datetime_parsed': None,
                    'easting': east,
                    'northing': north,
                    'depth': depth,
                    # placeholders for other fields used in existing merged files
                    'f2': None, 'f3': None, 'f4': None, 'f5': None, 'f6': None, 'f7': None, 'f8': None,
                    'f2_num': None, 'f3_num': None, 'f4_num': None, 'f5_num': None, 'f6_num': None, 'f7_num': None, 'f8_num': None,
                }
                rows.append(row)
    except Exception as e:
        try:
            print(f'Failed to read {p}: {e}')
        except Exception:
            print('Failed to read file (encoding issue)')

if rows:
    df_new = pd.DataFrame(rows)
    # write or append to merged_data_with_owendo_cols.csv
    owendo_path = OUT_DIR / 'merged_data_with_owendo_cols.csv'
    if owendo_path.exists():
        df_exist = pd.read_csv(owendo_path)
        df_combined = pd.concat([df_exist, df_new], ignore_index=True, sort=False)
        df_combined.to_csv(owendo_path, index=False)
        print(f'Appended {len(df_new)} rows to {owendo_path}')
    else:
        df_new.to_csv(owendo_path, index=False)
        print(f'Wrote {owendo_path}')

    # also write a simpler merged_data.csv (subset)
    merged_path = OUT_DIR / 'merged_data.csv'
    cols_subset = ['src_file','ping','datetime','datetime_parsed','easting','northing','depth','f2','f3','f4']
    df_merge_subset = df_new[[c for c in cols_subset if c in df_new.columns]]
    if merged_path.exists():
        dfm = pd.read_csv(merged_path)
        dfm_comb = pd.concat([dfm, df_merge_subset], ignore_index=True, sort=False)
        dfm_comb.to_csv(merged_path, index=False)
        print(f'Appended to {merged_path}')
    else:
        df_merge_subset.to_csv(merged_path, index=False)
        print(f'Wrote {merged_path}')

    # Attempt to create Lat/Lon using the available convert scripts if present
    # prefer convert_to_epsg32632.py if exists
    conv32632 = Path('analysis/scripts/convert_to_epsg32632.py')
    conv_generic = Path('analysis/scripts/convert_easting_to_latlon.py')
    if conv32632.exists():
        print('Running convert_to_epsg32632.py')
        subprocess.run([sys.executable, str(conv32632)])
    elif conv_generic.exists():
        print('Running convert_easting_to_latlon.py')
        subprocess.run([sys.executable, str(conv_generic)])
    else:
        print('No conversion script found to compute Lat/Lon')
    
    # Run GNSS finder script to detect any GNSS logs
    gnss_script = Path('analysis/scripts/find_gnss_tracks.py')
    if gnss_script.exists():
        print('Running GNSS discovery script...')
        subprocess.run([sys.executable, str(gnss_script)])

    # Try to assign GNSS positions to pings if possible
    assign_gnss = Path('analysis/scripts/assign_gnss_by_nearest.py')
    if assign_gnss.exists():
        print('Trying to assign GNSS positions to pings...')
        subprocess.run([sys.executable, str(assign_gnss)])

    # Run topo merge/interpolate if script exists
    topo_script = Path('analysis/scripts/merge_topo_and_interpolate.py')
    if topo_script.exists():
        print('Running topo merge/interpolation script...')
        subprocess.run([sys.executable, str(topo_script)])

    # Run section generation (2D/3D) if available
    gen_sections = Path('analysis/generate_sections.py')
    if gen_sections.exists():
        print('Running section generation (2D/3D)...')
        subprocess.run([sys.executable, str(gen_sections), '--merged', str(OUT_DIR / 'merged_data_with_owendo_cols.csv')])

    # write small preview files (first 100 rows) for quick validation
    try:
        df_new.head(100).to_csv(OUT_DIR / 'preview_merged_new_head100.csv', index=False)
    except Exception:
        pass
    try:
        if (OUT_DIR / 'merged_data_with_owendo_cols.csv').exists():
            pd.read_csv(OUT_DIR / 'merged_data_with_owendo_cols.csv').head(100).to_csv(OUT_DIR / 'preview_owendo_head100.csv', index=False)
    except Exception:
        pass
    try:
        if (OUT_DIR / 'merged_data_latlon_epsg32632.csv').exists():
            pd.read_csv(OUT_DIR / 'merged_data_latlon_epsg32632.csv').head(100).to_csv(OUT_DIR / 'preview_latlon_head100.csv', index=False)
    except Exception:
        pass
else:
    print('No rows parsed from .dat files; nothing written.')

# ---- Post-process: ensure z_water / z_bed present in merged OWENDO table
try:
    owendo_csv = OUT_DIR / 'merged_data_with_owendo_cols.csv'
    if owendo_csv.exists():
        df_all = pd.read_csv(owendo_csv)
        # prefer per-row GroundH(H) if present and not all-null
        zw = None
        if any(c.lower().replace(' ', '') == 'groundh(h)'.lower().replace(' ', '') for c in df_all.columns):
            # find exact column name
            colname = [c for c in df_all.columns if c.lower().replace(' ', '') == 'groundh(h)'.lower().replace(' ', '')][0]
            if not df_all[colname].isna().all():
                df_all['z_water'] = pd.to_numeric(df_all[colname], errors='coerce')
                zw = 'col'
        # fallback to config groundh_offset
        cfgp = Path('owendo-05-04-26-4-Outcome data_uzf/owendo_config.json')
        if zw is None and cfgp.exists():
            try:
                import json
                cfg = json.loads(cfgp.read_text())
                off = cfg.get('groundh_offset')
                if off is not None:
                    df_all['z_water'] = float(off)
                    zw = 'offset'
            except Exception:
                pass
        # final fallback: zeros
        if 'z_water' not in df_all.columns:
            df_all['z_water'] = 0.0

        # compute z_bed (z_water + h) — preserve existing h if present
        if 'h' in df_all.columns:
            try:
                df_all['z_bed'] = pd.to_numeric(df_all['z_water'], errors='coerce').fillna(0.0) + pd.to_numeric(df_all['h'], errors='coerce').fillna(0.0)
            except Exception:
                df_all['z_bed'] = df_all['z_water']
        else:
            df_all['z_bed'] = df_all['z_water']

        out_z = OUT_DIR / 'merged_data_with_owendo_cols_z.csv'
        df_all.to_csv(out_z, index=False)
        print('Wrote z-enhanced merged file:', out_z)
except Exception as e:
    print('Failed to post-process z columns:', e)

print('run_full_scan completed')
