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
try:
    from analysis.process_usf import process_usf_file
except Exception:
    process_usf_file = None

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

# also look for .usf files (some providers use .usf extension)
usf_files = list(BASE_DIR.glob('*.usf')) + list(BASE_DIR.glob('**/*.usf'))
if usf_files:
    print('Found .usf files:')
    for up in usf_files:
        try:
            print(' -', str(up))
        except Exception:
            print(' -', repr(up))
        # try processing via process_usf if available
        if process_usf_file is not None:
            try:
                print('Processing .usf via process_usf_file...')
                process_usf_file(str(up), outdir=str(OUT_DIR))
            except Exception as e:
                print('Failed to process .usf:', e)

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
        # Compute z_water and z_bed immediately so outputs include them
        try:
            # prefer per-row GroundH(H) if present and not all-null
            if any(c.lower().replace(' ', '') == 'groundh(h)'.lower().replace(' ', '') for c in df_new.columns):
                colname = [c for c in df_new.columns if c.lower().replace(' ', '') == 'groundh(h)'.lower().replace(' ', '')][0]
                if not df_new[colname].isna().all():
                    df_new['z_water'] = pd.to_numeric(df_new[colname], errors='coerce')
                else:
                    df_new['z_water'] = 0.0
            else:
                # fallback to config groundh_offset
                cfgp = Path('owendo-05-04-26-4-Outcome data_uzf/owendo_config.json')
                if cfgp.exists():
                    try:
                        import json
                        cfg = json.loads(cfgp.read_text())
                        off = cfg.get('groundh_offset')
                        if off is not None:
                            df_new['z_water'] = float(off)
                        else:
                            df_new['z_water'] = 0.0
                    except Exception:
                        df_new['z_water'] = 0.0
                else:
                    df_new['z_water'] = 0.0

                # compute z_bed from h or depth-like columns
                if 'h' in df_new.columns or any('depth' in c.lower() for c in df_new.columns):
                    # choose depth candidate
                    dcol = 'h' if 'h' in df_new.columns else next((c for c in df_new.columns if 'depth' in c.lower()), None)
                    try:
                        hnum = pd.to_numeric(df_new[dcol], errors='coerce')
                        if (hnum.dropna() < 0).mean() > 0.5:
                            df_new['z_bed'] = df_new['z_water'] + hnum
                        else:
                            df_new['z_bed'] = df_new['z_water'] - hnum.abs()
                    except Exception:
                        df_new['z_bed'] = df_new['z_water']
                else:
                    df_new['z_bed'] = df_new['z_water']

            # Ensure common columns for outputs (Lat/Lon, GroundH(H), datetime, Locked/Sats/Status, placeholders)
            def ensure_common_columns(df):
                import pandas as _pd
                # Lat/Lon
                for c in ('Lat','latitude'):
                    if c in df.columns:
                        df['Lat'] = _pd.to_numeric(df[c], errors='coerce')
                        break
                for c in ('Lon','longitude'):
                    if c in df.columns:
                        df['Lon'] = _pd.to_numeric(df[c], errors='coerce')
                        break
                # GroundH(H)
                if 'GroundH(H)' not in df.columns:
                    if 'z_water' in df.columns:
                        df['GroundH(H)'] = df['z_water']
                    else:
                        df['GroundH(H)'] = 0.0
                # datetime from utcTime
                if 'datetime' not in df.columns and 'utcTime' in df.columns:
                    try:
                        df['datetime'] = _pd.to_datetime(df['utcTime'], unit='ms', errors='coerce')
                        if df['datetime'].isna().all():
                            df['datetime'] = _pd.to_datetime(df['utcTime'], unit='s', errors='coerce')
                    except Exception:
                        df['datetime'] = _pd.NaT
                elif 'datetime' not in df.columns:
                    df['datetime'] = _pd.NaT
                # Locked / Sats / Status
                if 'Locked' not in df.columns:
                    df['Locked'] = 0
                if 'Sats' not in df.columns:
                    df['Sats'] = 0
                if 'Status' not in df.columns:
                    df['Status'] = ''
                # placeholders
                for i in range(2,9):
                    cname = f'f{i}'
                    numc = f'f{i}_num'
                    if cname not in df.columns:
                        df[cname] = _pd.NA
                    if numc not in df.columns:
                        df[numc] = _pd.to_numeric(df.get(cname), errors='coerce')
                # ensure z cols
                if 'z_water' not in df.columns:
                    df['z_water'] = _pd.to_numeric(df['GroundH(H)'], errors='coerce').fillna(0.0)
                if 'z_bed' not in df.columns:
                    if 'h' in df.columns:
                        try:
                            hnum = _pd.to_numeric(df['h'], errors='coerce')
                            if (hnum.dropna() < 0).mean() > 0.5:
                                df['z_bed'] = df['z_water'] - hnum
                            else:
                                df['z_bed'] = df['z_water'] - hnum.abs()
                        except Exception:
                            df['z_bed'] = df['z_water']
                    else:
                        df['z_bed'] = df['z_water']
                return df

            df_new = ensure_common_columns(df_new)
        except Exception:
            # ensure columns exist even on failure
            if 'z_water' not in df_new.columns:
                df_new['z_water'] = 0.0
            if 'z_bed' not in df_new.columns:
                df_new['z_bed'] = df_new['z_water']
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

        # overwrite the main OWENDO merged file so downstream previews include z columns
        df_all.to_csv(owendo_csv, index=False)
        out_z = OUT_DIR / 'merged_data_with_owendo_cols_z.csv'
        df_all.to_csv(out_z, index=False)
        # update preview files so they include z_water / z_bed
        try:
            df_all.head(100).to_csv(OUT_DIR / 'preview_merged_new_head100_with_z.csv', index=False)
        except Exception:
            pass
        try:
            df_all.head(100).to_csv(OUT_DIR / 'preview_owendo_head100.csv', index=False)
        except Exception:
            pass
        print('Wrote z-enhanced merged file:', out_z)
except Exception as e:
    print('Failed to post-process z columns:', e)

print('run_full_scan completed')
# normalize outputs to ensure all CSVs have required columns
try:
    from analysis.scripts.normalize_all_outputs import normalize_outputs
    normalize_outputs(str(OUT_DIR))
except Exception:
    try:
        import subprocess, sys
        subprocess.run([sys.executable, str(Path('analysis/scripts/normalize_all_outputs.py')), str(OUT_DIR)])
    except Exception:
        pass
