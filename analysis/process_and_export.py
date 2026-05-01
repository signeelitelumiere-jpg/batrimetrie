#!/usr/bin/env python3
"""Run parsing of raw Ln*.data files and export OWENDO survey TXT without
overwriting the existing OWENDO-BATHY-SURVEY.txt. Writes a generated file
`OWENDO-BATHY-SURVEY-generated.txt` in the Output folder.
"""
from pathlib import Path
import subprocess
import sys
import json
from typing import List

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path('owendo-05-04-26-4-Outcome data_uzf/data')
CFG_PATH = Path('owendo-05-04-26-4-Outcome data_uzf/owendo_config.json')
MERGED_OUT = Path('analysis/output_new/merged_data.csv')
OWN_OUT = Path('owendo-05-04-26-4-Outcome data_uzf/Output/OWENDO-BATHY-SURVEY-generated.txt')
OWN_CSV = Path('owendo-05-04-26-4-Outcome data_uzf/Output/OWENDO-BATHY-SURVEY-generated.csv')
OWN_HTML = Path('owendo-05-04-26-4-Outcome data_uzf/Output/OWENDO-BATHY-SURVEY-generated.html')


def run_merge():
    py = sys.executable
    cmd = [py, str(ROOT / 'analysis' / 'parse_raw_and_merge.py'), '--data-dir', str(DATA_DIR), '--out', str(MERGED_OUT), '--force']
    print('Running:', ' '.join(cmd))
    res = subprocess.run(cmd, check=False)
    if res.returncode != 0:
        raise SystemExit(f"merge script failed (rc={res.returncode})")


def parse_shn_files(extra_shn: List[Path]) -> None:
    """Try to extract ASCII-like records from .shn files and append to merged CSV.

    This is conservative: it will search for lines matching the same RE_DATE pattern
    used by `parse_raw_and_merge.parse_data_file` by invoking that function on the
    binary content where possible.
    """
    import importlib
    pr = importlib.import_module('analysis.parse_raw_and_merge')
    all_rows = []
    for sh in extra_shn:
        try:
            print('Parsing .shn (conservative):', sh)
            rows = pr.parse_data_file(sh)
            if rows:
                all_rows.extend(rows)
        except Exception as e:
            print('Failed parsing shn', sh, e)
    if not all_rows:
        return
    df_new = pr.normalize_rows(all_rows)
    # append to existing merged CSV if exists
    import pandas as pd
    if MERGED_OUT.exists():
        df_old = pd.read_csv(MERGED_OUT)
        df_comb = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_comb = df_new
    MERGED_OUT.parent.mkdir(parents=True, exist_ok=True)
    df_comb.to_csv(MERGED_OUT, index=False)
    print('Appended SHN-derived rows to merged CSV')


def run_export():
    # call export_survey from export_survey_format.py
    sys.path.insert(0, str(ROOT / 'analysis'))
    from export_survey_format import export_survey
    import pandas as pd

    cfg = None
    if CFG_PATH.exists():
        try:
            cfg = json.loads(CFG_PATH.read_text(encoding='utf-8'))
        except Exception:
            cfg = None

    gh = cfg.get('groundh_offset') if cfg else None

    # read merged, filter invalid rows
    if not MERGED_OUT.exists():
        raise SystemExit('Merged CSV not found for export')
    df = pd.read_csv(MERGED_OUT)
    # basic validation: require easting/northing/depth present and not NaN
    for c in ('easting', 'northing', 'depth'):
        if c not in df.columns:
            # try alternative names
            if 'CoordinateX' in df.columns and c == 'easting':
                df['easting'] = df['CoordinateX']
            if 'CoordinateY' in df.columns and c == 'northing':
                df['northing'] = df['CoordinateY']
    before = len(df)
    df_clean = df.dropna(subset=['easting', 'northing', 'depth'])
    dropped = before - len(df_clean)
    print(f'Filtered invalid rows: dropped {dropped} rows')

    # construct organized output CSV matching the TXT layout
    OWN_CSV.parent.mkdir(parents=True, exist_ok=True)
    # Ensure numeric columns exist
    df_clean['CoordinateX'] = pd.to_numeric(df_clean.get('easting', df_clean.get('CoordinateX')), errors='coerce')
    df_clean['CoordinateY'] = pd.to_numeric(df_clean.get('northing', df_clean.get('CoordinateY')), errors='coerce')
    df_clean['h'] = pd.to_numeric(df_clean.get('depth', df_clean.get('h')), errors='coerce')

    def compute_ground_h(val):
        try:
            if gh is not None:
                return float(val) - float(gh)
            return float(val) - 0.4
        except Exception:
            return None

    df_clean['GroundH(H)'] = df_clean['h'].apply(compute_ground_h)
    df_clean['Lat'] = df_clean.get('lat', '')
    df_clean['Lon'] = df_clean.get('lon', '')
    df_clean['Locked'] = 0
    df_clean['Sats'] = 4
    df_clean['Status'] = ''

    cols_out = ['CoordinateY', 'CoordinateX', 'h', 'GroundH(H)', 'Lat', 'Lon', 'Locked', 'Sats', 'Status']
    df_out = df_clean.reindex(columns=cols_out)
    # write CSV with desired column order
    df_out.to_csv(OWN_CSV, index=False)

    # produce formatted TXT with aligned columns
    out_txt = OWN_OUT
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    with out_txt.open('w', encoding='utf-8') as fh:
        hdr = cols_out
        fh.write(' '.join(hdr) + '\n')
        for _, r in df_out.iterrows():
            # format numeric fields with 6 decimals, keep empties
            def fmt(v):
                try:
                    if pd.isna(v) or v == '':
                        return ''
                    return f"{float(v):.6f}"
                except Exception:
                    return str(v)

            y = fmt(r['CoordinateY'])
            x = fmt(r['CoordinateX'])
            hval = fmt(r['h'])
            ghval = fmt(r['GroundH(H)'])
            lat = r['Lat'] if r['Lat'] != '' else ''
            lon = r['Lon'] if r['Lon'] != '' else ''
            locked = int(r['Locked']) if r['Locked'] != '' else 0
            sats = int(r['Sats']) if r['Sats'] != '' else 0
            status = r['Status'] if r['Status'] != '' else ''
            fh.write(f"{y} {x} {hval} {ghval} {lat} {lon} {locked} {sats} {status}\n")

    # produce a simple HTML colored table for UI (no overflow)
    try:
        html = df_clean.to_html(classes='table table-striped', index=False)
        OWN_HTML.parent.mkdir(parents=True, exist_ok=True)
        OWN_HTML.write_text('<style>table{border-collapse:collapse;width:100%;}th,td{padding:4px;border:1px solid #ccc;white-space:nowrap;}th{background:#003249;color:#fff}</style>' + html, encoding='utf-8')
    except Exception:
        pass

    # also call existing exporter for compatibility
    try:
        export_survey(MERGED_OUT, out_txt, groundh_offset=gh, cfg_path=CFG_PATH)
    except Exception:
        pass

    print('Exported to:', out_txt, OWN_CSV, OWN_HTML)
    return out_txt


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--include', default=None, help='Comma-separated Ln*.data filenames to include')
    p.add_argument('--no-shn', action='store_true', help="Disable .shn conservative parsing")
    args = p.parse_args()

    if not DATA_DIR.exists():
        raise SystemExit(f'data dir not found: {DATA_DIR}')

    # if include list provided, temporarily move other files aside
    if args.include:
        include = set([s.strip() for s in args.include.split(',') if s.strip()])
        # create temporary folder
        tmpdir = MERGED_OUT.parent / 'tmp_input_keep'
        tmpdir.mkdir(parents=True, exist_ok=True)
        # copy selected files to a temporary folder and run merge on that
        for fn in include:
            src = DATA_DIR / fn
            if src.exists():
                (tmpdir / fn).write_bytes(src.read_bytes())
        # run merge pointing to tmpdir
        print('Running merge on selected files only')
        py = sys.executable
        cmd = [py, str(ROOT / 'analysis' / 'parse_raw_and_merge.py'), '--data-dir', str(tmpdir), '--out', str(MERGED_OUT), '--force']
        subprocess.run(cmd, check=True)
    else:
        run_merge()

    # optionally parse .shn files located next to config
    if not args.no_shn:
        # look for .shn files at dataset root
        root_shn = list(Path('.').glob('owendo-05-04-26-4-Outcome data_uzf*.shn'))
        data_shn = list(DATA_DIR.glob('*.shn'))
        shn_list = root_shn + data_shn
        if shn_list:
            parse_shn_files(shn_list)

    exported = run_export()
    print('Done. File produced:', exported)


if __name__ == '__main__':
    main()
