#!/usr/bin/env python3
"""
Parse paired `.data` files in the `data/` folder and merge extracted records
into a single CSV table.

Behavior:
- Reads all `Ln*.data` files in a folder (default: ./owendo-.../data)
- Splits binary payload on NUL bytes and extracts CSV-like records
- Parses records like: "32,2026-04-05 12:59:12.400,HF,0.000,LF,0.000,32830.909,557689.579,-0.767"
- Produces a merged CSV with one row per record and basic numeric conversion

This is conservative: it does not assume waveform float arrays; it extracts
ASCII telemetry/measurement records embedded in the binary files and merges
them. If you want waveform retracking later, we can extend this script.
"""
from __future__ import annotations
import argparse
import csv
import datetime
import re
from pathlib import Path
from typing import List

import pandas as pd
import re as _re


RE_DATE = re.compile(r"^\d+,\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[\.:]\d+")


def parse_data_file(path: Path) -> List[dict]:
    b = path.read_bytes()
    # decode permissively; entries appear to be latin-1/ascii
    text = b.decode("latin-1", errors="replace")
    # split on NUL runs — each piece may contain one CSV-like record
    parts = re.split(r"\x00+", text)
    rows = []
    for p in parts:
        s = p.strip()
        if not s:
            continue
        # keep lines that start with an integer ping id and an ISO date
        if RE_DATE.match(s):
            # normalize whitespace
            s = s.replace('\r', '').replace('\n', '')
            try:
                parsed = next(csv.reader([s]))
            except Exception:
                continue
            if len(parsed) < 6:
                continue
            row = {
                "src_file": path.name,
                "ping": parsed[0],
                "datetime": parsed[1],
            }
            for i, val in enumerate(parsed[2:], start=2):
                row[f"f{i}"] = val
            rows.append(row)
    return rows


def normalize_rows(rows: List[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # parse datetime when possible
    def try_dt(v):
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S,%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.datetime.strptime(v, fmt)
            except Exception:
                pass
        return pd.NaT

    df["datetime_parsed"] = df["datetime"].apply(try_dt)

    # attempt to pull last three numeric columns as easting,northing,depth
    fcols = [c for c in df.columns if c.startswith("f")]
    if fcols:
        # robust numeric extraction: try direct numeric, else regex search for first float
        num_re = _re.compile(r"[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?")
        def extract_num(s):
            if pd.isna(s):
                return None
            try:
                return float(s)
            except Exception:
                m = num_re.search(str(s))
                if m:
                    try:
                        return float(m.group(0))
                    except Exception:
                        return None
                return None

        for col in fcols:
            df[col + "_num"] = df[col].apply(extract_num)
        nums = [c for c in df.columns if c.endswith("_num")]
        if len(nums) >= 3:
            df["easting"] = df[nums[-3]]
            df["northing"] = df[nums[-2]]
            df["depth"] = df[nums[-1]]

    return df


def find_data_files(folder: Path) -> List[Path]:
    return sorted(folder.glob("Ln*.data"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", default="./owendo-05-04-26-4-Outcome data_uzf/data")
    p.add_argument("--out", default="analysis/output_new/merged_data.csv")
    p.add_argument("--force", action="store_true", help="overwrite output if exists")
    args = p.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        raise SystemExit(f"data dir not found: {data_dir}")

    files = find_data_files(data_dir)
    if not files:
        raise SystemExit(f"no Ln*.data files found in {data_dir}")

    all_rows = []
    for f in files:
        print(f"Parsing {f.name}...")
        rows = parse_data_file(f)
        if not rows:
            print(f"  -> no records parsed from {f.name}")
        all_rows.extend(rows)

    df = normalize_rows(all_rows)
    if df.empty:
        print("No records parsed. Exiting.")
        return

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and not args.force:
        print(f"Output exists: {out_path} — use --force to overwrite")
        return

    if "datetime_parsed" in df.columns and not df["datetime_parsed"].isna().all():
        df = df.sort_values(by=["datetime_parsed"])

    # Ensure canonical OWENDO-style columns and compute z_water / z_bed
    # CoordinateX/CoordinateY: use easting/northing
    if 'easting' in df.columns:
        df['CoordinateX'] = pd.to_numeric(df['easting'], errors='coerce')
    if 'northing' in df.columns:
        df['CoordinateY'] = pd.to_numeric(df['northing'], errors='coerce')
    # h: use depth if present
    if 'depth' in df.columns:
        df['h'] = pd.to_numeric(df['depth'], errors='coerce')
    else:
        df['h'] = pd.NA
    # GroundH(H): try to derive from available altitude-like fields, else fallback to h
    if 'GroundH(H)' not in df.columns:
        # no altitude in Ln*.data parsing; set to h (mirrors existing OWENDO outputs)
        df['GroundH(H)'] = df['h']
    # Lat / Lon: not available in Ln*.data parsing — initialize as NA (numeric)
    if 'Lat' not in df.columns:
        df['Lat'] = pd.NA
    if 'Lon' not in df.columns:
        df['Lon'] = pd.NA
    # Locked / Sats / Status defaults
    if 'Locked' not in df.columns:
        df['Locked'] = 0
    if 'Sats' not in df.columns:
        df['Sats'] = 0
    if 'Status' not in df.columns:
        df['Status'] = ''

    # z_water / z_bed computation with sign heuristic — ensure numeric
    zw = pd.to_numeric(df.get('GroundH(H)'), errors='coerce')
    df['z_water'] = zw.fillna(0.0)
    hnum = pd.to_numeric(df.get('h'), errors='coerce')
    # if most h values are negative, treat h as negative offsets from surface
    if hnum.dropna().shape[0] > 0 and (hnum.dropna() < 0).mean() > 0.5:
        df['z_bed'] = df['z_water'] + hnum.fillna(0.0)
    else:
        df['z_bed'] = df['z_water'] - hnum.abs().fillna(0.0)

    # ensure numeric types
    df['z_water'] = pd.to_numeric(df['z_water'], errors='coerce')
    df['z_bed'] = pd.to_numeric(df['z_bed'], errors='coerce')

    cols = [c for c in ("src_file", "ping", "datetime", "datetime_parsed", "easting", "northing", "depth") if c in df.columns]
    # prefer to expose canonical columns first
    pref = ["CoordinateY","CoordinateX","h","GroundH(H)","Lat","Lon","Locked","Sats","Status","z_water","z_bed"]
    other = [c for c in df.columns if c not in cols and c not in pref]
    write_cols = []
    # include src/ping/datetime fields
    write_cols.extend(cols)
    # then canonical pref (if present)
    for c in pref:
        if c in df.columns:
            write_cols.append(c)
    write_cols.extend(other)
    df.to_csv(out_path, index=False, columns=write_cols)
    print(f"Wrote merged table: {out_path} ({len(df)} rows)")
    # Ensure Lat/Lon if easting/northing available
    # Compute Lat/Lon from easting/northing when available and values appear projected
    try:
        need_latlon = ('Lat' in df.columns and df['Lat'].isna().all()) or ('Lon' in df.columns and df['Lon'].isna().all())
        if need_latlon and 'easting' in df.columns and 'northing' in df.columns:
            try:
                from pyproj import Transformer
                xs = pd.to_numeric(df['easting'], errors='coerce')
                ys = pd.to_numeric(df['northing'], errors='coerce')
                if xs.max(skipna=True) > 180 or ys.max(skipna=True) > 90:
                    transformer = Transformer.from_crs('EPSG:32632', 'EPSG:4326', always_xy=True)
                    lon, lat = transformer.transform(xs.fillna(0.0).values, ys.fillna(0.0).values)
                    df['Lon'] = lon
                    df['Lat'] = lat
                    print('Computed Lat/Lon from easting/northing')
            except Exception as e:
                print('Lat/Lon computation failed:', e)
    except Exception:
        pass

    # recompute write columns to include any newly added Lat/Lon/z columns
    cols = [c for c in ("src_file", "ping", "datetime", "datetime_parsed", "easting", "northing", "depth") if c in df.columns]
    pref = ["CoordinateY","CoordinateX","h","GroundH(H)","Lat","Lon","Locked","Sats","Status","z_water","z_bed"]
    other = [c for c in df.columns if c not in cols and c not in pref]
    write_cols = []
    write_cols.extend(cols)
    for c in pref:
        if c in df.columns:
            write_cols.append(c)
    write_cols.extend(other)
    df.to_csv(out_path, index=False, columns=write_cols)
    print(f'Wrote merged table: {out_path} ({len(df)} rows)')


if __name__ == "__main__":
    main()
