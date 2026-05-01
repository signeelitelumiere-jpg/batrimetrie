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

    cols = [c for c in ("src_file", "ping", "datetime", "datetime_parsed", "easting", "northing", "depth") if c in df.columns]
    other = [c for c in df.columns if c not in cols]
    df.to_csv(out_path, index=False, columns=cols + other)
    print(f"Wrote merged table: {out_path} ({len(df)} rows)")


if __name__ == "__main__":
    main()
