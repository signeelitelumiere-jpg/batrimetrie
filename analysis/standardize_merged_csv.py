#!/usr/bin/env python3
"""Standardize merged CSV to required OWENDO column headers.

Creates columns in this order:
  CoordinateY CoordinateX h GroundH(H) Lat Lon Locked Sats Status

If a config file is provided, uses detected column names and groundh_offset.
Writes output to the same path by default (creates a .bak backup first).
"""
from __future__ import annotations
import argparse
from pathlib import Path
import json
import shutil
import pandas as pd
import numpy as np
from typing import Optional


def standardize(merged_path: Path, out_path: Optional[Path] = None, cfg_path: Optional[Path] = None, overwrite: bool = True) -> Path:
    merged_path = merged_path.resolve()
    if out_path is None:
        out_path = merged_path

    df = pd.read_csv(merged_path)

    # load config if available
    cfg = {}
    if cfg_path and cfg_path.exists():
        try:
            cfg = json.loads(cfg_path.read_text())
        except Exception:
            cfg = {}

    # determine source columns
    easting = cfg.get("easting_col") or next((c for c in df.columns if c.lower() in ("easting","x","coordx","coordinatex","f6","f6_num")), None)
    northing = cfg.get("northing_col") or next((c for c in df.columns if c.lower() in ("northing","y","coordy","coordinatey","f7","f7_num")), None)
    depth = cfg.get("depth_col") or next((c for c in df.columns if c.lower() in ("depth","z","h","f8","f8_num")), None)

    if not (easting and northing and depth):
        # try numeric fallback
        nums = [c for c in df.columns if np.issubdtype(df[c].dtype, np.number)]
        if len(nums) >= 3:
            easting = easting or nums[-3]
            northing = northing or nums[-2]
            depth = depth or nums[-1]

    if not (easting and northing and depth):
        raise RuntimeError(f"Could not find easting/northing/depth columns in {merged_path}")

    # choose ground offset
    groundh_offset = cfg.get("groundh_offset") if cfg else None

    # build standardized DataFrame
    out_df = pd.DataFrame()
    out_df["CoordinateY"] = pd.to_numeric(df[northing], errors="coerce")
    out_df["CoordinateX"] = pd.to_numeric(df[easting], errors="coerce")
    out_df["h"] = pd.to_numeric(df[depth], errors="coerce")

    # GroundH(H)
    if cfg and cfg.get("ground_col") and cfg.get("ground_col") in df.columns:
        out_df["GroundH(H)"] = pd.to_numeric(df[cfg.get("ground_col")], errors="coerce")
    elif groundh_offset is not None:
        out_df["GroundH(H)"] = out_df["h"] - float(groundh_offset)
    else:
        out_df["GroundH(H)"] = out_df["h"] - 0.4

    # Lat Lon detection
    lat = cfg.get("lat_col") if cfg else None
    lon = cfg.get("lon_col") if cfg else None
    if not lat or lat not in df.columns:
        lat = next((c for c in df.columns if "lat" in c.lower()), None)
    if not lon or lon not in df.columns:
        lon = next((c for c in df.columns if "lon" in c.lower()), None)

    out_df["Lat"] = pd.to_numeric(df[lat], errors="coerce") if lat and lat in df.columns else ""
    out_df["Lon"] = pd.to_numeric(df[lon], errors="coerce") if lon and lon in df.columns else ""

    out_df["Locked"] = 0
    out_df["Sats"] = 4
    out_df["Status"] = ""

    # backup
    bak = merged_path.with_suffix(merged_path.suffix + ".bak")
    if not bak.exists():
        shutil.copy2(merged_path, bak)

    # write
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    return out_path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--merged", default="analysis/output_new/merged_data.csv")
    p.add_argument("--cfg", default="owendo-05-04-26-4-Outcome data_uzf/owendo_config.json")
    p.add_argument("--out", default=None)
    args = p.parse_args()

    merged = Path(args.merged)
    cfg = Path(args.cfg)
    out = Path(args.out) if args.out else None
    res = standardize(merged, out_path=out, cfg_path=cfg)
    print(f"Wrote standardized CSV: {res}")


if __name__ == "__main__":
    main()
