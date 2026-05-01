#!/usr/bin/env python3
"""Analyze the OWENDO data folder and produce a config JSON with detected parameters."""
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any
import argparse
import sys

import pandas as pd

# ensure repository root is on sys.path so `analysis` package imports work
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis.parse_raw_and_merge import find_data_files


def detect_columns_from_merged(merged_path: Path) -> Dict[str, Any]:
    df = pd.read_csv(merged_path)
    info: Dict[str, Any] = {}
    info["rows"] = int(len(df))
    # prefer explicit names
    if "easting" in df.columns and "northing" in df.columns and "depth" in df.columns:
        info["easting_col"] = "easting"
        info["northing_col"] = "northing"
        info["depth_col"] = "depth"
    else:
        # fallback: find numeric f*_num columns; last three are used by parser
        nums = [c for c in df.columns if c.endswith("_num")]
        if len(nums) >= 3:
            info["easting_col"] = nums[-3]
            info["northing_col"] = nums[-2]
            info["depth_col"] = nums[-1]
        else:
            # try any numeric column heuristics
            numerics = df.select_dtypes(include=["number"]).columns.tolist()
            info["easting_col"] = numerics[0] if numerics else None
            info["northing_col"] = numerics[1] if len(numerics) > 1 else None
            info["depth_col"] = numerics[2] if len(numerics) > 2 else None

    # sample first rows for quick preview and sanitize NaN/NaT for JSON
    sample = df.head(5).copy()
    sample = sample.where(pd.notnull(sample), None)
    # convert to native Python types and replace any remaining NaN with None
    import math
    import numpy as _np

    raw = sample.to_dict(orient="records")
    clean_rows = []
    for rec in raw:
        new = {}
        for k, v in rec.items():
            # numpy scalar -> python
            if isinstance(v, _np.generic):
                try:
                    v = v.item()
                except Exception:
                    v = None
            # floats NaN -> None
            if isinstance(v, float) and math.isnan(v):
                v = None
            new[k] = v
        clean_rows.append(new)
    info["sample_rows"] = clean_rows
    # bounding box if easting/northing present
    try:
        e = pd.to_numeric(df[info["easting_col"]], errors="coerce")
        n = pd.to_numeric(df[info["northing_col"]], errors="coerce")
        info["bbox"] = {
            "e_min": float(e.min()), "e_max": float(e.max()),
            "n_min": float(n.min()), "n_max": float(n.max()),
        }
    except Exception:
        info["bbox"] = None

    return info


def analyze_and_write_config(data_dir: Path, merged_csv: Path, out_config: Path, overwrite: bool = False) -> Path:
    cfg: Dict[str, Any] = {}
    data_dir = data_dir.resolve()
    merged_csv = merged_csv.resolve()
    out_config = out_config.resolve()

    cfg["data_dir"] = str(data_dir)
    cfg["merged_csv"] = str(merged_csv)

    files = [str(p.name) for p in find_data_files(data_dir)]
    cfg["data_files"] = files
    cfg["file_count"] = len(files)

    if merged_csv.exists():
        cfg.update(detect_columns_from_merged(merged_csv))
    else:
        cfg.update({
            "rows": 0,
            "easting_col": None,
            "northing_col": None,
            "depth_col": None,
            "sample_rows": [],
            "bbox": None,
        })

    # sensible defaults
    cfg.setdefault("groundh_offset", 0.0)
    cfg.setdefault("export_out", str(data_dir.parent.joinpath("Output/OWENDO-BATHY-SURVEY.txt")))

    out_config.parent.mkdir(parents=True, exist_ok=True)
    if out_config.exists() and not overwrite:
        raise FileExistsError(f"Config exists: {out_config} (use overwrite=True)")

    out_config.write_text(json.dumps(cfg, indent=2, ensure_ascii=False))
    return out_config


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", default="owendo-05-04-26-4-Outcome data_uzf/data")
    p.add_argument("--merged", default="analysis/output_new/merged_data.csv")
    p.add_argument("--out", default="owendo-05-04-26-4-Outcome data_uzf/owendo_config.json")
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    data_dir = Path(args.data_dir)
    merged = Path(args.merged)
    out = Path(args.out)

    cfg_path = analyze_and_write_config(data_dir, merged, out, overwrite=args.force)
    print(f"Wrote config: {cfg_path}")


if __name__ == "__main__":
    main()
