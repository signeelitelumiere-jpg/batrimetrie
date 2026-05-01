"""Export merged parsed data to a whitespace-separated survey text file
matching the common 'OWENDO-BATHY-SURVEY.txt' column order.

Columns produced:
  CoordinateY CoordinateX h GroundH(H) Lat Lon Locked Sats Status

Behavior:
 - Reads `analysis/output_new/merged_data.csv` by default
 - Tries to use available numeric columns for easting/northing/depth
 - Attempts to detect lat/lon columns if present; otherwise leaves them blank
 - Writes `Output/OWENDO-BATHY-SURVEY-generated.txt` under the dataset folder
"""
from pathlib import Path
import json
import pandas as pd
import numpy as np
from typing import Optional, Dict


def detect_latlon(df: pd.DataFrame) -> tuple[Optional[str], Optional[str]]:
    """Find candidate lat/lon columns by name. Avoid using depth-like columns.

    Prefer columns whose names contain 'lat'/'lon' or 'latitude'/'longitude'.
    Returns (lat_col, lon_col) or (None, None).
    """
    names = {c: c.lower() for c in df.columns}
    lat_candidates = [c for c, lc in names.items() if 'lat' in lc or 'latitude' in lc]
    lon_candidates = [c for c, lc in names.items() if 'lon' in lc or 'longitude' in lc]
    lat_col = lat_candidates[0] if lat_candidates else None
    lon_col = lon_candidates[0] if lon_candidates else None
    return lat_col, lon_col


def export_survey(merged_csv: str | Path,
                  out_txt: str | Path,
                  groundh_offset: Optional[float] = None,
                  cols: Optional[Dict[str, str]] = None,
                  cfg_path: Optional[str | Path] = None) -> Path:
    merged_csv = Path(merged_csv)
    out_txt = Path(out_txt)
    df = pd.read_csv(merged_csv)

    # If a configuration path is provided, load and prefer its column mapping
    if cfg_path is not None:
        try:
            with open(cfg_path, "r", encoding="utf-8") as fh:
                cfg = json.load(fh)
            # populate cols from cfg if not explicitly provided
            if cols is None:
                cols = {}
            for k in ("easting_col", "northing_col", "depth_col", "lat_col", "lon_col", "ground_col"):
                if k in cfg and cfg[k] is not None and k not in cols:
                    # map keys to simple names
                    if k.endswith("_col"):
                        cols[k[:-4]] = cfg[k]
        except Exception:
            pass

    # identify easting/northing/depth candidates, prefer explicit cols param
    easting_col = cols.get("easting") if cols else None
    northing_col = cols.get("northing") if cols else None
    depth_col = cols.get("depth") if cols else None

    if easting_col is None or northing_col is None or depth_col is None:
        for c in df.columns:
            nc = c.lower()
            if easting_col is None and nc in ("easting", "x", "coordx", "coordinatex", "f6", "f6_num"):
                easting_col = c
            if northing_col is None and nc in ("northing", "y", "coordy", "coordinatey", "f7", "f7_num"):
                northing_col = c
            if depth_col is None and nc in ("depth", "z", "h", "f8", "f8_num"):
                depth_col = c

    # final fallback: use last three numeric columns
    if easting_col is None or northing_col is None or depth_col is None:
        numeric_cols = [c for c in df.columns if np.issubdtype(df[c].dtype, np.number)]
        if len(numeric_cols) >= 3:
            easting_col = easting_col or numeric_cols[-3]
            northing_col = northing_col or numeric_cols[-2]
            depth_col = depth_col or numeric_cols[-1]

    if easting_col is None or northing_col is None or depth_col is None:
        raise RuntimeError("Could not detect easting/northing/depth columns in merged CSV")

    # detect lat/lon (allow override in cols)
    lat_col = cols.get("lat") if cols else None
    lon_col = cols.get("lon") if cols else None
    if lat_col is None or lon_col is None:
        dlat, dlon = detect_latlon(df)
        lat_col = lat_col or dlat
        lon_col = lon_col or dlon

    # compute GroundH(H): prefer ground column from cfg/cols, else use offset or heuristic
    ground_col = cols.get("ground") if cols else None
    if ground_col is None and groundh_offset is None:
        ground_candidates = [c for c in df.columns if np.issubdtype(df[c].dtype, np.number) and df[c].min() < -0.5]
        if ground_candidates:
            ground_col = next((c for c in ground_candidates if 'ground' in c.lower()), None)
        else:
            ground_col = None

    out_txt.parent.mkdir(parents=True, exist_ok=True)
    with out_txt.open("w", encoding="utf-8") as fh:
        fh.write("CoordinateY CoordinateX h GroundH(H) Lat Lon Locked Sats Status\n")
        for _, row in df.iterrows():
            # CoordinateY CoordinateX h GroundH(H) Lat Lon Locked Sats Status
            y = row.get(northing_col, "")
            x = row.get(easting_col, "")
            h = row.get(depth_col, "")

            if ground_col is not None and ground_col in row:
                gh = row.get(ground_col, "")
            elif groundh_offset is not None:
                try:
                    gh = float(h) - float(groundh_offset)
                except Exception:
                    gh = ""
            else:
                try:
                    gh = float(h) - 0.4
                except Exception:
                    gh = ""

            lat = row.get(lat_col, "") if lat_col is not None else ""
            lon = row.get(lon_col, "") if lon_col is not None else ""

            locked = 0
            sats = 4
            status = ""

            # normalize numeric formatting for floats
            def fmt(v):
                try:
                    if v == "":
                        return ""
                    if isinstance(v, (int, float, np.floating, np.integer)):
                        return f"{float(v):.6f}"
                    return str(v)
                except Exception:
                    return str(v)

            fh.write(
                f"{fmt(y)} {fmt(x)} {fmt(h)} {fmt(gh)} {fmt(lat)} {fmt(lon)} {locked} {sats} {status}\n"
            )

    return out_txt


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--merged", default="analysis/output_new/merged_data.csv")
    p.add_argument("--out", default="owendo-05-04-26-4-Outcome data_uzf/Output/OWENDO-BATHY-SURVEY-generated.txt")
    p.add_argument("--offset", type=float, default=None)
    args = p.parse_args()
    out = export_survey(args.merged, args.out, groundh_offset=args.offset)
    print(f"Wrote: {out}")
