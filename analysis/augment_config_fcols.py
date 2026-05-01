#!/usr/bin/env python3
"""Augment existing owendo_config.json with f-column -> semantic name mappings.

Scans `sample_rows` in the config (first row) and maps f2..f8 and their _num
variants to semantic names. Writes updated config back to the same path.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict


def infer_f_mappings(sample: Dict) -> Dict[str, str]:
    # default semantic names
    mapping = {}
    # high frequency indicators
    if "f2" in sample:
        mapping["f2"] = "HF_flag"
    if "f3" in sample:
        mapping["f3"] = "HF_value"
    if "f4" in sample:
        mapping["f4"] = "LF_flag"
    if "f5" in sample:
        mapping["f5"] = "LF_value"
    if "f6" in sample or "f6_num" in sample:
        mapping["f6"] = "easting"
        mapping["f6_num"] = "easting"
    if "f7" in sample or "f7_num" in sample:
        mapping["f7"] = "northing"
        mapping["f7_num"] = "northing"
    if "f8" in sample or "f8_num" in sample:
        mapping["f8"] = "depth"
        mapping["f8_num"] = "depth"
    return mapping


def main(cfg_path: Path):
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    sample_rows = cfg.get("sample_rows") or []
    if not sample_rows:
        raise SystemExit("No sample_rows in config to infer f-columns from.")
    sample = sample_rows[0]
    fmap = infer_f_mappings(sample)
    cfg.setdefault("f_mappings", {})
    cfg["f_mappings"].update(fmap)
    # also ensure easting_col/northing_col/depth_col are set to mapped names if present
    if cfg.get("easting_col") is None and ("f6" in fmap or "f6_num" in fmap):
        cfg["easting_col"] = fmap.get("f6_num") or fmap.get("f6")
    if cfg.get("northing_col") is None and ("f7" in fmap or "f7_num" in fmap):
        cfg["northing_col"] = fmap.get("f7_num") or fmap.get("f7")
    if cfg.get("depth_col") is None and ("f8" in fmap or "f8_num" in fmap):
        cfg["depth_col"] = fmap.get("f8_num") or fmap.get("f8")

    cfg_path.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Updated config with f_mappings: {cfg_path}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("cfg", default="owendo-05-04-26-4-Outcome data_uzf/owendo_config.json")
    args = p.parse_args()
    main(Path(args.cfg))
