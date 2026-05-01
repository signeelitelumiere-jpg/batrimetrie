#!/usr/bin/env python3
"""Wrapper to produce OWENDO-BATHY-SURVEY.txt at the exact required path.

Reads `owendo-05-04-26-4-Outcome data_uzf/owendo_config.json` to locate the merged CSV,
then calls `export_survey` to write the requested TXT file with the exact header.
"""
from pathlib import Path
import json
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'analysis'))
from export_survey_format import export_survey


def main():
    cfg_path = Path('owendo-05-04-26-4-Outcome data_uzf/owendo_config.json')
    if not cfg_path.exists():
        print('Config not found:', cfg_path)
        return 2

    cfg = json.loads(cfg_path.read_text(encoding='utf-8'))
    merged = cfg.get('merged_csv') or cfg.get('merged') or 'analysis/output_new/merged_data.csv'
    merged_path = Path(merged)
    if not merged_path.exists():
        print('Merged CSV not found:', merged_path)
        return 3

    out_path = Path('owendo-05-04-26-4-Outcome data_uzf/Output/OWENDO-BATHY-SURVEY.txt')
    out = export_survey(merged_path, out_path, groundh_offset=cfg.get('groundh_offset'), cfg_path=cfg_path)
    print('Wrote:', out)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
