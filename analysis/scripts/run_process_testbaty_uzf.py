from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
UZF_PATH = ROOT / 'bord de mer bathy' / 'testbaty-Outcome data.uzf'
OUT_DIR = ROOT / 'analysis' / 'output_new'
OWENDO_FOLDER = ROOT / 'owendo-05-04-26-4-Outcome data_uzf'

def main():
    sys.path.insert(0, str(ROOT / 'analysis'))
    try:
        from process_uzf import process_uzf_file
    except Exception as e:
        print('Cannot import process_uzf:', e)
        return 2

    if not UZF_PATH.exists():
        print('UZF file not found:', UZF_PATH)
        return 3

    print('Processing UZF:', UZF_PATH)
    res = process_uzf_file(str(UZF_PATH), outdir=str(OUT_DIR))
    print('Result keys:', list(res.keys()))

    # write minimal owendo_config.json so other scripts can use it
    OWENDO_FOLDER.mkdir(parents=True, exist_ok=True)
    cfg_path = OWENDO_FOLDER / 'owendo_config.json'
    cfg = {}
    # prefer merged_csv / merged_auto / merged_structured keys
    for k in ('merged_csv','merged_auto','merged_structured','merged'):
        if k in res:
            cfg['merged_csv'] = res[k]
            break
    # fallback: look for a merged file in output dir with stem
    if 'merged_csv' not in cfg:
        candidate = OUT_DIR / (UZF_PATH.stem + '_merged_auto.csv')
        if candidate.exists():
            cfg['merged_csv'] = str(candidate)

    # default offsets
    cfg.setdefault('groundh_offset', 0)
    cfg.setdefault('source_uzf', str(UZF_PATH))
    cfg_path.write_text(json.dumps(cfg, indent=2), encoding='utf-8')
    print('Wrote owendo config:', cfg_path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
