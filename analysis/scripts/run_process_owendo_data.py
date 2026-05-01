from pathlib import Path
import sys
import json

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / 'owendo-05-04-26-4-Outcome data_uzf' / 'data'
OUT_DIR = ROOT / 'analysis' / 'output_new'
CFG_PATH = ROOT / 'owendo-05-04-26-4-Outcome data_uzf' / 'owendo_config.json'

def main():
    sys.path.insert(0, str(ROOT / 'analysis'))
    try:
        from parse_raw_and_merge import main as parse_main
    except Exception as e:
        print('Cannot import parse_raw_and_merge:', e)
        return 2

    if not DATA_DIR.exists():
        print('Data dir not found:', DATA_DIR)
        return 3

    print('Running parse_raw_and_merge on', DATA_DIR)
    # call parse_raw_and_merge as a module (it uses argparse in main)
    try:
        # emulate CLI args; ensure --out is a file path
        out_file = OUT_DIR / 'merged_data.csv'
        sys.argv = [sys.argv[0], '--data-dir', str(DATA_DIR), '--out', str(out_file), '--force']
        parse_main()
    except SystemExit as e:
        if e.code != 0:
            print('parse_raw_and_merge exited with', e.code)
            return int(e.code or 1)
    except Exception as e:
        print('parse_raw_and_merge failed:', e)
        return 4

    # write minimal config if missing
    CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
    cfg = {}
    candidate = OUT_DIR / 'merged_data.csv'
    if candidate.exists():
        cfg['merged_csv'] = str(candidate)
    cfg.setdefault('groundh_offset', 0)
    CFG_PATH.write_text(json.dumps(cfg, indent=2), encoding='utf-8')
    print('Wrote config:', CFG_PATH)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
