from pathlib import Path
import sys
import shutil

# ensure project root is on sys.path so we can import analysis.process_uzf
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.process_uzf import process_uzf_file

# configure input UZF and output dir
uzf = Path(r"c:\Users\Admin\Pictures\DAT.ERT\batrimetrie\bord de mer bathy\testbaty-Outcome data.uzf")
outdir = Path('analysis/output_new')

res = process_uzf_file(str(uzf), outdir=str(outdir))
# find gps csv in result or output dir
canonical = outdir / (uzf.stem + '_gps.csv')
found = None
if 'gps_csv' in res and res['gps_csv']:
    found = Path(res['gps_csv'])
else:
    # try common names
    for p in outdir.glob(uzf.stem + '*gps*.csv'):
        found = p
        break

if found is None:
    print('No gps CSV generated')
else:
    if found.resolve() != canonical.resolve():
        try:
            shutil.copy2(str(found), str(canonical))
            print('Copied', found, '->', canonical)
        except Exception:
            try:
                found.replace(canonical)
                print('Renamed', found, '->', canonical)
            except Exception as e:
                print('Could not copy/rename:', e)
    else:
        print('GPS CSV already at canonical location:', canonical)

# print small head
if canonical.exists():
    txt = canonical.read_text(encoding='utf-8', errors='replace')
    lines = txt.splitlines()
    print('LINES_COUNT:', len(lines))
    print('\n'.join(lines[:10]))
else:
    print('Canonical GPS CSV not found at', canonical)
