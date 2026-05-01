from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from analysis import parse_raw_and_merge as prm

p = Path('owendo-05-04-26-4-Outcome data_uzf') / 'data'
print('data dir exists?', p.exists())
files = prm.find_data_files(p)
print('found data files:', len(files))
for f in files[:10]:
    rows = prm.parse_data_file(f)
    print(f.name, 'rows:', len(rows))
