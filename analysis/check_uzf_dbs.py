import zipfile, tempfile, shutil, sqlite3, sys
from pathlib import Path

if len(sys.argv) < 2:
    print('Usage: check_uzf_dbs.py <path_to.uzf>')
    raise SystemExit(1)

uzf = Path(sys.argv[1])
if not uzf.exists():
    print('File not found', uzf)
    raise SystemExit(1)

tmp = Path(tempfile.mkdtemp(prefix='chkuzf_'))
try:
    ztmp = tmp / 'in.zip'
    shutil.copy2(uzf, ztmp)
    try:
        with zipfile.ZipFile(ztmp, 'r') as zf:
            zf.extractall(tmp)
    except zipfile.BadZipFile:
        print('BadZipFile')

    db_files = list(tmp.rglob('*.data'))
    if not db_files:
        print('No .data files found')
    for db in db_files:
        print('---', db)
        try:
            conn = sqlite3.connect(str(db))
            cur = conn.cursor()
            # list tables
            tabs = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            print('tables:', tabs)
            if 'gps_data' in tabs:
                try:
                    n = cur.execute('SELECT COUNT(*) FROM gps_data').fetchone()[0]
                    print('gps_data rows:', n)
                except Exception as e:
                    print('gps_data count error:', e)
            conn.close()
        except Exception as e:
            print('open error', e)
finally:
    shutil.rmtree(tmp)
