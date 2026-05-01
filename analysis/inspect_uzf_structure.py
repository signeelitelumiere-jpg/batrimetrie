import binascii
from pathlib import Path
import zipfile
import sqlite3


def hexdump_bytes(b: bytes, length=256):
    toshow = b[:length]
    hexs = binascii.hexlify(toshow).decode('ascii')
    # spaced hex
    spaced = ' '.join(hexs[i:i+2] for i in range(0, len(hexs), 2))
    return spaced


def extract_uzf(uzf_path, dest_dir):
    uzf = Path(uzf_path)
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(uzf, 'r') as zf:
            zf.extractall(dest)
    except zipfile.BadZipFile:
        # not a zip or corrupt
        raise
    return dest


def inspect(uzf_path):
    uzf = Path(uzf_path)
    print('UZF path:', uzf)
    print('\n--- HEX dump (first 512 bytes) of .uzf file ---')
    with open(uzf, 'rb') as fh:
        b = fh.read(512)
        print(hexdump_bytes(b, 512))

    # try extract
    tmp = Path('analysis/temp_extracted_for_inspect')
    if tmp.exists():
        import shutil
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    try:
        extract_uzf(uzf, tmp)
    except Exception as e:
        print('Extraction failed:', e)
        return

    # find .data (sqlite) files
    dbs = list(tmp.rglob('*.data'))
    print('\nFound .data files:')
    for d in dbs:
        print(' -', d)

    if not dbs:
        print('No .data found in archive.')
    
    for db in dbs:
        print('\n--- HEX dump (first 256 bytes) of', db.name, '---')
        with open(db, 'rb') as fh:
            b = fh.read(256)
            print(hexdump_bytes(b, 256))

        print('\n--- SQLite schema and sample rows for', db.name, '---')
        conn = sqlite3.connect(str(db))
        cur = conn.cursor()
        # tables
        tbls = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        for t in tbls:
            print('\nTable:', t)
            cols = cur.execute(f"PRAGMA table_info('{t}')").fetchall()
            # PRAGMA returns (cid,name,type,notnull,dflt_value,pk)
            for c in cols:
                cid, name, ctype, notnull, dflt, pk = c
                print(f'  - column: {name}  type: {ctype}  pk:{pk} notnull:{notnull} default:{dflt}')

            # show up to 5 rows, but present actual repr for each value; for bytes show hex
            rows = cur.execute(f"SELECT * FROM '{t}' LIMIT 5").fetchall()
            if rows:
                print('  sample rows:')
                for row in rows:
                    out = []
                    for val in row:
                        if isinstance(val, (bytes, bytearray)):
                            s = hexdump_bytes(val, 64)
                            out.append(f'<BLOB hex:{s}>')
                        else:
                            out.append(repr(val))
                    print('   ', ', '.join(out))
            else:
                print('  (no rows)')

        conn.close()


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print('Usage: inspect_uzf_structure.py <path_to_uzf>')
        raise SystemExit(1)
    inspect(sys.argv[1])
