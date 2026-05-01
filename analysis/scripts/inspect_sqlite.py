import sqlite3
from pathlib import Path
p = Path('analysis/temp_extracted_testbaty/testbaty/Backup/testbaty.data')
if not p.exists():
    print('DB not found at', p)
    raise SystemExit(1)
conn = sqlite3.connect(str(p))
c = conn.cursor()
rows = c.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view')").fetchall()
print('Found objects:')
for name,typ in rows:
    print('-', typ, name)

for name,typ in rows:
    print('\n==', name, '==')
    try:
        cols = [d[0] for d in c.execute(f"PRAGMA table_info('{name}')").fetchall()]
        print('columns:', cols)
        for r in c.execute(f"SELECT * FROM '{name}' LIMIT 5"):
            print(r)
    except Exception as e:
        print('Error reading table', name, e)

conn.close()
