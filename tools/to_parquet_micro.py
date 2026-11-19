# Usage: to_parquet_micro.py module_dir db outdir [key=value ...]
# The Harmony tables written by microparquet's writer: sorted by lookup key,
# default settings unless overridden (e.g. page_size=4096 compression_level=19).
import sqlite3, sys, os, time
sys.path.insert(0, sys.argv[1]); import microparquet as mpq
db = sqlite3.connect(sys.argv[2]); od = sys.argv[3]; os.makedirs(od, exist_ok=True)
kw = {k: (int(v) if v.isdigit() else v) for k, v in (a.split('=', 1) for a in sys.argv[4:])}
ORDER = {'codeset_cmd': ['codeset_id', 'ord'], 'device': ['manufacturer', 'model']}
total = 0
for t in ['protocol', 'code', 'name', 'codeset', 'codeset_cmd', 'device']:
    cur = db.execute(f'select * from {t}'); cols = [d[0] for d in cur.description]; rows = cur.fetchall()
    data = {c: [r[i] for r in rows] for i, c in enumerate(cols)}
    t0 = time.perf_counter()
    mpq.write(os.path.join(od, t + '.parquet'), data, sort_by=ORDER.get(t, ['id']), **kw)
    dt = time.perf_counter() - t0; total += dt
    print(f'{t:12s} rows={len(rows):8d} write={dt:6.2f}s size={os.path.getsize(os.path.join(od, t + ".parquet"))/1e6:6.2f}MB')
print(f'total write {total:.2f}s')
