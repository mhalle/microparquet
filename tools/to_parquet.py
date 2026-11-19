# Usage: to_parquet.py db outdir [sorted]
# "sorted" writes the lookup-friendly layout: sorted by key, small row groups, page index on.
import sqlite3, sys, os, pyarrow as pa, pyarrow.parquet as pq
db = sqlite3.connect(sys.argv[1]); od = sys.argv[2]; os.makedirs(od, exist_ok=True)
tuned = len(sys.argv) > 3 and sys.argv[3] == 'sorted'
ORDER = {'codeset_cmd': 'codeset_id, ord', 'device': 'manufacturer, model', 'code': 'id', 'name': 'id', 'codeset': 'id', 'protocol': 'id'}
for t in ['protocol', 'code', 'name', 'codeset', 'codeset_cmd', 'device']:
    q = f'select * from {t}' + (f' order by {ORDER[t]}' if tuned else '')
    cur = db.execute(q); cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    tbl = pa.table({c: [r[i] for r in rows] for i, c in enumerate(cols)})
    kw = dict(compression='zstd', compression_level=19)
    if tuned: kw.update(row_group_size=65536, write_page_index=True, data_page_size=8192)
    pq.write_table(tbl, os.path.join(od, t + '.parquet'), **kw)
