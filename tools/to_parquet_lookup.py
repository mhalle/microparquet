# Usage: to_parquet_lookup.py db outdir -- lookup-tuned layout: sorted by key, 64K-row groups,
# 4 KB pages, page index, dictionary only for low-cardinality string columns.
import sqlite3, sys, os, pyarrow as pa, pyarrow.parquet as pq
db = sqlite3.connect(sys.argv[1]); od = sys.argv[2]; os.makedirs(od, exist_ok=True)
ORDER = {'codeset_cmd': 'codeset_id, ord', 'device': 'manufacturer, model'}
DICT = ['manufacturer', 'keycode', 'protocol_id', 'name_id', 'code_id']
for t in ['protocol', 'code', 'name', 'codeset', 'codeset_cmd', 'device']:
    cur = db.execute(f'select * from {t} order by {ORDER.get(t, "id")}'); cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    tbl = pa.table({c: [r[i] for r in rows] for i, c in enumerate(cols)})
    pq.write_table(tbl, os.path.join(od, t + '.parquet'), compression='zstd', compression_level=19,
                   row_group_size=65536, data_page_size=4096, write_page_index=True,
                   use_dictionary=[c for c in cols if c in DICT])
