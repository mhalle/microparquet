# Usage: verify_tables.py module_dir tables_dir  -- full read of each table vs pyarrow
import sys, os, time
sys.path.insert(0, sys.argv[1]); import microparquet
import pyarrow.parquet as pq
d = sys.argv[2]
for t in ['protocol','code','name','codeset','codeset_cmd','device']:
    p = os.path.join(d, t + '.parquet')
    t0 = time.perf_counter(); mine = microparquet.read(p); dt = time.perf_counter() - t0
    mine = {k: list(v) for k, v in mine.items()}
    ref = pq.read_table(p).to_pydict()
    print(f'{t:12s} rows={len(next(iter(ref.values()))):>9,} {dt:6.2f}s', 'MATCH' if ref == mine else 'MISMATCH')
