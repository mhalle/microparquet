# Usage: real_files.py module_dir file_or_dir... [--no-time]
# Reads each file with microparquet and pyarrow (to_pydict, nested form) and compares,
# printing per-column mismatches and timings.
import sys, os, math, time, uuid, glob
sys.path.insert(0, sys.argv[1]); import microparquet as mpq
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc

def norm(v):
    if isinstance(v, float) and math.isnan(v): return 'NaN'
    if isinstance(v, (list, tuple)): return [norm(x) for x in v]
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    if isinstance(v, uuid.UUID): return v.bytes
    return v

def fix_ns(col):
    ty = col.type
    if pa.types.is_timestamp(ty) and ty.unit == 'ns':
        return pc.floor_temporal(col, unit='microsecond').cast(pa.timestamp('us', ty.tz), safe=False)
    if pa.types.is_duration(ty): return col.cast(pa.int64())       # durations: we return raw ints
    if pa.types.is_dictionary(ty): return col.cast(ty.value_type)
    return col

files = []
for a in sys.argv[2:]:
    if a.startswith('--'): continue
    files += sorted(glob.glob(os.path.join(a, '*.parquet'))) if os.path.isdir(a) else [a]
bad_files = []
for f in files:
    name = os.path.basename(f)
    try:
        pfa = pq.ParquetFile(f); pf = mpq.ParquetFile(f)
    except Exception as e:
        print(f'{name:40s} open fails: {type(e).__name__} {e}'[:200], flush=True); bad_files.append(name); continue
    t_pa = t_tp = 0.0; bad = set(); extra = set(); err = None; rows = 0
    try:
        it = pf.iter_row_groups()
        for i in range(pfa.num_row_groups):
            t0 = time.perf_counter(); tb = pfa.read_row_group(i); ref = {n: fix_ns(c).to_pylist() for n, c in zip(tb.column_names, tb.columns)}
            t_pa += time.perf_counter() - t0; rows += tb.num_rows
            if not tb.num_rows: continue
            t0 = time.perf_counter(); mine = next(it); t_tp += time.perf_counter() - t0
            bad |= {c for c in ref if c not in mine or norm(list(mine[c])) != norm(ref[c])}
            extra |= {c for c in mine if c not in ref}
    except Exception as e:
        err = f'{type(e).__name__}: {e}'
    status = 'ERROR ' + err if err else 'MATCH' if not bad and not extra else f'MISMATCH {sorted(bad)} extra={sorted(extra)}'
    if err or bad or extra: bad_files.append(name)
    print(f'{name:40s} rows={rows:>9,} micro={t_tp:6.2f}s pyarrow={t_pa:5.2f}s {status}'[:260], flush=True)
print('\nPROBLEM FILES:', bad_files or 'none')
sys.exit(1 if bad_files else 0)
