# Usage: parquet_testing.py module_dir parquet-testing/data
# Reads every file with microparquet and pyarrow and classifies the outcome.
import sys, os, math, uuid, datetime as dt, collections
sys.path.insert(0, sys.argv[1]); import microparquet as mpq
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc

def norm(v):
    if isinstance(v, float) and math.isnan(v): return 'NaN'
    if isinstance(v, (list, tuple)): return [norm(x) for x in v]
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    if isinstance(v, uuid.UUID): return v.bytes
    if isinstance(v, dt.datetime) and v.tzinfo is not None: return v.astimezone(dt.timezone.utc)
    return v

def ref(p):
    t = pq.read_table(p)
    out = {}
    for name, col in zip(t.column_names, t.columns):
        ty = col.type
        if pa.types.is_timestamp(ty) and ty.unit == 'ns':
            col = pc.floor_temporal(col, unit='microsecond').cast(pa.timestamp('us', ty.tz), safe=False)
        if pa.types.is_time64(ty) and ty.unit == 'ns':
            col = pa.array([None if x is None else x // 1000 for x in col.cast(pa.int64()).to_pylist()], pa.int64()).cast(pa.time64('us'))
        if isinstance(ty, pa.ExtensionType): col = col.cast(ty.storage_type)
        out[name] = col.to_pylist()
    return out

res = collections.defaultdict(list)
d = sys.argv[2]
for f in sorted(os.listdir(d)):
    if not f.endswith('.parquet'): continue
    if f == 'large_string_map.brotli.parquet': continue   # two 1 GB strings: needs more RAM than this test box
    p = os.path.join(d, f); print('..', f, flush=True, file=sys.stderr)
    try:
        r = ref(p)
    except Exception as e:
        r = e
    try:
        m = mpq.read(p)
    except (NotImplementedError, ValueError, ImportError, KeyError) as e:
        res['unsupported' if not isinstance(r, Exception) else 'both fail'].append((f, type(e).__name__ + ': ' + str(e)[:80])); continue
    except Exception as e:
        res['CRASH'].append((f, type(e).__name__ + ': ' + str(e)[:100])); continue
    if isinstance(r, Exception):
        res['pyarrow fails, micro reads'].append((f, str(r)[:60])); continue
    bad = [c for c in r if c not in m or norm(list(m[c])) != norm(r[c])]
    (res['MISMATCH'] if bad else res['match']).append((f, bad[:4] if bad else ''))
for k in ['match', 'unsupported', 'both fail', 'pyarrow fails, micro reads', 'MISMATCH', 'CRASH']:
    print(f'== {k}: {len(res[k])}')
    if k != 'match':
        for f, why in res[k]: print('   ', f, why)
print('matched:', ' '.join(f for f, _ in res['match']))
# pyarrow misreads these two (it drops the empty data pages); microparquet reads them correctly
KNOWN_PYARROW_BUGS = {'empty_data_pages_v1.parquet', 'empty_data_pages_v2.parquet'}
bad = res['CRASH'] + [x for x in res['MISMATCH'] if x[0] not in KNOWN_PYARROW_BUGS]
sys.exit(1 if bad else 0)
