# Usage: corpus.py module_dir out_dir
# Generates a corpus with pyarrow (plus polars and duckdb when installed) and checks
# microparquet against pyarrow for full reads, column subsets, filters, array and
# dictionary output modes.
import sys, os, random, math, datetime as dt, decimal, uuid, traceback
sys.path.insert(0, sys.argv[1]); import microparquet as mpq
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
out = sys.argv[2]; os.makedirs(out, exist_ok=True); random.seed(1)

def norm(v):
    if isinstance(v, float) and math.isnan(v): return 'NaN'
    if isinstance(v, (list, tuple)): return [norm(x) for x in v]
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    if isinstance(v, uuid.UUID): return v.bytes
    return v

def ref_table(p, **kw):
    t = pq.read_table(p, **kw)
    cols = {}
    for name, col in zip(t.column_names, t.columns):
        ty = col.type
        if pa.types.is_timestamp(ty) and ty.unit == 'ns':
            col = pc.floor_temporal(col, unit='microsecond').cast(pa.timestamp('us', ty.tz))
        if pa.types.is_time64(ty) and ty.unit == 'ns':
            col = pc.cast(col.cast(pa.int64()), pa.int64())
            col = pa.array([None if x is None else x // 1000 for x in col.to_pylist()], pa.int64()).cast(pa.time64('us'))
        if isinstance(ty, pa.ExtensionType): col = col.cast(ty.storage_type) if hasattr(col, 'cast') else col
        cols[name] = col.to_pylist()
    return cols

OPS = {'=': lambda a, b: a == b, '!=': lambda a, b: a != b, '<': lambda a, b: a < b, '<=': lambda a, b: a <= b,
       '>': lambda a, b: a > b, '>=': lambda a, b: a >= b, 'in': lambda a, b: a in b, 'not in': lambda a, b: a not in b}
def py_filter(ref, flt):
    """Independent reference: evaluate DNF filters in plain Python (nulls never match)."""
    dnf = [flt] if isinstance(flt[0], tuple) else flt
    n = len(next(iter(ref.values())))
    def get(c, i):                      # dotted path into struct dicts
        top, *rest = c.split('.'); v = ref[top][i]
        for k in rest: v = None if v is None else v[k]
        return v
    keep = [i for i in range(n) if any(all(get(c, i) is not None and OPS[op](get(c, i), x) for c, op, x in conj) for conj in dnf)]
    return {c: [v[i] for i in keep] for c, v in ref.items()}
def same(a, b): return norm(list(a)) == norm(list(b))
fails = []
def check(label, ok, detail=''):
    print(f'  {"ok  " if ok else "FAIL"} {label} {detail}')
    if not ok: fails.append(label)

def verify(p, filters_list=(), expect_error=None):
    name = os.path.basename(p)
    print(name)
    try:
        pf = mpq.ParquetFile(p)
        mine = pf.read()
    except Exception as e:
        if expect_error and isinstance(e, expect_error): print('  ok   expected error:', e); return
        traceback.print_exc(); check('read', False); return
    if expect_error: check('expected error', False); return
    ref = ref_table(p)
    bad = [c for c in ref if not same(ref[c], mine.get(c, []))]
    check('full read', not bad, bad)
    # column subset in reverse order
    some = list(ref)[::-2]
    sub = pf.read(some)
    check('subset', list(sub) == some and all(same(sub[c], ref[c]) for c in some))
    # arrays / dictionary modes must round-trip to the same values
    for kw in (dict(arrays=True), dict(dictionaries=True)):
        m2 = pf.read(**kw)
        conv = {c: (list(v) if not isinstance(v, mpq.DictionaryColumn) else v.to_list()) for c, v in m2.items()}
        okc = []
        for n in pf.column_names:
            c = pf._nodes[n]
            if kw.get('arrays') and c.leaf is not None and pf.columns[c.leaf].tc and pf.columns[c.leaf].conv: continue  # raw values by design
            okc.append(same(conv[n], mine[n]))
        check(f'{list(kw)[0]} mode', all(okc))
    for flt in filters_list:
        try:
            try: r = ref_table(p, filters=flt)
            except Exception: r = py_filter(ref, flt)
            m = pf.read(filters=flt)
            bad = [c for c in r if not same(r[c], m[c])]
            check(f'filter {flt}', not bad, f'rows={len(next(iter(r.values())))} {bad}')
        except Exception:
            traceback.print_exc(); check(f'filter {flt}', False)

N = 120_000
def nul(xs, p): return [None if random.random() < p else x for x in xs]
base = pa.table({
    'i64_hi': pa.array([random.getrandbits(40) for _ in range(N)], pa.int64()),
    'i32': pa.array(nul([random.randint(-10**9, 10**9) for _ in range(N)], .1), pa.int32()),
    'f64': pa.array(nul([random.random() for _ in range(N)], .3), pa.float64()),
    'f32': pa.array([random.random() for _ in range(N)], pa.float32()),
    'b': pa.array(nul([random.random() < .5 for _ in range(N)], .2), pa.bool_()),
    's': pa.array(nul([random.choice(['α', 'beta', '', 'x' * 50]) + str(random.randint(0, 999)) for _ in range(N)], .05)),
    'allnull': pa.array([None] * N, pa.string()),
    'runs': pa.array([i // 5000 for i in range(N)], pa.int64()),
})
F = [[('runs', '=', 7)], [('runs', '>=', 20), ('i32', '<', 0)], [('s', 'in', {'beta5', 'x' * 50 + '17'})],
     [[('runs', '<', 1)], [('runs', '>', 22)]], [('b', '=', True), ('runs', '!=', 3)], [('allnull', '=', 'x')],
     [('f64', '>', 0.999)], [('runs', 'not in', {0, 1, 2})]]
variants = {
    'zstd_dict_v1': dict(compression='zstd'),
    'zstd_plain_v1': dict(compression='zstd', use_dictionary=False),
    'gzip_v2': dict(compression='gzip', data_page_version='2.0'),
    'none_v2_plain': dict(compression='none', data_page_version='2.0', use_dictionary=False),
    'small_rg': dict(compression='zstd', row_group_size=7_777, data_page_size=4096),
    'snappy': dict(compression='snappy'),
    'snappy_v2_plain': dict(compression='snappy', data_page_version='2.0', use_dictionary=False),
    'lz4': dict(compression='lz4'),
    'pageindex': dict(compression='zstd', row_group_size=30_000, data_page_size=2048, write_page_index=True),
    'pageindex_v2': dict(compression='snappy', data_page_version='2.0', data_page_size=1024, write_page_index=True),
    'pageindex_many_pages': dict(compression='zstd', row_group_size=200_000, data_page_size=128, write_batch_size=40, write_page_index=True),   # >256 pages per chunk
    'no_stats': dict(write_statistics=False),
    'delta': dict(use_dictionary=False, column_encoding={'i64_hi': 'DELTA_BINARY_PACKED', 'i32': 'DELTA_BINARY_PACKED',
                  'runs': 'DELTA_BINARY_PACKED', 's': 'DELTA_BYTE_ARRAY', 'allnull': 'DELTA_LENGTH_BYTE_ARRAY'}),
    'delta_len': dict(use_dictionary=False, data_page_version='2.0', column_encoding={'s': 'DELTA_LENGTH_BYTE_ARRAY'}),
    'bss': dict(use_dictionary=False, column_encoding={'f64': 'BYTE_STREAM_SPLIT', 'f32': 'BYTE_STREAM_SPLIT',
                'i32': 'BYTE_STREAM_SPLIT', 'i64_hi': 'BYTE_STREAM_SPLIT'}),
    'dict_fallback': dict(dictionary_pagesize_limit=4096, data_page_size=4096),
}
for name, kw in variants.items():
    p = os.path.join(out, name + '.parquet'); pq.write_table(base, p, **kw); verify(p, F)

# delta edge cases: overflow, extremes, tiny/odd counts
for n in (0, 1, 2, 127, 128, 129, 1000):
    vals = [random.choice([-2**63, 2**63 - 1, 0, -1, random.getrandbits(63)]) for _ in range(n)]
    t = pa.table({'q': pa.array(vals, pa.int64()), 'i': pa.array([v % 2**32 - 2**31 for v in vals], pa.int32()),
                  'u64': pa.array([v % 2**64 for v in vals], pa.uint64()), 'u32': pa.array([v % 2**32 for v in vals], pa.uint32())})
    p = os.path.join(out, f'delta_edge_{n}.parquet')
    pq.write_table(t, p, use_dictionary=False, column_encoding='DELTA_BINARY_PACKED'); verify(p)

# logical types
M = 5000
utc = dt.timezone.utc
lt = pa.table({
    'date': pa.array(nul([dt.date(1900, 1, 1) + dt.timedelta(days=random.randint(0, 60000)) for _ in range(M)], .1), pa.date32()),
    'ts_ms': pa.array([dt.datetime(2020, 1, 1) + dt.timedelta(milliseconds=random.randint(0, 10**10)) for _ in range(M)], pa.timestamp('ms')),
    'ts_us_utc': pa.array(nul([dt.datetime(1960, 1, 1, tzinfo=utc) + dt.timedelta(microseconds=random.randint(0, 10**16)) for _ in range(M)], .2), pa.timestamp('us', 'UTC')),
    'ts_ns': pa.array([random.randint(0, 2**62) for _ in range(M)], pa.timestamp('ns')),
    't_ms': pa.array([random.randint(0, 86399999) for _ in range(M)], pa.time32('ms')),
    't_us': pa.array([random.randint(0, 86399999999) for _ in range(M)], pa.time64('us')),
    'dec9': pa.array([decimal.Decimal(random.randint(-10**8, 10**8)).scaleb(-3) for _ in range(M)], pa.decimal128(9, 3)),
    'dec18': pa.array(nul([decimal.Decimal(random.randint(-10**17, 10**17)).scaleb(-6) for _ in range(M)], .1), pa.decimal128(18, 6)),
    'dec38': pa.array([decimal.Decimal(random.randint(-10**37, 10**37)).scaleb(-10) for _ in range(M)], pa.decimal128(38, 10)),
    'u8': pa.array([random.randint(0, 255) for _ in range(M)], pa.uint8()),
    'u32': pa.array([random.randint(0, 2**32 - 1) for _ in range(M)], pa.uint32()),
    'u64': pa.array([random.randint(0, 2**64 - 1) for _ in range(M)], pa.uint64()),
    'i8': pa.array([random.randint(-128, 127) for _ in range(M)], pa.int8()),
    'i16': pa.array([random.randint(-2**15, 2**15 - 1) for _ in range(M)], pa.int16()),
    'bin': pa.array([random.randbytes(random.randint(0, 20)) for _ in range(M)], pa.binary()),
    'fixed': pa.array([random.randbytes(6) for _ in range(M)], pa.binary(6)),
    'large_str': pa.array(['ü' * random.randint(0, 5) for _ in range(M)], pa.large_string()),
    'f16': pa.array([random.random() for _ in range(M)], pa.float16()) if hasattr(pa, 'float16') else pa.array([0.0] * M),
    'uuid': pa.array([uuid.uuid4().bytes for _ in range(M)], pa.uuid()),
    'nanf': pa.array([float('nan') if random.random() < .1 else random.random() for _ in range(M)], pa.float64()),
    'st': pa.array(nul([{'a': random.randint(0, 9), 'b': nul(['x'], .3)[0]} for _ in range(M)], .2),
                   pa.struct([('a', pa.int32()), ('b', pa.string())])),
})
LF = [[('date', '<', dt.date(1950, 1, 1))], [('ts_us_utc', '>=', dt.datetime(2200, 1, 1, tzinfo=utc))],
      [('dec18', '>', decimal.Decimal('99999999999'))], [('u64', '>', 2**63)], [('u32', '<=', 1000000)],
      [('nanf', '>', .5)], [('fixed', '<', b'\x10')], [('st.a', '=', 3)]]
for name, kw in {'logical_dict': {}, 'logical_plain': dict(use_dictionary=False),
                 'logical_v2_snappy': dict(data_page_version='2.0', compression='snappy'),
                 'logical_pi': dict(write_page_index=True, data_page_size=1024, row_group_size=1500)}.items():
    p = os.path.join(out, name + '.parquet'); pq.write_table(lt, p, **kw); verify(p, LF)

# legacy writer settings: INT96 timestamps, parquet format 1.0 converted types
legacy = pa.table({'ts': pa.array([dt.datetime(1999, 1, 1) + dt.timedelta(seconds=random.randint(0, 10**9)) for _ in range(M)], pa.timestamp('us')),
                   'u16': pa.array([random.randint(0, 65535) for _ in range(M)], pa.uint16())})
p = os.path.join(out, 'int96.parquet')
pq.write_table(legacy, p, use_deprecated_int96_timestamps=True, version='1.0'); verify(p)
p = os.path.join(out, 'brotli.parquet'); pq.write_table(legacy, p, compression='brotli'); verify(p, expect_error=None if mpq._cramjam or mpq._brotli else ImportError)

# list column: must give a clear error when read, but other columns stay readable
lst = pa.table({'id': list(range(100)), 'xs': [[i] * (i % 3) for i in range(100)]})
p = os.path.join(out, 'list.parquet'); pq.write_table(lst, p)
print('list.parquet')
check('flat column of file with list', mpq.read(p, ['id'])['id'] == list(range(100)))
check('list column', same(mpq.read(p, ['xs'])['xs'], pq.read_table(p)['xs'].to_pylist()))


# richer list columns: null lists, empty lists, null elements, strings, many pages
L = 20_000
def rl(mk):
    r = random.random()
    return None if r < .1 else [] if r < .2 else [mk() if random.random() > .1 else None for _ in range(random.randint(1, 6))]
lists = pa.table({'id': list(range(L)), 'li': [rl(lambda: random.randint(0, 99)) for _ in range(L)],
                  'ls': [rl(lambda: random.choice(['a', 'bb', 'ccc'])) for _ in range(L)],
                  'lreq': pa.array([[1, 2]] * L, pa.list_(pa.field('item', pa.int64(), nullable=False)))})
for name, kw in {'lists_v1': dict(data_page_size=2048, row_group_size=7000), 'lists_v2': dict(data_page_version='2.0', data_page_size=1024),
                 'lists_pi': dict(write_page_index=True, data_page_size=1024, row_group_size=6000)}.items():
    p = os.path.join(out, name + '.parquet'); pq.write_table(lists, p, **kw)
    print(name); r = pq.read_table(p).to_pydict(); m = mpq.read(p)
    check('lists full read', all(same(r[c], m[c]) for c in r))
    m = mpq.read(p, ['id', 'ls'], filters=[('id', '>=', 12345), ('id', '<', 12400)])
    check('lists filtered', same(m['ls'], r['ls'][12345:12400]) and m['id'] == list(range(12345, 12400)))

# other writers
try:
    import polars as pl
    df = pl.from_arrow(base)
    for comp in ('zstd', 'snappy', 'lz4', 'uncompressed'):
        p = os.path.join(out, f'polars_{comp}.parquet'); df.write_parquet(p, compression=comp, row_group_size=40_000); verify(p, F[:4])
    p = os.path.join(out, 'polars_logical.parquet'); pl.from_arrow(lt.drop(['uuid', 'f16'])).write_parquet(p); verify(p, LF[:5])
except ImportError: print('polars not installed')
try:
    import duckdb
    con = duckdb.connect(); con.register('base', base); con.register('lt', lt.drop(['uuid', 'f16', 'st', 'u64', 'large_str']))
    for comp in ('zstd', 'snappy', 'gzip', 'uncompressed'):
        p = os.path.join(out, f'duckdb_{comp}.parquet')
        con.execute(f"copy base to '{p}' (format parquet, compression {comp}, row_group_size 50000)"); verify(p, F[:4])
    p = os.path.join(out, 'duckdb_v2.parquet')
    con.execute(f"copy base to '{p}' (format parquet, parquet_version v2)"); verify(p, F[:4])
    p = os.path.join(out, 'duckdb_logical.parquet'); con.execute(f"copy lt to '{p}' (format parquet)"); verify(p, [LF[0], LF[1], LF[2], LF[4]])
except ImportError: print('duckdb not installed')

print('\nFAILURES:', fails if fails else 'none')
sys.exit(1 if fails else 0)
