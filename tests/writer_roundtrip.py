# Usage: writer_roundtrip.py module_dir out_dir n_tables [seed]
# Random tables (every supported type, nulls, nesting, edge values) written by
# microparquet with random settings, then read back by pyarrow, polars, duckdb,
# fastparquet (flat columns) and microparquet, and compared with the input.
import sys, os, random, math, decimal, datetime as dt, uuid, struct, traceback
from array import array
sys.path.insert(0, sys.argv[1]); import microparquet as mpq
import pyarrow.parquet as pq
out, N = sys.argv[2], int(sys.argv[3]); seed = int(sys.argv[4]) if len(sys.argv) > 4 else 0
os.makedirs(out, exist_ok=True); R = random.Random(seed)
UTC = dt.timezone.utc

def f32(x): return struct.unpack('<f', struct.pack('<f', x))[0]
STR = ['', 'a', 'é', '中文', 'z' * 100, '\x00', 'emoji 🎉', 'a b', 'A']

# column generators: (python value maker, declared type or None, expected-value transform)
def gen_columns(n):
    nul = lambda p, f: [None if R.random() < p else f() for _ in range(n)]
    p = R.choice([0, 0, .2, .9, 1.0])
    scale = R.randint(0, 6)
    cols = {
        'i64': (nul(p, lambda: R.choice([0, -1, 2**63 - 1, -2**63, R.randint(-1000, 1000)])), None, None),
        'f64': (nul(p, lambda: R.choice([0.0, -0.0, 1.5, float('inf'), -float('inf'), float('nan'), R.uniform(-1e9, 1e9)])), None, None),
        's': (nul(p, lambda: R.choice(STR) + R.choice(['', str(R.randint(0, 99))])), None, None),
        'bin': (nul(p, lambda: bytes(R.randrange(256) for _ in range(R.randint(0, 20)))), None, None),
        'b': (nul(p, lambda: R.random() < .5), None, None),
        'd': (nul(p, lambda: dt.date(1, 1, 1) + dt.timedelta(days=R.randint(0, 3652058))), None, None),
        'ts': (nul(p, lambda: dt.datetime(1900, 1, 1) + dt.timedelta(microseconds=R.randint(0, 2**57))), None, None),
        'tsz': (nul(p, lambda: dt.datetime(1970, 1, 1, tzinfo=UTC) + dt.timedelta(microseconds=R.randint(-2**55, 2**55))), None, None),
        'dec': (nul(p, lambda: decimal.Decimal(R.randint(-10**12, 10**12)).scaleb(-scale)), None, None),
        'u': (nul(p, lambda: uuid.UUID(int=R.getrandbits(128))), None, None),
        'i32': (nul(p, lambda: R.randint(-2**31, 2**31 - 1)), 'int32', None),
        'u32': (nul(p, lambda: R.choice([0, 2**31, 2**32 - 1, R.randint(0, 2**32 - 1)])), 'uint32', None),
        'u64': (nul(p, lambda: R.choice([0, 2**63, 2**64 - 1, R.randint(0, 2**64 - 1)])), 'uint64', None),
        'f32': (nul(p, lambda: R.uniform(-10, 10)), 'float32', f32),
        'tms': (nul(p, lambda: dt.datetime(2000, 1, 1) + dt.timedelta(milliseconds=R.randint(0, 2**40))), 'timestamp[ms]', None),
        'tm': (nul(p, lambda: (dt.datetime.min + dt.timedelta(microseconds=R.randint(0, 86399999999))).time()), 'time', None),
        'dec30': (nul(p, lambda: decimal.Decimal(R.randint(-10**29, 10**29)).scaleb(-4)), 'decimal(30,4)', None),
        'lst': (nul(p, lambda: [R.choice([None, R.randint(0, 9)]) for _ in range(R.randint(0, 5))]), None, None),
        'lls': (nul(p, lambda: [None if R.random() < .1 else [R.choice(STR) for _ in range(R.randint(0, 3))] for _ in range(R.randint(0, 3))]), None, None),
        'st': (nul(p, lambda: {'a': R.choice([None, R.randint(0, 9)]), 'b': R.choice([None, 'x', 'yy'])}), None, None),
        'msgs': (nul(p, lambda: [None if R.random() < .1 else {'role': R.choice(['user', 'assistant']), 'content': R.choice([None, 'hi', 'x' * 50]),
                                  'n': R.choice([None, 1.5])} for _ in range(R.randint(0, 4))]), None, None),
        'sl': (nul(p, lambda: {'xs': [R.randint(0, 3) for _ in range(R.randint(0, 3))], 'tag': R.choice(['p', 'q'])}), None, None),
    }
    pick = R.sample(list(cols), R.randint(1, len(cols)))
    return {k: cols[k] for k in pick}

def norm(v):
    if isinstance(v, float):
        if math.isnan(v): return 'NaN'
        if v == 0: return ('zero', math.copysign(1, v))
        return v
    if isinstance(v, (list, tuple)): return [norm(x) for x in v]
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    if isinstance(v, uuid.UUID): return v.bytes
    if isinstance(v, dt.datetime) and v.tzinfo is not None: return ('tz', v.astimezone(UTC).replace(tzinfo=None))
    if isinstance(v, decimal.Decimal): return ('dec', str(v.normalize()) if v else '0')
    if isinstance(v, (bytearray, memoryview)): return bytes(v)
    return v

fails = 0
all_forms = []
def check(label, ok, detail=''):
    global fails
    if not ok:
        fails += 1; print(f'FAIL {label} {str(detail)[:400]}', flush=True)

for k in range(N):
    n = R.choice([0, 1, 7, 100, 1000, 5000])
    spec = gen_columns(n)
    data = {c: v for c, (v, t, f) in spec.items()}
    types = {c: t for c, (v, t, f) in spec.items() if t}
    expect = {c: [f(x) if (f and x is not None) else x for x in v] for c, (v, t, f) in spec.items()}
    kw = dict(compression=R.choice(['zstd', 'gzip', 'none', 'snappy']), page_size=R.choice([64, 1024, 8192, 1 << 20]),
              row_group_size=R.choice([1, 50, 1000, 65536]), dictionary=R.choice(['auto', True, False]))
    sortable = [c for c in data if c in ('i64', 's', 'd', 'ts', 'i32', 'u32', 'u64', 'dec', 'b', 'bin', 'tms', 'tm')]
    if sortable and R.random() < .4: kw['sort_by'] = R.choice(sortable)
    p = os.path.join(out, f'w{seed}_{k}.parquet')
    batched = R.random() < .5                    # streaming: random batches and input forms
    if batched:
        if 'sort_by' not in kw and R.random() < .5: kw['row_group_bytes'] = R.choice([1, 500, 20000])
        types = {**{c: mpq._infer(v, c) for c, v in data.items()}, **types}   # the first batch may be all null
        cuts = sorted({0, n} | {R.randrange(n + 1) for _ in range(R.randint(0, 6))})
        batches = [(a, b) for a, b in zip(cuts, cuts[1:])] or [(0, 0)]
        forms = all_forms
    label = f'{p} {kw} batched={batches if batched else None} types={types}'
    def as_form(c, v):                           # list, array.array or DictionaryColumn for a batch slice
        f = R.random()
        tc = {'i64': 'q', 'f64': 'd', 'i32': 'i', 'u32': 'I', 'u64': 'Q', 'f32': 'f'}.get(c)
        if tc and None not in v and f < .4 and not (c == 'f64' and any(isinstance(x, int) for x in v)):
            forms.append((c, 'array')); return array(tc, v)
        if f > .7 and c not in ('lst', 'lls', 'st', 'msgs', 'sl'):
            dk = lambda x: ('f', struct.pack('<d', x)) if isinstance(x, float) else x   # keeps -0.0 and NaN apart
            ix, d = {}, []
            for x in v:
                if x is not None and dk(x) not in ix: ix[dk(x)] = len(d); d.append(x)
            idx = array('i', [-1 if x is None else ix[dk(x)] for x in v])
            forms.append((c, 'dict')); return mpq.DictionaryColumn(idx, d)
        return v
    try:
        if not batched: mpq.write(p, data, types, **kw)
        else:
            with mpq.ParquetWriter(p, types, **kw) as w:
                for a, b in batches: w.write({c: as_form(c, v[a:b]) for c, v in data.items()})
    except Exception:
        check('write ' + label, False, traceback.format_exc()); continue
    if 'sort_by' in kw:                          # expected order: stable sort, nulls last, per flushed buffer
        key = data[kw['sort_by']]
        srt = lambda ix: sorted(ix, key=lambda i: (key[i] is None, key[i] if key[i] is not None else 0))
        rgs = kw['row_group_size']; order, buf = [], []
        for a, b in (batches if batched else [(0, n)]):
            buf += range(a, b)
            if len(buf) >= rgs:
                buf = srt(buf); m = len(buf) // rgs * rgs; order += buf[:m]; buf = buf[m:]
        order += srt(buf)
        expect = {c: [v[i] for i in order] for c, v in expect.items()}
    if batched and n:                            # row groups: full ones, then the rest
        md = pq.ParquetFile(p).metadata; sizes = [md.row_group(i).num_rows for i in range(md.num_row_groups)]
        if 'row_group_bytes' not in kw:
            check('row group sizes ' + label, sizes == [kw['row_group_size']] * (n // kw['row_group_size']) + ([n % kw['row_group_size']] if n % kw['row_group_size'] else []), sizes)
        else: check('row group sizes ' + label, sum(sizes) == n and all(sizes), sizes)
    exp = {c: norm(v) for c, v in expect.items()}
    # microparquet
    try:
        m = mpq.read(p); check('micro ' + label, all(norm(list(m[c])) == exp[c] for c in data), [c for c in data if norm(list(m[c])) != exp[c]])
        for c in data:                           # filtered reads use the written statistics/page index
            if c in ('i64', 's', 'd', 'i32', 'u64') and n:
                vals = [x for x in expect[c] if x is not None]
                if not vals: continue
                x = R.choice(vals)
                got = mpq.read(p, [c], filters=[(c, '=', x)])[c]
                check(f'micro filter {c}={x!r} ' + label, list(got) == [v for v in expect[c] if v == x], (len(got), sum(v == x for v in expect[c] if v is not None)))
    except Exception: check('micro read ' + label, False, traceback.format_exc())
    # pyarrow (also validates the page index it reads)
    try:
        t = pq.read_table(p)
        r = {c: t[c].to_pylist() for c in data}
        bad = [c for c in data if norm(r[c]) != exp[c]]
        check('pyarrow ' + label, not bad, {c: (r[c][:3], expect[c][:3]) for c in bad[:2]})
        md = pq.ParquetFile(p).metadata
        if md.num_row_groups: check('pyarrow page index ' + label, md.row_group(0).column(0).has_offset_index and md.row_group(0).column(0).has_column_index)
    except Exception: check('pyarrow read ' + label, False, traceback.format_exc())
    # polars
    try:
        import polars as pl
        df = pl.read_parquet(p)
        for c in data:
            got = df[c].to_list()
            if c in ('u',): got = [None if x is None else bytes(x) for x in got]
            check(f'polars {c} ' + label, norm(got) == exp[c], (got[:3], expect[c][:3]))
    except Exception: check('polars read ' + label, False, traceback.format_exc()[-600:])
    # duckdb
    try:
        import duckdb
        con = duckdb.connect(); con.execute("SET TimeZone='UTC'")
        rows = con.execute(f"select {', '.join(chr(34) + c + chr(34) for c in data)} from read_parquet('{p}')").fetchall()
        for j, c in enumerate(data):
            got = [row[j] for row in rows]
            check(f'duckdb {c} ' + label, norm(got) == exp[c], (got[:3], expect[c][:3]))
    except Exception: check('duckdb read ' + label, False, traceback.format_exc()[-600:])
    # fastparquet: flat columns only
    try:
        import fastparquet
        flat = [c for c in data if c in ('i64', 's', 'b', 'f64', 'i32', 'bin')]
        if flat and n:
            df = fastparquet.ParquetFile(p).to_pandas(columns=flat)
            import pandas as pd
            for c in flat:
                got = [None if (x is None or x is pd.NA or (isinstance(x, float) and math.isnan(x) and expect[c][i] is None)) else
                       (x.item() if hasattr(x, 'item') else x) for i, x in enumerate(df[c].tolist())]
                if c == 'i64' and None in expect[c]: got = [None if x is None else int(x) for x in got]
                check(f'fastparquet {c} ' + label, norm(got) == exp[c], (got[:3], expect[c][:3]))
    except Exception:
        try:                                     # fastparquet bug, or ours? retry on a pyarrow-written copy
            pq.write_table(pq.read_table(p), p + '.pa'); fastparquet.ParquetFile(p + '.pa').to_pandas(columns=flat)
            check('fastparquet read ' + label, False, traceback.format_exc()[-600:])
        except Exception: pass                   # pyarrow's file fails the same way: not a writer problem
print(f'{N} files, {fails} failures; batched inputs used: {dict(__import__("collections").Counter(f for c, f in all_forms))}')
sys.exit(1 if fails else 0)
