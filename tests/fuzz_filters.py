# Usage: fuzz_filters.py module_dir out_dir n_tables [seed] [pyarrow|micro]
# 'micro' writes the tables with microparquet's writer instead of pyarrow, so
# pyarrow's own pruned reads also check the statistics and page index it wrote.
# Differential test of filters, limit and batches against pyarrow on random tables
# written with random settings. Prints each disagreement with a reproducer.
import sys, os, random, math, decimal, datetime as dt
sys.path.insert(0, sys.argv[1]); import microparquet as mpq
import pyarrow as pa, pyarrow.parquet as pq
out, N = sys.argv[2], int(sys.argv[3]); seed = int(sys.argv[4]) if len(sys.argv) > 4 else 0
WRITER = sys.argv[5] if len(sys.argv) > 5 else 'pyarrow'
os.makedirs(out, exist_ok=True); R = random.Random(seed)

def norm(v):
    if isinstance(v, float) and math.isnan(v): return 'NaN'
    if isinstance(v, float) and v == 0: return 0.0          # -0.0 vs 0.0 compare equal in both
    if isinstance(v, list): return [norm(x) for x in v]
    return v

STR = ['', 'a', 'ab', 'abc', 'b', 'é', 'ü', '中', '中文', 'z' * 70, 'z' * 71, '\x00', 'A', 'a ', 'aa']
def table(n):
    nul = lambda p, f: [None if R.random() < p else f() for _ in range(n)]
    pn = R.choice([0, 0, .1, .5, .95])
    ilo, ihi = R.choice([(0, 3), (-50, 50), (-2**62, 2**62)])
    cols = {
        'i': pa.array(nul(pn, lambda: R.randint(ilo, ihi)), pa.int64()),
        'f': pa.array(nul(pn, lambda: R.choice([0.0, -0.0, 1.5, -1.5, float('nan'), float('inf'), -float('inf'), R.uniform(-5, 5)])), pa.float64()),
        's': pa.array(nul(pn, lambda: R.choice(STR) + R.choice(['', '', str(R.randint(0, 9))]))),
        'd': pa.array(nul(pn, lambda: dt.date(2000, 1, 1) + dt.timedelta(days=R.randint(-400, 400))), pa.date32()),
        'b': pa.array(nul(pn, lambda: R.random() < .5), pa.bool_()),
        'u': pa.array(nul(pn, lambda: R.choice([0, 1, 2**31 - 1, 2**31, 2**32 - 1, R.randint(0, 2**32 - 1)])), pa.uint32()),
        'dec': pa.array(nul(pn, lambda: decimal.Decimal(R.randint(-10**6, 10**6)).scaleb(-2)), pa.decimal128(9, 2)),
    }
    t = pa.table(cols)
    if R.random() < .5: t = t.sort_by([(R.choice(['i', 's', 'f', 'u']), R.choice(['ascending', 'descending']))])
    return t

def settings():
    kw = dict(row_group_size=R.choice([7, 100, 1000, 100000]), data_page_size=R.choice([64, 512, 4096, 1 << 20]),
              write_page_index=R.random() < .6, use_dictionary=R.random() < .6,
              compression=R.choice(['none', 'snappy', 'zstd', 'gzip']), data_page_version=R.choice(['1.0', '2.0']),
              write_statistics=R.random() < .85)
    if not kw['use_dictionary'] and R.random() < .3:
        kw['column_encoding'] = {'i': R.choice(['DELTA_BINARY_PACKED', 'PLAIN']), 's': R.choice(['DELTA_BYTE_ARRAY', 'DELTA_LENGTH_BYTE_ARRAY', 'PLAIN'])}
    return kw

def literal(t, c):
    vals = [v for v in t[c].to_pylist() if v is not None]
    if vals and R.random() < .7: v = R.choice(vals)
    else: v = {'i': 0, 'f': 0.0, 's': 'a', 'd': dt.date(2000, 1, 1), 'b': True, 'u': 2**31, 'dec': decimal.Decimal('0.00')}[c]
    if c in ('i', 'u') and R.random() < .3: v = v + R.choice([-1, 1])
    if c == 'i' and R.random() < .1: v = float(v) + .5                 # int column vs float literal
    if c == 'f' and R.random() < .1: v = float('nan')
    if c == 's' and R.random() < .2: v = v + R.choice(['', '\x00', 'a', 'é'])
    return v

def pred(t):
    c = R.choice(list(t.column_names)); op = R.choice(['=', '==', '!=', '<', '<=', '>', '>=', 'in', 'not in'])
    if c == 'u' and op in ('in', 'not in'): op = '='
    if op in ('in', 'not in'): return (c, op, {literal(t, c) for _ in range(R.choice([0, 1, 2, 5]))})
    return (c, op, literal(t, c))

fails = 0; checks = 0; pa_bugs = []
for k in range(N):
    t = table(R.choice([0, 1, 5, 50, 500, 3000])); kw = settings()
    p = os.path.join(out, f'f{seed}_{k}.parquet')
    try:
        if WRITER == 'micro':
            kw = dict(row_group_size=R.choice([7, 100, 1000, 100000]), page_size=R.choice([16, 64, 512, 8192, 1 << 20]),
                      dictionary=R.choice(['auto', 'auto', True, False]), compression=R.choice(['none', 'zstd', 'gzip']))
            if R.random() < .5: kw['sort_by'] = R.sample(['i', 's', 'd', 'u', 'dec', 'b'], R.randint(1, 2))
            mpq.write(p, t.to_pydict(), {'i': 'int64', 'f': 'float64', 's': 'string', 'd': 'date', 'b': 'bool',
                                         'u': 'uint32', 'dec': 'decimal(9,2)'}, **kw)
        else: pq.write_table(t, p, **kw)
    except Exception as e:
        if WRITER == 'micro': fails += 1; print('WRITE FAIL', p, kw, repr(e))
        continue
    pf = mpq.ParquetFile(p)
    for _ in range(6):
        flt = [pred(t) for _ in range(R.choice([1, 1, 2, 3]))]
        if R.random() < .25: flt = [flt, [pred(t)]]
        cols = R.sample(t.column_names, R.randint(1, len(t.column_names)))
        try:      # gold standard: pyarrow's filter semantics applied to all rows, no pruning
            full = pq.read_table(p); ref = full.filter(pq.filters_to_expression(flt)).select(cols).to_pydict()
        except Exception: continue                                  # pyarrow can't express it
        try:
            pruned = pq.read_table(p, columns=cols, filters=flt).to_pydict()
            if not all(norm(pruned[c]) == norm(ref[c]) for c in cols): pa_bugs.append((p, flt))
        except Exception: pass
        n = len(ref[cols[0]]); checks += 1
        what = 'read'
        try:
            got = pf.read(cols, filters=flt)
            ok = all(norm(list(got[c])) == norm(ref[c]) for c in cols)
            if ok:
                what = 'limit'; lim = R.randint(0, n + 2)
                got2 = pf.read(cols, filters=flt, limit=lim)
                ok = all(norm(list(got2[c])) == norm(ref[c][:lim]) for c in cols)
            if ok:
                what = 'batches'; bs = R.choice([1, 3, 64, 1000])
                acc = {c: [] for c in cols}
                for b in pf.iter_batches(cols, flt, batch_size=bs):
                    for c in cols: acc[c] += list(b[c])
                ok = all(norm(acc[c]) == norm(ref[c]) for c in cols)
        except Exception as e:
            ok = False; got = repr(e)
        if not ok:
            fails += 1
            print(f'FAIL[{what}] file={p} settings={kw}\n   filters={flt!r} cols={cols}\n   ref rows={n} got={str(got)[:300]}', flush=True)
print(f'{checks} checks, {fails} failures; pyarrow filtered reads disagreeing with pyarrow semantics: {len(pa_bugs)}')
for p, flt in pa_bugs[:5]: print('   pyarrow pruning bug?', p, flt)
sys.exit(1 if fails else 0)
