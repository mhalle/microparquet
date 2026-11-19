# Usage: bench.py lib task tables_dir [module_dir]
# tasks: all (every table -> Python values), lookup (one device's commands with Pronto)
import sys, os, time, resource
t0 = time.perf_counter()
lib, task, d = sys.argv[1], sys.argv[2], sys.argv[3]
if len(sys.argv) > 4: sys.path.insert(0, sys.argv[4])
MAN, MOD = 'Magnavox', 'RJ5540'
T = ['protocol', 'code', 'name', 'codeset', 'codeset_cmd', 'device']
P = lambda t: d + '/' + t + '.parquet' if d.startswith('http') else os.path.join(d, t + '.parquet')
if lib == 'micro': import microparquet as mpq
elif lib == 'pyarrow': import pyarrow.parquet as pq
elif lib == 'polars': import polars as pl
elif lib == 'duckdb': import duckdb
t1 = time.perf_counter()
extra = ''
if task == 'all':
    if lib == 'micro': r = [mpq.read(P(t)) for t in T]
    if lib == 'pyarrow': r = [pq.read_table(P(t)).to_pydict() for t in T]
    if lib == 'polars': r = [pl.read_parquet(P(t)).to_dict(as_series=False) for t in T]
    if lib == 'duckdb': r = [duckdb.sql(f"select * from '{P(t)}'").fetchall() for t in T]
elif task == 'lookup':
    if lib == 'micro':
        files = {t: (mpq.open_url(P(t)) if d.startswith('http') else mpq.ParquetFile(P(t))) for t in ['device', 'codeset_cmd', 'name', 'code']}
        cs = files['device'].read(['codeset_id'], [('manufacturer', '=', MAN), ('model', '=', MOD)])['codeset_id'][0]
        cc = files['codeset_cmd'].read(['ord', 'name_id', 'code_id'], [('codeset_id', '=', cs)])
        rows = sorted(zip(cc['ord'], cc['name_id'], cc['code_id']))
        nm = files['name'].read(['id', 'name'], [('id', 'in', {n for o, n, c in rows})])
        pr = files['code'].read(['id', 'pronto'], [('id', 'in', {c for o, n, c in rows})])
        nm, pr = dict(zip(nm['id'], nm['name'])), dict(zip(pr['id'], pr['pronto']))
        r = [(nm[n], pr[c]) for o, n, c in rows]
        if d.startswith('http'):
            s = [f._src for f in files.values()]
            extra = f' requests={sum(x.requests for x in s)} bytes={sum(x.bytes_read for x in s)/1e3:.0f}KB'
    elif lib == 'duckdb':
        r = duckdb.sql(f"""select n.name, c.pronto from '{P('device')}' d
            join '{P('codeset_cmd')}' cc on cc.codeset_id = d.codeset_id
            join '{P('name')}' n on n.id = cc.name_id join '{P('code')}' c on c.id = cc.code_id
            where d.manufacturer = '{MAN}' and d.model = '{MOD}' order by cc.ord""").fetchall()
    elif lib == 'polars':
        r = (pl.scan_parquet(P('device')).filter((pl.col('manufacturer') == MAN) & (pl.col('model') == MOD))
             .join(pl.scan_parquet(P('codeset_cmd')), on='codeset_id')
             .join(pl.scan_parquet(P('name')).rename({'id': 'name_id'}), on='name_id')
             .join(pl.scan_parquet(P('code')).rename({'id': 'code_id'}), on='code_id')
             .sort('ord').select('name', 'pronto').collect().rows())
    elif lib == 'pyarrow':
        dv = pq.read_table(P('device'), columns=['codeset_id'], filters=[('manufacturer', '=', MAN), ('model', '=', MOD)])
        cs = dv['codeset_id'][0].as_py()
        cc = pq.read_table(P('codeset_cmd'), filters=[('codeset_id', '=', cs)]).sort_by('ord').to_pylist()
        nm = pq.read_table(P('name'), filters=[('id', 'in', list({x['name_id'] for x in cc}))]).to_pydict()
        pr = pq.read_table(P('code'), columns=['id', 'pronto'], filters=[('id', 'in', list({x['code_id'] for x in cc}))]).to_pydict()
        nm, pr = dict(zip(nm['id'], nm['name'])), dict(zip(pr['id'], pr['pronto']))
        r = [(nm[x['name_id']], pr[x['code_id']]) for x in cc]
    extra = f' n={len(r)} first={r[0][0]!r} md5={__import__("hashlib").md5(repr(r).encode()).hexdigest()[:8]}' + extra
t2 = time.perf_counter()
rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
print(f"{lib:8s} {task:7s} import={t1-t0:5.2f}s work={t2-t1:6.3f}s peakRSS={rss:5.0f}MB{extra}")
