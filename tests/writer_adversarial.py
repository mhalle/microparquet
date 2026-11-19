# Usage: writer_adversarial.py module_dir tmp_dir
# Bad inputs, type mismatches, failure atomicity and odd-but-legal tables for the writer.
import sys, os, io, math, decimal, datetime as dt, glob, traceback
sys.path.insert(0, sys.argv[1]); import microparquet as mpq
import pyarrow.parquet as pq
T = sys.argv[2]; os.makedirs(T, exist_ok=True)
fails = 0
def ok(label, cond, detail=''):
    global fails
    if not cond: fails += 1; print('FAIL', label, str(detail)[:300])
def raises(label, exc, fn):
    try: fn()
    except exc as e:
        ok(label + ' (no temp files left)', not glob.glob(os.path.join(T, '.*.tmp')), glob.glob(os.path.join(T, '.*.tmp')))
        for f in glob.glob(os.path.join(T, '.*.tmp')): os.remove(f)
        return str(e)
    except Exception as e: ok(label, False, 'wrong exception %r' % e); return
    ok(label, False, 'no exception')
P = lambda n: os.path.join(T, n)
def both(p):
    a = pq.read_table(p).to_pydict(); b = {k: list(v) for k, v in mpq.read(p).items()}; return a, b

# -- rejected inputs, each leaving no file behind
cases = [
    ('mixed str/int', TypeError, {'x': [1, 'a']}, {}),
    ('mixed bool/int', TypeError, {'x': [True, 2]}, {}),
    ('int > 64 bits', ValueError, {'x': [2**64]}, {}),
    ('int < -2**63', ValueError, {'x': [-2**63 - 1]}, {}),
    ('unsupported object', TypeError, {'x': [object()]}, {}),
    ('set value', TypeError, {'x': [{1, 2}]}, {}),
    ('NaN decimal', ValueError, {'x': [decimal.Decimal('NaN')]}, {}),
    ('int32 overflow', (ValueError, OverflowError), {'x': [2**31]}, {'x': 'int32'}),
    ('uint32 negative', (ValueError, OverflowError), {'x': [-1]}, {'x': 'uint32'}),
    ('int8 overflow', (ValueError, OverflowError), {'x': [128]}, {'x': 'int8'}),
    ('float32 overflow', (ValueError, OverflowError), {'x': [1e300]}, {'x': 'float32'}),
    ('str in int column', TypeError, {'x': ['1']}, {'x': 'int64'}),
    ('float in int column', TypeError, {'x': [1.5]}, {'x': 'int64'}),
    ('surrogate string', (ValueError, UnicodeError), {'x': ['\ud800']}, {}),
    ('decimal too wide', ValueError, {'x': [decimal.Decimal('123.45')]}, {'x': 'decimal(4,2)'}),
    ('naive into utc ts', (TypeError, ValueError), {'x': [dt.datetime(2020, 1, 1)]}, {'x': 'timestamp[us,UTC]'}),
    ('aware into naive ts', (TypeError, ValueError), {'x': [dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)]}, {'x': 'timestamp[us]'}),
    ('mixed naive/aware', TypeError, {'x': [dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)]}, {}),
    ('scalar in list column', TypeError, {'x': [[1], 2]}, {}),
    ('scalar in declared list', TypeError, {'x': [3]}, {'x': 'list<int64>'}),
    ('non-dict in struct column', TypeError, {'x': [{'a': 1}, [1]]}, {}),
    ('unknown type name', ValueError, {'x': [1]}, {'x': 'int128'}),
    ('bad decimal spec', ValueError, {'x': [1]}, {'x': 'decimal(4,5)'}),
    ('types for missing column', KeyError, {'x': [1]}, {'y': 'int64'}),
    ('ragged columns', ValueError, {'x': [1, 2], 'y': [1]}, {}),
    ('non-string name', TypeError, {1: [1]}, {}),
    ('uuid wrong type', TypeError, {'x': ['not-a-uuid']}, {'x': 'uuid'}),
    ('bytes in string col', TypeError, {'x': [b'a']}, {'x': 'string'}),
    ('date into timestamp', TypeError, {'x': [dt.date(2020, 1, 1)]}, {'x': 'timestamp[ms]'}),
]
for label, exc, data, types in cases:
    raises(label, exc, lambda: mpq.write(P('bad.parquet'), data, types))
    ok(label + ' no output', not os.path.exists(P('bad.parquet')))
    if os.path.exists(P('bad.parquet')): os.remove(P('bad.parquet'))
for label, kw in [('sort_by unknown', dict(sort_by='nope')), ('sort_by nested', dict(sort_by='l')),
                  ('row_group_size 0', dict(row_group_size=0)), ('page_size 0', dict(page_size=0)),
                  ('bad compression', dict(compression='lzma')), ('bad dictionary', dict(dictionary='sometimes'))]:
    raises(label, (ValueError, KeyError, TypeError), lambda: mpq.write(P('bad.parquet'), {'x': [1], 'l': [[1]]}, **kw))
    ok(label + ' no output', not os.path.exists(P('bad.parquet')))
    if os.path.exists(P('bad.parquet')): os.remove(P('bad.parquet'))

# -- an existing file survives a failed rewrite
mpq.write(P('keep.parquet'), {'x': [1, 2, 3]})
raises('failed overwrite', TypeError, lambda: mpq.write(P('keep.parquet'), {'x': [1, 'a']}))
ok('old file intact', list(mpq.read(P('keep.parquet'))['x']) == [1, 2, 3])

# -- streaming: a bad batch is rejected whole; the writer stays usable
w = mpq.ParquetWriter(P('stream.parquet'))
w.write({'a': [1, 2], 's': ['x', 'y']})
for bad in ({'a': [3, 4], 's': ['z', 5]}, {'a': [3, 'q'], 's': ['z', 'w']}, {'a': [3], 'b': ['z']}, {'s': ['z'], 'a': [3]}, {'a': [2**70], 's': ['z']}):
    try: w.write(bad); ok('bad batch %r accepted' % bad, False)
    except (TypeError, ValueError, OverflowError): pass
w.write({'a': [5, None], 's': [None, 'v']}); w.close()
a, b = both(P('stream.parquet'))
ok('stream contents (pyarrow)', a == {'a': [1, 2, 5, None], 's': ['x', 'y', None, 'v']}, a)
ok('stream contents (micro)', b == a, b)
raises('write after close', ValueError, lambda: w.write({'a': [1], 's': ['x']}))

# -- exception inside "with" removes the temp file and writes nothing
try:
    with mpq.ParquetWriter(P('ctx.parquet')) as w:
        w.write({'a': [1]}); raise RuntimeError('boom')
except RuntimeError: pass
ok('ctx abort: no file', not os.path.exists(P('ctx.parquet')) and not glob.glob(P('.*.tmp')))

# -- an abandoned writer does not leave its temp file forever
w = mpq.ParquetWriter(P('abandoned.parquet')); w.write({'a': [1]}); del w
import gc; gc.collect()
ok('abandoned: temp file removed', not glob.glob(P('.*.tmp')), glob.glob(P('.*.tmp')))

# -- odd but legal tables
legal = {
    'empty dict': ({}, {}),
    'zero rows': ({'a': [], 's': []}, {}),
    'zero rows typed': ({'a': []}, {'a': 'int32'}),
    'all null': ({'a': [None, None]}, {}),
    'all null typed': ({'a': [None, None]}, {'a': 'string'}),
    'empty lists only': ({'l': [[], [], None]}, {}),
    'dotted names': ({'a.b': [1, 2], 'a': [{'c': 'x'}, {'c': 'y'}]}, {}),
    'unicode names': ({'名前': ['x'], 'naïve col': [1]}, {}),
    'deep nesting': ({'d': [[[{'k': [1, None, 2]}]], None, [[None, {'k': None}], []]]}, {}),
    'list of lists of str': ({'l': [[['a'], [], None], None]}, {}),
    'one huge string': ({'s': ['x' * 5_000_000]}, {}),
    'bytes types': ({'b': [b'', bytearray(b'ab'), memoryview(b'cd')]}, {}),
    'tuple as list': ({'t': [(1, 2), (3,)]}, {}),
    'rows input': ([{'a': 1}, {'b': 'x'}, {}], {}),
    'extreme ints': ({'i': [-2**63, 2**63 - 1, 0]}, {}),
    'uint64 inferred': ({'i': [0, 2**64 - 1]}, {}),
    'float specials': ({'f': [math.inf, -math.inf, math.nan, -0.0, 5e-324]}, {}),
    'timestamp ns': ({'t': [dt.datetime(2262, 4, 11), dt.datetime(1677, 9, 22)]}, {'t': 'timestamp[ns]'}),
    'decimal 38': ({'d': [decimal.Decimal('9' * 38), decimal.Decimal('-' + '9' * 38)]}, {'d': 'decimal(38,0)'}),
    'json type': ({'j': ['{"a": 1}', None]}, {'j': 'json'}),
    'null type': ({'n': [None]}, {'n': 'null'}),
}
for label, (data, types) in legal.items():
    p = P('legal.parquet')
    try:
        mpq.write(p, data, types, row_group_size=2, page_size=1)
        a, b = both(p)
        exp = {k: list(v) for k, v in mpq._columns(data).items()}
        norm = lambda d: repr({k: [('nan' if isinstance(x, float) and math.isnan(x) else bytes(x) if isinstance(x, (bytearray, memoryview)) else
                                    [list(y) if isinstance(y, tuple) else y for y in x] if False else x) for x in v] for k, v in d.items()})
        if label == 'tuple as list': exp = {'t': [[1, 2], [3]]}
        if label == 'rows input': exp = {'a': [1, None, None], 'b': [None, 'x', None]}
        ok(label + ' micro', norm(b) == norm(exp), (b, exp))
        ok(label + ' pyarrow', norm(a) == norm(b) or label in ('uint64 inferred',) or (label == 'timestamp ns' and a == b), (a, b))
    except Exception: ok(label, False, traceback.format_exc()[-600:])

raises('ambiguous dotted paths', ValueError, lambda: mpq.write(P('amb.parquet'), {'a.b': [1], 'a': [{'b': 'x'}]}))
# a failed batch in a file object is cut off again
buf = io.BytesIO(); w = mpq.ParquetWriter(buf); w.write({'a': [1], 's': ['x']})
try: w.write({'a': [2], 's': [3]})
except TypeError: pass
w.write({'a': [4], 's': ['y']}); w.close()
ok('BytesIO rollback', {k: list(v) for k, v in mpq.read(buf.getvalue()).items()} == {'a': [1, 4], 's': ['x', 'y']})
# a failed first batch lets the next batch choose the schema
w = mpq.ParquetWriter(P('refirst.parquet'))
try: w.write({'a': [1, 'x']})
except TypeError: pass
w.write({'b': ['ok']}); w.close()
ok('schema after failed first batch', {k: list(v) for k, v in mpq.read(P('refirst.parquet')).items()} == {'b': ['ok']})

# -- file objects as destinations
buf = io.BytesIO(); mpq.write(buf, {'a': [1, 2]})
ok('BytesIO', list(mpq.read(buf.getvalue())['a']) == [1, 2])
raises('dest is a directory', (OSError, ValueError), lambda: mpq.write(T, {'a': [1]}))
raises('dest dir missing', OSError, lambda: mpq.write(os.path.join(T, 'nope', 'x.parquet'), {'a': [1]}))
print(f'{fails} failures')
sys.exit(1 if fails else 0)
