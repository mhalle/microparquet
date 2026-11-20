# Usage: writer_adversarial.py module_dir tmp_dir
# Bad inputs, type mismatches, failure atomicity and odd-but-legal tables for the writer.
import sys, os, io, math, decimal, datetime as dt, glob, traceback
sys.path.insert(0, sys.argv[1]); import microparquet as mpq
import pyarrow as pa, pyarrow.parquet as pq   # pa.BufferReader: pyarrow can hang at exit after reading a BytesIO
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

# -- streaming: buffering, columnar inputs, row-group limits (session 7)
from array import array
import random
def rd(p): return {k: list(v) for k, v in mpq.read(p).items()}

# many small batches become full row groups, not one row group per batch
with mpq.ParquetWriter(P('many.parquet'), row_group_size=1000) as w:
    for i in range(250): w.write({'a': list(range(i * 10, i * 10 + 10))})
md = pq.ParquetFile(P('many.parquet')).metadata
ok('small batches -> full row groups', [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [1000, 1000, 500])
ok('small batches values', rd(P('many.parquet'))['a'] == list(range(2500)))

# a bad batch after buffered (not yet written) rows is dropped; buffered rows survive
w = mpq.ParquetWriter(P('buf.parquet'), row_group_size=100)
w.write({'a': [1, 2], 's': ['x', 'y']})
for bad in ({'a': [3], 's': [4]}, {'a': ['q'], 's': ['z']}, {'a': [2**64], 's': ['z']}, {'s': ['z'], 'a': [1]}):
    try: w.write(bad); ok('bad buffered batch accepted %r' % bad, False)
    except (TypeError, ValueError, OverflowError): pass
w.write({'a': [5], 's': [None]}); w.close()
ok('buffered rows kept after rejected batches', rd(P('buf.parquet')) == {'a': [1, 2, 5], 's': ['x', 'y', None]}, rd(P('buf.parquet')))

# a write that fails while flushing (I/O error part way) is undone, file and buffer
class Flaky(io.BytesIO):
    def __init__(self): super().__init__(); self.calls = 0; self.fail_at = None
    def write(self, b):
        self.calls += 1
        if self.fail_at is not None and self.calls == self.fail_at: raise OSError('disk full (simulated)')
        return super().write(b)
for fail_after in (1, 3, 5, 8):          # the batch makes 2 row groups x 2 columns x 2 writes
    f = Flaky(); w = mpq.ParquetWriter(f, row_group_size=50)
    w.write({'a': list(range(30)), 's': ['v%d' % i for i in range(30)]})            # buffered only
    f.fail_at = f.calls + fail_after
    try: w.write({'a': list(range(30, 130)), 's': ['v%d' % i for i in range(30, 130)]}); ok('flaky write %d raised' % fail_after, False)
    except OSError: pass
    f.fail_at = None
    w.write({'a': list(range(200, 260)), 's': ['w%d' % i for i in range(60)]}); w.close()
    got = {k: list(v) for k, v in mpq.read(f.getvalue()).items()}
    ok('flaky write %d undone' % fail_after, got == {'a': list(range(30)) + list(range(200, 260)), 's': ['v%d' % i for i in range(30)] + ['w%d' % i for i in range(60)]}, {k: v[:5] for k, v in got.items()})
    pa_ok = pq.read_table(pa.BufferReader(f.getvalue()), use_threads=False).num_rows == 90
    ok('flaky write %d: pyarrow reads the result' % fail_after, pa_ok)

# KeyboardInterrupt while flushing: same undo
class Interrupt(io.BytesIO):
    armed = False
    def write(self, b):
        if self.armed: self.armed = False; raise KeyboardInterrupt
        return super().write(b)
f = Interrupt(); w = mpq.ParquetWriter(f, row_group_size=10)
w.write({'a': list(range(5))}); f.armed = True
try: w.write({'a': list(range(5, 25))})
except KeyboardInterrupt: pass
w.write({'a': [99]}); w.close()
ok('interrupted flush undone', list(mpq.read(f.getvalue())['a']) == [0, 1, 2, 3, 4, 99])

# sort_by across buffered batches keeps rows together, nested columns included
R = random.Random(3); rows = [(R.randint(0, 40), [R.randint(0, 9) for _ in range(R.randint(0, 3))], {'x': R.choice([None, 'p', 'q'])}) for _ in range(700)]
with mpq.ParquetWriter(P('sorted.parquet'), sort_by='k', row_group_size=256) as w:
    for i in range(0, 700, 37):
        part = rows[i:i + 37]; w.write({'k': [r[0] for r in part], 'l': [r[1] for r in part], 'st': [r[2] for r in part]})
got = rd(P('sorted.parquet')); md = pq.ParquetFile(P('sorted.parquet')).metadata
ok('sorted batches: same rows', sorted(map(repr, zip(got['k'], got['l'], got['st']))) == sorted(map(repr, rows)))
starts = [0]
for i in range(md.num_row_groups): starts.append(starts[-1] + md.row_group(i).num_rows)
ok('sorted batches: each row group sorted', all(got['k'][a:b] == sorted(got['k'][a:b]) for a, b in zip(starts, starts[1:])))
ok('sorted batches: full row groups', [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [256, 256, 188])

# row_group_bytes splits wide data; a single row larger than the limit is its own group
mpq.write(P('bytes.parquet'), {'s': ['x' * 1000] * 50, 'i': list(range(50))}, row_group_bytes=10_000)
md = pq.ParquetFile(P('bytes.parquet')).metadata
ok('row_group_bytes splits', md.num_row_groups > 1 and all(md.row_group(i).num_rows <= 10 for i in range(md.num_row_groups)), [md.row_group(i).num_rows for i in range(md.num_row_groups)])
ok('row_group_bytes values', rd(P('bytes.parquet'))['i'] == list(range(50)))
mpq.write(P('huge.parquet'), {'s': ['y' * 100_000, 'z']}, row_group_bytes=10)
ok('row larger than row_group_bytes', rd(P('huge.parquet'))['s'] == ['y' * 100_000, 'z'] and pq.ParquetFile(P('huge.parquet')).metadata.num_row_groups == 2)
raises('row_group_bytes 0', ValueError, lambda: mpq.ParquetWriter(P('x.parquet'), row_group_bytes=0))

# array.array inputs: typecodes, ranges, declared types
for tc in 'bBhHiIlLqQfd':
    a = array(tc, [0, 1, 2, 100] if tc not in 'fd' else [0.5, -1.0, float('inf'), float('nan')])
    mpq.write(P('arr.parquet'), {'a': a})
    got = rd(P('arr.parquet'))['a']
    ok('array %s round trip' % tc, repr(got) == repr(a.tolist()), (got, a.tolist()))
mpq.write(P('arrq.parquet'), {'a': array('Q', [0, 2**64 - 1])})
ok('array Q -> uint64', rd(P('arrq.parquet'))['a'] == [0, 2**64 - 1] and str(pq.read_schema(P('arrq.parquet')).field('a').type) == 'uint64')
raises('array Q into int64', ValueError, lambda: mpq.write(P('bad.parquet'), {'a': array('Q', [2**63])}, {'a': 'int64'}))
raises('array q into uint64', ValueError, lambda: mpq.write(P('bad.parquet'), {'a': array('q', [-1])}, {'a': 'uint64'}))
raises('array i into int8', ValueError, lambda: mpq.write(P('bad.parquet'), {'a': array('i', [300])}, {'a': 'int8'}))
raises('array B into int8', ValueError, lambda: mpq.write(P('bad.parquet'), {'a': array('B', [200])}, {'a': 'int8'}))
raises('array d into float32 overflow', OverflowError, lambda: mpq.write(P('bad.parquet'), {'a': array('d', [1e300])}, {'a': 'float32'}))
raises('array into string column', TypeError, lambda: mpq.write(P('bad.parquet'), {'a': array('q', [1])}, {'a': 'string'}))
import warnings
with warnings.catch_warnings():
    warnings.simplefilter('ignore', DeprecationWarning)
    raises('array u typecode', TypeError, lambda: mpq.write(P('bad.parquet'), {'a': array('u', 'ab')}))
mpq.write(P('arrf.parquet'), {'a': array('f', [1.5, -0.0])}, {'a': 'float64'})
ok('array f into float64', repr(rd(P('arrf.parquet'))['a']) == repr([1.5, -0.0]))

# DictionaryColumn inputs, including what read(dictionaries=True) returns
DC = mpq.DictionaryColumn
mpq.write(P('dc.parquet'), {'s': DC(array('i', [1, -1, 0, 1]), ['a', 'b'])})
ok('DictionaryColumn round trip', rd(P('dc.parquet'))['s'] == ['b', None, 'a', 'b'])
raises('DictionaryColumn index too big', ValueError, lambda: mpq.write(P('bad.parquet'), {'s': DC(array('i', [2]), ['a', 'b'])}))
raises('DictionaryColumn index < -1', ValueError, lambda: mpq.write(P('bad.parquet'), {'s': DC(array('i', [-2]), ['a'])}))
raises('DictionaryColumn wrong value type', TypeError, lambda: mpq.write(P('bad.parquet'), {'s': DC(array('i', [0]), ['a', 5])}, {'s': 'string'}))
raises('DictionaryColumn length mismatch', ValueError, lambda: mpq.write(P('bad.parquet'), {'s': DC(array('i', [0, 0]), ['a']), 'i': [1]}))
mpq.write(P('dcn.parquet'), {'s': DC([0, 1, 0], ['a', None])})
ok('DictionaryColumn with None entry', rd(P('dcn.parquet'))['s'] == ['a', None, 'a'])
mpq.write(P('dce.parquet'), {'s': DC(array('i'), [])})
ok('empty DictionaryColumn', rd(P('dce.parquet'))['s'] == [])
src = mpq.ParquetFile(P('stream.parquet')).read(dictionaries=True, arrays=True)
mpq.write(P('reread.parquet'), src)
ok('read(dictionaries=True, arrays=True) writes back', rd(P('reread.parquet')) == rd(P('stream.parquet')))

# empty first batches do not fix the types; all-null first batches say what to do
with mpq.ParquetWriter(P('emptyfirst.parquet')) as w:
    w.write({'a': [], 's': []}); w.write({'a': [1], 's': ['x']})
ok('empty first batch defers types', rd(P('emptyfirst.parquet')) == {'a': [1], 's': ['x']} and str(pq.read_schema(P('emptyfirst.parquet')).field('a').type) == 'int64')
with mpq.ParquetWriter(P('onlyempty.parquet')) as w: w.write({'a': []})
ok('only empty batches: column kept', mpq.ParquetFile(P('onlyempty.parquet')).column_names == ['a'])
w = mpq.ParquetWriter(io.BytesIO()); w.write({'a': []})
msg = raises('empty batch then other columns', ValueError, lambda: w.write({'b': [1]})); w.abort()
w = mpq.ParquetWriter(io.BytesIO()); w.write({'a': [None]})
msg = raises('all-null first batch, then values', TypeError, lambda: w.write({'a': [1]})); w.abort()
ok('all-null message points to types=', msg is not None and 'types=' in msg, msg)

# numpy/pandas-like objects with tolist(), tuples, and bad type specs
class FakeNumpy:
    def __init__(self, v): self.v = v
    def __len__(self): return len(self.v)
    def tolist(self): return list(self.v)
mpq.write(P('np.parquet'), {'a': FakeNumpy([1, 2]), 't': (3, 4)})
ok('tolist() and tuple columns', rd(P('np.parquet')) == {'a': [1, 2], 't': [3, 4]})
raises('type spec object', TypeError, lambda: mpq.write(P('bad.parquet'), {'a': [1]}, {'a': int}))
raises('type spec tuple', TypeError, lambda: mpq.write(P('bad.parquet'), {'a': [1]}, {'a': ('list', 'int64')}))

# struct type names
mpq.write(P('stt.parquet'), {'m': [None, {'Role': 'u', 'Tags': ['a', None], 'p': None}, {}]},
          {'m': 'struct<Role: string, Tags: list<string>, p: decimal(10, 2)>'})
ok('struct type name', rd(P('stt.parquet'))['m'] == [None, {'Role': 'u', 'Tags': ['a', None], 'p': None}, {'Role': None, 'Tags': None, 'p': None}])
ok('struct type name schema', str(pq.read_schema(P('stt.parquet')).field('m').type) == 'struct<Role: string, Tags: list<element: string>, p: decimal128(10, 2)>')
for spec in ('struct<>', 'struct<a>', 'struct<a: int64, a: string>', 'struct<a: list<int64>', 'struct<: int64>', 'list<>', 'struct<a: nope>'):
    raises('bad type name %r' % spec, (ValueError, TypeError), lambda: mpq.write(P('bad.parquet'), {'m': [None]}, {'m': spec}))
raises('struct type, non-dict value', TypeError, lambda: mpq.write(P('bad.parquet'), {'m': [5]}, {'m': 'struct<a: int64>'}))
raises('struct field dotted collision', ValueError, lambda: mpq.write(P('bad.parquet'), {'m.a': [1], 'm': [None]}, {'m': 'struct<a: int64>'}))

# binary sort keys given as memoryview/bytearray (memoryview does not sort)
with mpq.ParquetWriter(P('mv.parquet'), sort_by='b', row_group_size=4) as w:
    for i in range(5): w.write({'b': [memoryview(b'z%d' % i), bytearray(b'a%d' % i), None], 'i': [i, i, i]})
got = rd(P('mv.parquet'))
ok('memoryview sort keys', sorted(map(repr, zip(got['b'], got['i']))) == sorted(map(repr, [(x, i) for i in range(5) for x in (b'z%d' % i, b'a%d' % i, None)])))

# -- file objects as destinations
buf = io.BytesIO(); mpq.write(buf, {'a': [1, 2]})
ok('BytesIO', list(mpq.read(buf.getvalue())['a']) == [1, 2])
raises('dest is a directory', (OSError, ValueError), lambda: mpq.write(T, {'a': [1]}))
raises('dest dir missing', OSError, lambda: mpq.write(os.path.join(T, 'nope', 'x.parquet'), {'a': [1]}))
print(f'{fails} failures')
sys.exit(1 if fails else 0)
