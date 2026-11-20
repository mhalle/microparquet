# Usage: fuzz_writer.py module_dir n_runs [seed] [progress]
# Fault-injection fuzzing of the streaming writer. Each run writes random batches
# (lists, array.array and DictionaryColumn inputs, nested columns) under random
# settings; some batches carry a bad value and some writes hit a simulated I/O
# error or KeyboardInterrupt part way. The file must hold exactly the accepted
# batches, in order (sorted per flushed buffer with sort_by), and read the same
# in microparquet and pyarrow.
import sys, io, math, random, traceback
from array import array
sys.path.insert(0, sys.argv[1]); import microparquet as mpq
import pyarrow as pa, pyarrow.parquet as pq   # pa.BufferReader: pyarrow can hang at exit after reading a BytesIO
N = int(sys.argv[2]); seed = int(sys.argv[3]) if len(sys.argv) > 3 else 0
R = random.Random(seed)
PROGRESS = len(sys.argv) > 4

class Faulty(io.BytesIO):
    def __init__(self): super().__init__(); self.calls = 0; self.fail_at = None; self.exc = OSError
    def write(self, b):
        self.calls += 1
        if self.fail_at is not None and self.calls >= self.fail_at:
            self.fail_at = None; raise self.exc('simulated failure')
        return super().write(b)

def norm(v):
    if isinstance(v, float): return 'NaN' if math.isnan(v) else ('z', math.copysign(1, v)) if v == 0 else v
    if isinstance(v, list): return [norm(x) for x in v]
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    return v

TYPES = {'i': 'int64', 'u': 'uint32', 'f': 'float64', 's': 'string', 'l': 'list<int32>', 'st': 'struct<a: string, b: list<string>>'}
def gen(n):
    nul = lambda p, f: [None if R.random() < p else f() for _ in range(n)]
    p = R.choice([0, 0, .3, 1.0])
    return {'i': nul(p, lambda: R.randint(-2**63, 2**63 - 1) if R.random() < .1 else R.randint(0, 30)),
            'u': nul(p, lambda: R.choice([0, 2**32 - 1, R.randint(0, 99)])),
            'f': nul(p, lambda: R.choice([0.0, -0.0, math.nan, math.inf, R.random()])),
            's': nul(p, lambda: R.choice(['', 'a', 'é', 'x' * 70, 'k%d' % R.randint(0, 9)])),
            'l': nul(p, lambda: [R.choice([None, R.randint(-5, 5)]) for _ in range(R.randint(0, 3))]),
            'st': nul(p, lambda: {'a': R.choice([None, 'p', 'q']), 'b': R.choice([None, [], ['x', None]])})}

BAD = {'i': ['1', 1.5, 2**64, True], 'u': [-1, 2**32, 'x'], 'f': ['1.0', b'x', True], 's': [5, b'x', 1.5, '\ud800'],
       'l': [7, 'abc', [1.5], [2**40]], 'st': [5, ['a'], {'a': 5}, {'b': 'notalist'}]}

def form(c, v):
    if c in ('i', 'u', 'f') and None not in v and R.random() < .3:
        try: return array({'i': 'q', 'u': R.choice('IL'), 'f': 'd'}[c], v)
        except (OverflowError, TypeError): return v
    if c in ('s', 'i') and R.random() < .3:
        d, ix = [], {}
        for x in v:
            if x is not None and x not in ix: ix[x] = len(d); d.append(x)
        return mpq.DictionaryColumn(array('i', [-1 if x is None else ix[x] for x in v]), d)
    return v

fails = runs = 0
import time as _time
_T0 = _time.time()
for k in range(N):
    kw = dict(row_group_size=R.choice([1, 7, 64, 1000]), page_size=R.choice([32, 1024, 65536]),
              dictionary=R.choice(['auto', True, False]), compression=R.choice(['none', 'gzip', 'zstd']))
    if R.random() < .3: kw['sort_by'] = R.choice(['i', 's', 'u'])
    elif R.random() < .3: kw['row_group_bytes'] = R.choice([1, 300, 5000])
    dest = Faulty(); w = mpq.ParquetWriter(dest, TYPES, **kw)
    if PROGRESS: print('run', k, kw, '%.1fs' % (_time.time() - _T0), flush=True)
    accepted, events = [], []
    try:
        for b in range(R.randint(0, 12)):
            n = R.choice([0, 1, 3, 20, 150])
            data = gen(n); bad = None
            if n and R.random() < .25:
                c = R.choice(list(BAD)); r = R.randrange(n); data[c][r] = R.choice(BAD[c]); bad = (c, r, data[c][r])
            if R.random() < .15:
                dest.fail_at = dest.calls + R.randint(1, 12); dest.exc = R.choice([OSError, KeyboardInterrupt])
            # the column holding an injected bad value stays a list: array() and a dictionary would coerce it
            batch = {c: v if bad and bad[0] == c else form(c, v) for c, v in data.items()}
            try:
                w.write(batch)
                if bad is not None: events.append(('accepted bad', bad))
                accepted.append(data)
            except (TypeError, ValueError, OverflowError) as e:
                if bad is None: events.append(('rejected good batch', repr(e)))
            except (OSError, KeyboardInterrupt):
                pass                                  # undone; the batch counts as not written
            dest.fail_at = None
        w.close()
    except Exception:
        fails += 1; print('FAIL run %d %s: %s' % (k, kw, traceback.format_exc()[-800:]), flush=True); continue
    runs += 1
    for what, detail in events:
        fails += 1; print('FAIL run %d %s: %s %r' % (k, kw, what, detail), flush=True)
    cols = list(TYPES)
    ref = {c: [x for d in accepted for x in d[c]] for c in cols}
    if 'sort_by' in kw:                               # stable sort per flushed buffer, nulls last
        key = ref[kw['sort_by']]; rgs = kw['row_group_size']
        srt = lambda ix: sorted(ix, key=lambda i: (key[i] is None, key[i] if key[i] is not None else 0))
        order, buf, pos = [], [], 0
        for d in accepted:
            m = len(d['i']); buf += range(pos, pos + m); pos += m
            if len(buf) >= rgs:
                buf = srt(buf); cut = len(buf) // rgs * rgs; order += buf[:cut]; buf = buf[cut:]
        order += srt(buf)
        ref = {c: [v[i] for i in order] for c, v in ref.items()}
    raw = dest.getvalue()
    try:
        m = mpq.read(raw)
        bad = [c for c in cols if norm(list(m[c])) != norm(ref[c])]
        if bad: fails += 1; print('FAIL run %d %s: microparquet differs in %s' % (k, kw, bad), flush=True)
        t = pq.read_table(pa.BufferReader(raw), use_threads=False).to_pydict()
        bad = [c for c in cols if norm(t[c]) != norm(ref[c])]
        if bad: fails += 1; print('FAIL run %d %s: pyarrow differs in %s' % (k, kw, bad), flush=True)
    except Exception:
        fails += 1; print('FAIL run %d %s read: %s' % (k, kw, traceback.format_exc()[-800:]), flush=True)
print('%d runs, %d failures' % (runs, fails))
sys.exit(1 if fails else 0)
