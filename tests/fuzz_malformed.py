# Usage: fuzz_malformed.py module_dir n_cases seed file...
# Mutates valid Parquet files and reads them. Any exception is fine; what matters is
# hangs (> 3 s), memory blowups (3 GB cap) and crashes of the interpreter.
import sys, os, random, signal, resource, collections
sys.path.insert(0, sys.argv[1]); import microparquet as mpq
n, seed, files = int(sys.argv[2]), int(sys.argv[3]), sys.argv[4:]
resource.setrlimit(resource.RLIMIT_AS, (3 << 30, 3 << 30))
sys.setrecursionlimit(5000)
R = random.Random(seed)
class Hang(Exception): pass
def alarm(*a): raise Hang()
signal.signal(signal.SIGALRM, alarm)
seeds = [open(f, 'rb').read() for f in files]
outcomes, examples = collections.Counter(), {}

def mutate(b):
    b = bytearray(b); kind = R.choice(['flip', 'flip', 'bytes', 'trunc', 'footer', 'zero'])
    if kind == 'flip':
        for _ in range(R.randint(1, 8)): i = R.randrange(len(b)); b[i] ^= 1 << R.randrange(8)
    elif kind == 'bytes':
        i = R.randrange(len(b)); k = R.randint(1, 16); b[i:i + k] = bytes(R.choice([0, 0xFF, R.randrange(256)]) for _ in range(k))
    elif kind == 'trunc':
        cut = R.randrange(4, len(b)); b = b[:cut] + b[-8:]   # keep a footer length + magic
    elif kind == 'footer':                                   # mutate inside the footer
        mlen = int.from_bytes(b[-8:-4], 'little'); lo = max(0, len(b) - 8 - mlen)
        for _ in range(R.randint(1, 6)): i = R.randrange(lo, len(b) - 8); b[i] = R.randrange(256)
    else:
        i = R.randrange(len(b)); b[i:i + 64] = bytes(64)
    return bytes(b), kind

for k in range(n):
    src = R.randrange(len(seeds)); data, kind = mutate(seeds[src])
    signal.alarm(3)
    try:
        pf = mpq.ParquetFile(data); r = pf.read()
        for c in list(r)[:2]: len(r[c])
        res = 'ok'
    except Hang: res = 'HANG'
    except MemoryError: res = 'MEMORY'
    except RecursionError: res = 'RECURSION'
    except Exception as e: res = 'exc:' + type(e).__name__
    finally: signal.alarm(0)
    outcomes[res] += 1
    if res in ('HANG', 'MEMORY', 'RECURSION') or res.startswith('exc:') and res not in examples:
        key = res if res in ('HANG', 'MEMORY', 'RECURSION') else res
        if key not in examples:
            examples[key] = (files[src], kind, k)
            if res in ('HANG', 'MEMORY'): open(os.path.join(__import__('tempfile').gettempdir(), f'malformed_{res}_{k}.parquet'), 'wb').write(data)
print(dict(outcomes.most_common()))
for k, v in examples.items(): print(f'  {k:28s} first from {os.path.basename(v[0])} ({v[1]}) case {v[2]}')
ALLOWED = {'ok', 'exc:ParquetError', 'exc:NotImplementedError', 'exc:ImportError'}
sys.exit(1 if set(outcomes) - ALLOWED else 0)
