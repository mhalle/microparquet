# Usage: bench_real.py micro|pyarrow file [module_dir] -- full read to Python objects, prints seconds and peak RSS
import sys, time, resource
lib, f = sys.argv[1], sys.argv[2]
if len(sys.argv) > 3: sys.path.insert(0, sys.argv[3])
t = time.perf_counter()
if lib == 'micro':
    import microparquet as mpq; mpq.read(f)
else:
    import pyarrow.parquet as pq; pq.read_table(f).to_pydict()
print(f'{time.perf_counter() - t:.2f} {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.0f}')
