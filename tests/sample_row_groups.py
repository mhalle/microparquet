# Usage: sample_row_groups.py module_dir file [n_sample] -- compare n evenly spaced row groups with pyarrow
import sys, time, math
sys.path.insert(0, sys.argv[1]); import microparquet as mpq, pyarrow.parquet as pq
f = sys.argv[2]; n = int(sys.argv[3]) if len(sys.argv) > 3 else 10
def norm(v):
    if isinstance(v, float) and math.isnan(v): return 'NaN'
    if isinstance(v, (list, tuple)): return [norm(x) for x in v]
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    return v
pa_f, pf = pq.ParquetFile(f), mpq.ParquetFile(f)
k = pa_f.num_row_groups; picks = sorted({int(i * (k - 1) / max(1, n - 1)) for i in range(n)})
ta = tt = 0; bad = set()
for i in picks:
    t0 = time.perf_counter(); ref = pa_f.read_row_group(i).to_pydict(); ta += time.perf_counter() - t0
    t0 = time.perf_counter(); mine = next(pf.iter_row_groups(row_groups=[i])); tt += time.perf_counter() - t0
    bad |= {c for c in ref if norm(list(mine[c])) != norm(ref[c])}
print(f'{f.rsplit("/",1)[1]}: {len(picks)} row groups, micro {tt:.2f}s, pyarrow {ta:.2f}s,', 'MATCH' if not bad else f'MISMATCH {sorted(bad)}')
