# Usage: test_stdlib.py [module_dir]
# Standard-library-only suite: files written by microparquet's writer are read
# back by its reader (full reads, filters against a plain-Python reference, the
# output modes, batches, every source type including HTTP range requests), and
# the writer's input checking and atomicity. Runs on Python 3.9+ with nothing
# installed; set MICROPARQUET_PURE=1 to ignore optional accelerators.
import sys, os, io, math, random, decimal, datetime as dt, uuid, tempfile, threading, re, glob
sys.path.insert(0, sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import microparquet as mpq
import http.server

R = random.Random(7)
T = tempfile.mkdtemp(prefix="mpq-stdlib-")
P = lambda n: os.path.join(T, n)
fails = 0

def check(label, ok, detail=""):
    global fails
    print(("PASS " if ok else "FAIL ") + label + ("" if ok else "  " + str(detail)[:300]), flush=True)
    if not ok: fails += 1

def norm(v):
    if isinstance(v, float):
        return "NaN" if math.isnan(v) else ("zero", math.copysign(1, v)) if v == 0 else v
    if isinstance(v, (list, tuple)): return [norm(x) for x in v]
    if isinstance(v, dict): return {k: norm(x) for k, x in v.items()}
    if isinstance(v, decimal.Decimal): return ("dec", v.normalize() if v else 0)
    return v

def same(a, b): return norm(list(a)) == norm(list(b))

UTC = dt.timezone.utc
STR = ["", "a", "é", "中文", "z" * 80, "\x00", "emoji 🎉", "A"]

def table(n):
    nul = lambda p, f: [None if R.random() < p else f() for _ in range(n)]
    p = R.choice([0, .2, .9])
    return {
        "i": nul(p, lambda: R.choice([0, -1, 2**63 - 1, -2**63, R.randint(-500, 500)])),
        "f": nul(p, lambda: R.choice([0.0, -0.0, 1.5, math.inf, -math.inf, math.nan, R.uniform(-9, 9)])),
        "s": nul(p, lambda: R.choice(STR) + str(R.randint(0, 20))),
        "b": nul(p, lambda: R.random() < .5),
        "bin": nul(p, lambda: bytes(R.randrange(256) for _ in range(R.randint(0, 9)))),
        "d": nul(p, lambda: dt.date(1, 1, 1) + dt.timedelta(days=R.randint(0, 3652058))),
        "ts": nul(p, lambda: dt.datetime(1950, 1, 1) + dt.timedelta(microseconds=R.randint(0, 2**50))),
        "tz": nul(p, lambda: dt.datetime(1970, 1, 1, tzinfo=UTC) + dt.timedelta(seconds=R.randint(-2**31, 2**31))),
        "tm": nul(p, lambda: dt.time(R.randint(0, 23), R.randint(0, 59), R.randint(0, 59), R.randint(0, 999999))),
        "dec": nul(p, lambda: decimal.Decimal(R.randint(-10**10, 10**10)).scaleb(-3)),
        "u": nul(p, lambda: uuid.UUID(int=R.getrandbits(128))),
        "lst": nul(p, lambda: [R.choice([None, R.randint(0, 9)]) for _ in range(R.randint(0, 4))]),
        "st": nul(p, lambda: {"a": R.choice([None, R.randint(0, 9)]), "b": R.choice([None, "x", "yy"])}),
        "ls": nul(p, lambda: [None if R.random() < .1 else {"k": R.choice(STR), "v": [R.random()]} for _ in range(R.randint(0, 3))]),
    }

ZSTD = mpq._zstd is not None

# -- round trips under many settings
for k in range(40):
    n = R.choice([0, 1, 9, 300, 2500])
    data = table(n)
    kw = dict(compression=R.choice(["none", "gzip"] + (["zstd"] if ZSTD else [])),
              page_size=R.choice([64, 4096, 65536]), row_group_size=R.choice([1, 100, 65536]),
              dictionary=R.choice(["auto", True, False]))
    if R.random() < .4: kw["sort_by"] = R.sample(["i", "s", "d", "dec"], R.randint(1, 2))
    mpq.write(P("rt.parquet"), data, **kw)
    expect = data
    if "sort_by" in kw:
        order = list(range(n))
        for c in reversed(kw["sort_by"]):
            col = data[c]
            order = sorted([i for i in order if col[i] is not None], key=col.__getitem__) + [i for i in order if col[i] is None]
        expect = {c: [v[i] for i in order] for c, v in data.items()}
    got = mpq.read(P("rt.parquet"))
    bad = [c for c in data if not same(got[c], expect[c])]
    check("round trip %d n=%d %s" % (k, n, kw), not bad, bad)

# -- filters against a plain-Python reference (pyarrow semantics for nulls)
OPS = {"=": lambda a, b: a == b, "!=": lambda a, b: a != b, "<": lambda a, b: a < b, "<=": lambda a, b: a <= b,
       ">": lambda a, b: a > b, ">=": lambda a, b: a >= b}
def ref_filter(data, flt):
    n = len(next(iter(data.values())))
    def ok(i, c, op, x):
        v = data[c][i]
        if op == "in": return v in x
        if op == "not in": return v not in x
        return v is not None and OPS[op](v, x)
    keep = [i for i in range(n) if all(ok(i, *t) for t in flt)]
    return {c: [v[i] for i in keep] for c, v in data.items()}

for k in range(25):
    n = 3000
    data = {"id": list(range(n)), "g": [R.randint(0, 50) for _ in range(n)],
            "s": [None if R.random() < .1 else R.choice(STR) for _ in range(n)],
            "d": [dt.date(2020, 1, 1) + dt.timedelta(days=R.randint(0, 900)) for _ in range(n)]}
    mpq.write(P("flt.parquet"), data, sort_by=R.choice(["id", "g", "s", "d"]), page_size=R.choice([256, 4096]),
              row_group_size=R.choice([500, 65536]))
    data = mpq.read(P("flt.parquet"))
    data = {c: list(v) for c, v in data.items()}
    for _ in range(8):
        c = R.choice(["id", "g", "s", "d"]); op = R.choice(list(OPS) + ["in", "not in"])
        vals = [v for v in data[c] if v is not None]
        x = {R.choice(vals) for _ in range(3)} if op in ("in", "not in") else R.choice(vals)
        flt = [(c, op, x)]
        if R.random() < .3: flt.append(("g", ">=", R.randint(0, 50)))
        got = mpq.read(P("flt.parquet"), filters=flt)
        ref = ref_filter(data, flt)
        check("filter %r" % (flt,), all(same(got[c], ref[c]) for c in ref), {c: len(got[c]) for c in got})

# -- output modes, batches, limits, sources
data = table(5000); data["i"] = [R.randint(0, 9) for _ in range(5000)]
mpq.write(P("modes.parquet"), data, row_group_size=1500, page_size=2048)
pf = mpq.ParquetFile(P("modes.parquet"))
full = pf.read()
check("num_rows", pf.num_rows == 5000)
check("column_names", pf.column_names == list(data))
check("arrays mode", list(pf.read(["i"], arrays=True)["i"]) == data["i"])
dc = pf.read(["s"], dictionaries=True)["s"]
check("dictionaries mode", list(dc) == full["s"] if isinstance(dc, mpq.DictionaryColumn) else same(dc, full["s"]))
acc = {c: [] for c in data}
for b in pf.iter_batches(batch_size=777):
    for c in data: acc[c] += list(b[c])
check("iter_batches", all(same(acc[c], full[c]) for c in data))
check("head", all(same(v, full[c][:7]) for c, v in pf.head(7).items()))
check("limit + filter", same(pf.read(["i"], [("i", "=", 3)], limit=5)["i"], [3] * 5))
check("rows", list(pf.rows(["i", "s"], limit=4)) == list(zip(full["i"][:4], full["s"][:4])))
check("statistics", len(pf.statistics("i")) == pf.num_row_groups)
raw = open(P("modes.parquet"), "rb").read()
check("bytes source", same(mpq.read(raw)["s"], full["s"]))
check("file object source", same(mpq.read(io.BytesIO(raw))["s"], full["s"]))

class Ranges(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        n = len(raw); m = re.match(r"bytes=(\d*)-(\d*)", self.headers.get("Range", ""))
        a, b = m.groups()
        a, b = (max(0, n - int(b)), n - 1) if a == "" else (int(a), min(int(b) if b else n - 1, n - 1))
        self.send_response(206); self.send_header("Content-Range", "bytes %d-%d/%d" % (a, b, n))
        self.send_header("ETag", '"v1"'); self.send_header("Content-Length", str(b - a + 1)); self.end_headers()
        self.wfile.write(raw[a:b + 1])
srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Ranges)
threading.Thread(target=srv.serve_forever, daemon=True).start()
url = "http://127.0.0.1:%d/modes.parquet" % srv.server_address[1]
rf = mpq.open_url(url)
check("open_url full read", same(rf.read(["s"])["s"], full["s"]))
check("open_url filtered", same(rf.read(["i"], [("i", "=", 3)])["i"], [3] * full["i"].count(3)))
try: mpq.read(url); check("URL string is not fetched", False)
except FileNotFoundError: check("URL string is not fetched", True)
srv.shutdown()

# -- writer input checking and atomicity
def rejects(label, data, types=None):
    try: mpq.write(P("bad.parquet"), data, types)
    except (TypeError, ValueError, OverflowError, KeyError): check(label, not os.path.exists(P("bad.parquet")) and not glob.glob(P(".*.tmp")))
    else: check(label, False, "accepted"); os.remove(P("bad.parquet"))
rejects("mixed types", {"x": [1, "a"]})
rejects("int in string column", {"x": ["a", 5]}, {"x": "string"})
rejects("str in int column", {"x": ["1"]}, {"x": "int64"})
rejects("int32 overflow", {"x": [2**31]}, {"x": "int32"})
rejects("decimal scale", {"x": [decimal.Decimal("1.234")]}, {"x": "decimal(9,2)"})
rejects("naive into UTC", {"x": [dt.datetime(2020, 1, 1)]}, {"x": "timestamp[us,UTC]"})
rejects("ragged", {"x": [1, 2], "y": [1]})
mpq.write(P("keep.parquet"), {"x": [1, 2]})
try: mpq.write(P("keep.parquet"), {"x": [1, "a"]})
except TypeError: pass
check("failed overwrite keeps old file", list(mpq.read(P("keep.parquet"))["x"]) == [1, 2])
w = mpq.ParquetWriter(P("stream.parquet")); w.write({"a": [1], "s": ["x"]})
try: w.write({"a": [2], "s": [3]})
except TypeError: pass
w.write({"a": [4], "s": ["y"]}); w.close()
got = mpq.read(P("stream.parquet"))
check("rejected batch is undone", list(got["a"]) == [1, 4] and list(got["s"]) == ["x", "y"])
try:
    with mpq.ParquetWriter(P("ctx.parquet")) as w2:
        w2.write({"a": [1]}); raise RuntimeError
except RuntimeError: pass
check("exception in with: no file", not os.path.exists(P("ctx.parquet")) and not glob.glob(P(".*.tmp")))

print("%d failures" % fails)
sys.exit(1 if fails else 0)
