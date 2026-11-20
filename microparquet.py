"""microparquet: a small, dependency-free Parquet reader and writer.

"micro" as in dependencies (standard library only), not MicroPython: it needs
CPython (or Pyodide) modules such as array, struct and itertools.

Pure Python in one file. Standard library only on Python 3.14+ (zstd comes from
`compression.zstd`). It runs on Python 3.9+; there, zstd files need the
`zstandard` or `cramjam` package. Optional accelerators, used when installed and
never required: `cramjam` (C-speed snappy, LZ4, Brotli, zstd; one small wheel),
or `python-snappy`, `brotli`, `lz4` individually. Set MICROPARQUET_PURE=1 to
ignore them.

    import microparquet as mpq
    mpq.read("code.parquet")                         # {column: list of values}

    pf = mpq.ParquetFile("device.parquet")           # local path, bytes or binary file
    pf.schema, pf.column_names, pf.num_rows, pf.metadata, pf.statistics("model")
    pf.head(5)                                      # reads only the pages it needs
    pf.read(["model", "codeset_id"], filters=[("manufacturer", "=", "Sony")])
    pf.read(filters=[[("id", "<", 10)], [("id", "in", {99, 100})]])   # OR of ANDs
    pf.read(limit=1000)                             # first 1000 (matching) rows
    for batch in pf.iter_batches(batch_size=50_000): ...   # bounded memory
    for row in pf.rows(["id", "model"]): ...       # tuples
    pf = mpq.open_url("https://example.org/data.parquet")  # remote: HTTP range requests
    pf = mpq.ParquetFile(mpq.HTTPSource(url, headers={"Authorization": "Bearer ..."}))

Values match pyarrow's Table.to_pydict(): structs are dicts, lists are lists,
maps are lists of (key, value) tuples, at any nesting depth. Column names are
top-level fields; a dotted path ("bbox.xmin", "answers.text") selects any field
inside. Nanosecond times are truncated to microseconds.

Filters use pyarrow's form: a list of (column, op, value) tuples that are ANDed,
or a list of such lists that are ORed. Ops: = == != < <= > >= in, not in.
Columns may be struct fields by dotted path; list elements cannot be filtered.
Null and NaN semantics follow pyarrow: nulls satisfy only `not in` (or `in` a set
holding None); NaN equals NaN and -0.0 differs from 0.0 inside sets. Row groups are skipped with column statistics; pages with
the page index (ColumnIndex/OffsetIndex) or page-header statistics; whole
chunks when no dictionary entry can match. Surviving rows are filtered exactly,
one filter column at a time, and only the pages holding matching rows are read
for the other columns. Remote files fetch the footer, then only those pages,
coalescing nearby ranges and fetching in parallel; without a page index,
partial reads fetch column chunks a piece at a time.

arrays=True returns numeric columns as array.array (raw physical values: dates
and timestamps stay integers) when the result has no nulls. dictionaries=True
returns dictionary-encoded columns as DictionaryColumn(indices, dictionary),
with -1 marking nulls, without building a value per row.

Corrupt or truncated input raises ParquetError (a ValueError) naming the column
and row group; counts and sizes read from the file are bounded, so bad input
fails instead of hanging or exhausting memory. Page checksums are not verified.
Paths (str or os.PathLike) are always local files, even if they look like URLs;
remote files must be asked for with open_url() or HTTPSource, which accept only
http(s) URLs, follow only http(s) redirects, and drop credentials when a redirect
changes host. Local files are read with os.pread (not memory-mapped), so a file
changed by another process while open gives an error or new bytes, not a crash.
Responses from servers are checked (status, Content-Range, length, size and ETag
must stay the same) and bounded in size and time; see HTTPSource.

Encodings: PLAIN, dictionary, RLE, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY,
DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT; data pages v1 and v2. Codecs: UNCOMPRESSED,
SNAPPY, GZIP, ZSTD, LZ4_RAW, LZ4 (Hadoop and raw), BROTLI (needs cramjam or
brotli). Types: STRING, ENUM, JSON, DECIMAL, DATE, TIME, TIMESTAMP, INT96,
unsigned INT, UUID, FLOAT16. Not supported: LZO, the ALP encoding, encryption,
bloom filters (ignored), filters on list columns.

Writing covers the common cases and produces files that read well here and
elsewhere (checked against pyarrow, polars, DuckDB and fastparquet):

    mpq.write("out.parquet", {"id": [1, 2], "tags": [["a"], []]}, sort_by="id")
    mpq.write("out.parquet", rows)                  # or a list of row dicts
    with mpq.ParquetWriter("big.parquet", types={"price": "decimal(10,2)"}) as w:
        for batch in batches: w.write(batch)        # buffered into full row groups
    mpq.write("copy.parquet", pf.read(dictionaries=True))

Types are inferred from Python values (bool, int, float, str, bytes, date,
datetime naive or aware, time, Decimal, UUID, lists, dicts as structs) or named
in types= ("struct<a: int64, b: list<string>>" and the like). Columns may be
lists, array.array or DictionaryColumn. Files get statistics, a page index,
dictionary encoding where it pays,
zstd compression (gzip where zstd is unavailable) and v1 data pages. Values are
checked strictly against the column type; a rejected batch leaves the file as it
was, and the file appears (atomically) only when the writer closes cleanly.
Not written: maps, INT96, delta or byte-stream-split encodings, bloom filters.
"""
import datetime as _dt, decimal as _dec, operator as _op, struct, sys, uuid, weakref, zlib
from array import array
from bisect import bisect_right
from itertools import accumulate, chain, compress, repeat

import os

def _optional(name):                            # optional C accelerators, never required
    if os.environ.get("MICROPARQUET_PURE"): return None
    try: return __import__(name)
    except ImportError: return None

_cramjam = _optional("cramjam")                 # snappy, lz4, brotli, zstd in one small wheel
_pysnappy, _brotli, _lz4 = _optional("snappy"), _optional("brotli"), _optional("lz4.block")
try:
    from compression.zstd import decompress as _zstd, ZstdDecompressor as _ZstdD
    def _zstd_limited(b, size):                 # stop at the declared size (decompression bombs)
        out = bytearray()
        while b:
            d = _ZstdD(); out += d.decompress(b, size + 1 - len(out))
            if len(out) > size or not d.eof: break
            b = d.unused_data
        return out
except ImportError:  # Python < 3.14
    _ZstdD = None
    _zs = _optional("zstandard")
    if _zs: _zstd = lambda b, n=None: _zs.ZstdDecompressor().decompress(b, max_output_size=(n or 0) + 1)
    elif _cramjam: _zstd = lambda b, n=None: _into(_cramjam.zstd.decompress_into, b, n)
    else: _zstd = None

__version__ = "0.1.0"
__all__ = ["ParquetFile", "read", "open_url", "Column", "DictionaryColumn", "HTTPSource", "ParquetError",
           "ParquetWriter", "write"]

class ParquetError(ValueError):
    """A file is corrupt, truncated, or uses something this reader cannot decode."""
_BIG = sys.byteorder == "big"

# ---- Thrift compact protocol: decodes any struct into {field id: value} ----

def _tstruct(b, p):
    """Thrift compact protocol struct at b[p] -> ({field id: value}, end). b is bytes."""
    out, fid = {}, 0
    while True:
        h = b[p]; p += 1
        if h == 0: return out, p
        t = h & 15
        if h >> 4: fid += h >> 4
        else:
            x = b[p]; p += 1; v = x & 0x7F; sh = 7
            while x & 0x80: x = b[p]; p += 1; v |= (x & 0x7F) << sh; sh += 7
            fid = (v >> 1) ^ -(v & 1)
        if t == 12: v, p = _tstruct(b, p)
        elif 4 <= t <= 6 or t == 8:             # zigzag ints, or binary with a varint length
            x = b[p]; p += 1; v = x & 0x7F; sh = 7
            while x & 0x80: x = b[p]; p += 1; v |= (x & 0x7F) << sh; sh += 7
            if t == 8: v, p = b[p:p + v], p + v
            else: v = (v >> 1) ^ -(v & 1)
        elif t <= 2: v = t == 1                 # bool in a struct field
        elif t in (9, 10): v, p = _tlist(b, p)
        else: v, p = _tother(b, p, t)
        out[fid] = v

def _tlist(b, p):
    h = b[p]; p += 1; n, et = h >> 4, h & 15
    if n == 15:
        x = b[p]; p += 1; n = x & 0x7F; sh = 7
        while x & 0x80: x = b[p]; p += 1; n |= (x & 0x7F) << sh; sh += 7
    if n > len(b) - p: raise ParquetError("corrupt metadata (list size)")
    out = []
    if et == 12:
        for _ in range(n): v, p = _tstruct(b, p); out.append(v)
    elif 4 <= et <= 6 or et == 8:
        for _ in range(n):
            x = b[p]; p += 1; v = x & 0x7F; sh = 7
            while x & 0x80: x = b[p]; p += 1; v |= (x & 0x7F) << sh; sh += 7
            if et == 8: v, p = b[p:p + v], p + v
            else: v = (v >> 1) ^ -(v & 1)
            out.append(v)
    elif et <= 2: out = [x == 1 for x in b[p:p + n]]; p += n
    else:
        for _ in range(n): v, p = _tother(b, p, et); out.append(v)
    return out, p

def _tother(b, p, t):                           # rarely used types
    if t == 3: v = b[p]; return (v - 256 if v > 127 else v), p + 1
    if t == 7: return struct.unpack_from("<d", b, p)[0], p + 8
    if t in (9, 10): return _tlist(b, p)
    if t == 11:
        x = b[p]; p += 1; n = x & 0x7F; sh = 7
        while x & 0x80: x = b[p]; p += 1; n |= (x & 0x7F) << sh; sh += 7
        if not n: return {}, p
        if n > len(b) - p: raise ParquetError("corrupt metadata (map size)")
        h = b[p]; p += 1; out = {}
        for _ in range(n):
            k, p = _tvalue(b, p, h >> 4); v, p = _tvalue(b, p, h & 15); out[k] = v
        return out, p
    if t == 12: return _tstruct(b, p)
    raise ValueError("thrift type %d" % t)

def _tvalue(b, p, t):
    if t <= 2: return b[p] == 1, p + 1          # bools inside containers take one byte
    if 4 <= t <= 6 or t == 8:
        x = b[p]; p += 1; v = x & 0x7F; sh = 7
        while x & 0x80: x = b[p]; p += 1; v |= (x & 0x7F) << sh; sh += 7
        return (b[p:p + v], p + v) if t == 8 else ((v >> 1) ^ -(v & 1), p)
    return _tother(b, p, t)

def _thrift(buf, pos=0):
    """Decode a Thrift struct from any buffer; returns (dict, end position)."""
    if type(buf) is not bytes:
        b = bytes(buf[pos:pos + 256])           # page headers are small: try a short copy first
        try:
            v, p = _tstruct(b, 0); return v, pos + p
        except (IndexError, ParquetError):      # longer than the prefix (size checks see the cut too)
            b = bytes(buf[pos:]); v, p = _tstruct(b, 0); return v, pos + p
    return _tstruct(buf, pos)

# ---- Codecs ----

def _copy(out, off, ln):                        # LZ77 back-reference, may overlap
    if not 0 < off <= len(out): raise ValueError("corrupt compressed data")
    start = len(out) - off
    if off >= ln: out += out[start:start + ln]
    else: out += (out[start:] * (ln // off + 1))[:ln]

def _snappy(src):
    n = s = 0; sh = 0
    while True:                                 # preamble: uncompressed length
        x = src[s]; s += 1; n |= (x & 0x7F) << sh; sh += 7
        if x < 0x80: break
    out, end = bytearray(), len(src)
    while s < end:
        tag = src[s]; t = tag & 3
        if t == 0:                              # literal
            ln = tag >> 2
            if ln >= 60:
                k = ln - 59; ln = int.from_bytes(src[s + 1:s + 1 + k], "little"); s += k
            s += 1; out += src[s:s + ln + 1]; s += ln + 1
        elif t == 1: _copy(out, (tag >> 5) << 8 | src[s + 1], ((tag >> 2) & 7) + 4); s += 2
        elif t == 2: _copy(out, src[s + 1] | src[s + 2] << 8, (tag >> 2) + 1); s += 3
        else: _copy(out, int.from_bytes(src[s + 1:s + 5], "little"), (tag >> 2) + 1); s += 5
    return out

def _lz4_block(src, out=None, limit=None):
    out = bytearray() if out is None else out
    s, end, limit = 0, len(src), (1 << 62) if limit is None else limit
    while s < end:
        if len(out) > limit: raise ParquetError("LZ4 data larger than its page claims")
        tok = src[s]; s += 1; ln = tok >> 4
        if ln == 15:
            while True:
                b = src[s]; s += 1; ln += b
                if b != 255: break
        out += src[s:s + ln]; s += ln
        if s >= end: break
        off = src[s] | src[s + 1] << 8; s += 2; ml = tok & 15
        if ml == 15:
            while True:
                b = src[s]; s += 1; ml += b
                if b != 255: break
        _copy(out, off, ml + 4)
    return out

def _lz4_hadoop(src, size):                     # codec LZ4: big-endian length-framed blocks
    out, s = bytearray(), 0
    try:
        while s < len(src):
            n, c = struct.unpack_from(">II", src, s); s += 8
            _lz4_block(src[s:s + c], out, size); s += c
        if len(out) == size: return out
    except (struct.error, IndexError, ValueError):
        pass
    return _lz4_block(src, None, size)          # some writers use raw blocks here

def _snappy_len(src):
    n = s = sh = 0
    while True:
        x = src[s]; s += 1; n |= (x & 0x7F) << sh; sh += 7
        if x < 0x80: return n

MAX_PAGE_SIZE = 1 << 30      # pages claiming more than this (uncompressed) are treated as corrupt

def _into(fn, data, size):                      # cramjam *_into: output can't exceed the buffer
    out = bytearray(size); return memoryview(out)[:fn(bytes(data), out)]

def _decompress(codec, data, size):
    if codec == 0 or not len(data): return data
    if not isinstance(size, int) or not 0 <= size <= MAX_PAGE_SIZE:
        raise ParquetError("page claims %r uncompressed bytes (limit MAX_PAGE_SIZE=%d)" % (size, MAX_PAGE_SIZE))
    if codec == 1 and _snappy_len(data) > size:
        raise ParquetError("snappy data larger than its page claims")
    if codec == 6:
        if _zstd is None: raise ImportError("zstd needs Python 3.14+, or the zstandard or cramjam package")
        out = _zstd_limited(bytes(data), size) if _ZstdD else _zstd(bytes(data), size)
    elif codec == 1:
        if _cramjam: out = _cramjam.snappy.decompress_raw(bytes(data))
        elif _pysnappy: out = _pysnappy.uncompress(bytes(data))
        else: out = _snappy(data)
    elif codec == 4:
        if _cramjam: out = _into(_cramjam.brotli.decompress_into, data, size)
        elif _brotli: out = _brotli_limited(bytes(data), size)
        else: raise ImportError("BROTLI needs the cramjam or brotli package")
    elif codec == 7 and (_cramjam or _lz4):
        out = (_cramjam.lz4.decompress_block(bytes(data), output_len=size) if _cramjam
               else _lz4.block.decompress(bytes(data), uncompressed_size=size))
    elif codec == 2:                            # gzip, possibly several members
        out, data, room = bytearray(), bytes(data), (1 << 62) if size is None else size + 1
        while data and room > 0:
            d = zlib.decompressobj(47); chunk = d.decompress(data, room)
            out += chunk; room -= len(chunk); data = d.unused_data
    elif codec == 7: out = _lz4_block(data, None, size)
    elif codec == 5: out = _lz4_hadoop(data, size)
    else: raise ValueError("compression codec %d not supported" % codec)
    if len(out) > size: raise ParquetError("page decompresses to more than its declared size")
    return memoryview(out)

def _brotli_limited(b, size):
    d = _brotli.Decompressor()
    try: out = d.process(b, output_buffer_limit=size + 1)   # brotli >= 1.1
    except TypeError:
        raise ImportError("bounded BROTLI decoding needs cramjam or brotli >= 1.1") from None
    return out

# ---- Encodings ----

def _uvarint(b, p):
    r = s = 0
    while True:
        x = b[p]; p += 1; r |= (x & 0x7F) << s; s += 7
        if x < 0x80: return r, p

_TABLES = {}
_U32 = struct.Struct("<I").unpack_from
_UINT = {8: "B", 16: "H", 32: "I", 64: "Q"}

def _bitunpack(buf, pos, w, n):
    """n unsigned w-bit values, packed LSB first from byte pos, as a list."""
    nb = (n * w + 7) // 8
    if w == 0: return [0] * n
    if w in (1, 2, 4):                          # per-byte lookup table
        t = _TABLES.get(w)
        if t is None:
            m = (1 << w) - 1
            t = _TABLES[w] = [tuple(b >> s & m for s in range(0, 8, w)) for b in range(256)]
        out = list(chain.from_iterable(map(t.__getitem__, buf[pos:pos + nb])))
    elif w in _UINT:                            # byte-aligned: C-speed array decode
        a = array(_UINT[w]); a.frombytes(buf[pos:pos + nb - nb % a.itemsize])
        if _BIG: a.byteswap()
        out = a.tolist()
    elif n <= 64:                               # short run (common in RLE hybrids): one int
        m, x = (1 << w) - 1, int.from_bytes(buf[pos:pos + nb], "little")
        return [x >> s & m for s in range(0, n * w, w)]
    else:                                       # 64 values per int keeps shifts cheap
        m, step, end, out = (1 << w) - 1, 8 * w, pos + nb, []
        shifts = range(0, 64 * w, w)
        for o in range(pos, end, step):
            x = int.from_bytes(buf[o:min(o + step, end)], "little")
            out += [x >> s & m for s in shifts]
    del out[n:]
    return out

def _rle(buf, pos, end, w, n):
    """RLE / bit-packed hybrid (levels, dictionary indices, booleans)."""
    out, nb = [], (w + 7) // 8
    while len(out) < n and pos < end:
        h, pos = _uvarint(buf, pos)
        if h & 1:                               # bit-packed groups of 8
            k = (h >> 1) * 8; out += _bitunpack(buf, pos, w, min(k, n - len(out))); pos += k * w // 8
        else:                                   # repeated value (run lengths from the data are clamped)
            out += [int.from_bytes(buf[pos:pos + nb], "little")] * min(h >> 1, n - len(out)); pos += nb
    del out[n:]
    return out

def _levels(buf, pos, end, maxd, n):
    """Definition levels, or None when every value is present."""
    h, p = _uvarint(buf, pos)
    w = maxd.bit_length()
    if not h & 1 and h >> 1 >= n and int.from_bytes(buf[p:p + (w + 7) // 8], "little") == maxd:
        return None                             # one run of "present": skip decoding
    return _rle(buf, pos, end, w, n)

def _zz(v): return (v >> 1) ^ -(v & 1)

def _delta(buf, pos, cap=None):
    """DELTA_BINARY_PACKED -> (values, end position). cap: most values expected."""
    bs, pos = _uvarint(buf, pos); nmb, pos = _uvarint(buf, pos)
    total, pos = _uvarint(buf, pos); v, pos = _uvarint(buf, pos)
    if not (0 < nmb and 0 < bs <= 1 << 20 and bs % nmb == 0 and bs // nmb % 8 == 0) or \
            cap is not None and total > cap:
        raise ParquetError("corrupt DELTA_BINARY_PACKED header")
    out, per = [_zz(v)], bs // nmb
    while len(out) < total:
        md, pos = _uvarint(buf, pos); md = _zz(md)
        widths = buf[pos:pos + nmb]; pos += nmb
        for w in widths:
            if len(out) >= total: break         # unused miniblocks have no data
            d = _bitunpack(buf, pos, w, per); pos += per * w // 8
            out += accumulate(map(_op.add, d, repeat(md)), initial=out[-1])
            del out[-per - 1]                   # drop the repeated initial value
    del out[total:]
    return out, pos

def _wrap(vals, bits, signed):                  # undo 32/64-bit overflow in deltas
    lo = -(1 << bits - 1) if signed else 0
    if vals and (min(vals) < lo or max(vals) >= lo + (1 << bits)):
        m = (1 << bits) - 1
        vals = [((v - lo) & m) + lo for v in vals]
    return vals

def _slices(b, lens):                           # split bytes b into consecutive lengths
    offs = list(accumulate(lens, initial=0))
    return list(map(b.__getitem__, map(slice, offs, offs[1:])))

def _dlba(buf, pos, cap=None):
    lens, pos = _delta(buf, pos, cap); total = sum(lens)
    if total > len(buf) - pos or min(lens, default=0) < 0: raise ParquetError("corrupt DELTA_LENGTH_BYTE_ARRAY")
    return _slices(bytes(buf[pos:pos + total]), lens), pos + total

def _dba(buf, pos, cap=None):
    pre, pos = _delta(buf, pos, cap); suf, pos = _dlba(buf, pos, cap)
    out, prev = [], b""
    for p, s in zip(pre, suf):
        prev = prev[:p] + s; out.append(prev)
    return out, pos

def _plain(buf, pos, col, n):
    if col.tc:                                  # numbers: one C-speed copy
        a = array(col.tc); a.frombytes(buf[pos:pos + n * a.itemsize])
        if _BIG: a.byteswap()
        return a
    t = col.ptype
    if t == 6:                                  # BYTE_ARRAY: 4-byte length + bytes
        b, out, u, pos = bytes(buf[pos:]), [], _U32, 0   # slicing bytes beats memoryview
        if n and len(b) >= 4:                   # all the same length? (hashes, codes): C speed
            step = 4 + u(b, 0)[0]; end = n * step
            if len(b) >= end and all(b[j:end:step] == b[j:j + 1] * n for j in range(4)):
                return list(map(b.__getitem__, map(slice, range(4, end + 1, step), range(step, end + 1, step))))
        ap = out.append
        for _ in range(n):
            ln = u(b, pos)[0]; ap(b[pos + 4:pos + 4 + ln]); pos += 4 + ln
        return out
    if t == 0: return list(map(bool, _bitunpack(buf, pos, 1, n)))
    w = 12 if t == 3 else col.type_length       # INT96 / FIXED_LEN_BYTE_ARRAY
    return _slices(bytes(buf[pos:pos + n * w]), repeat(w, n))

class _LazyDict:
    """BYTE_ARRAY dictionary that decodes entries on first use. Used when a read
    touches few rows of a chunk with a large dictionary."""
    def __init__(self, data, n, col):
        self.b, self.n, self.col, self.offs, self.cache = bytes(data), n, col, None, {}
        if col.ptype != 6: self.offs = _plain(data, 0, col, n)   # fixed width: raw values, converted on use

    def __len__(self): return self.n

    def __getitem__(self, i):
        v = self.cache.get(i)
        if v is None and i >= 0:
            if self.offs is None:               # entry offsets: one cheap pass
                offs, pos, b, u = [], 0, self.b, _U32
                ap = offs.append
                for _ in range(self.n): ap(pos); pos += 4 + u(b, pos)[0]
                self.offs = offs
            if self.col.ptype == 6:
                o = self.offs[i]; v = self.b[o + 4:o + 4 + _U32(self.b, o)[0]]
            else: v = self.offs[i]
            if self.col.conv: v = self.col.convert([v])[0]
            self.cache[i] = v
        return v

def _values(data, p, col, enc, n, cap=None):
    if enc == 0: return _plain(data, p, col, n)
    if enc == 3:                                # RLE booleans
        ln = int.from_bytes(data[p:p + 4], "little")
        return list(map(bool, _rle(data, p + 4, p + 4 + ln, 1, n)))
    if enc == 5:
        return _wrap(_delta(data, p, cap)[0][:n], 32 if col.ptype == 1 else 64, col.tc not in "IQ")
    if enc == 6: return _dlba(data, p, cap)[0][:n]
    if enc == 7: return _dba(data, p, cap)[0][:n]
    if enc == 9:                                # BYTE_STREAM_SPLIT
        k = col.width; src = data[p:p + n * k]; buf = bytearray(n * k)
        for j in range(k): buf[j::k] = src[j * n:(j + 1) * n]
        return _plain(memoryview(buf), 0, col, n)
    raise ValueError("encoding %d not supported" % enc)

def _expand(vals, defs, maxd, fill):
    """Spread non-null values over the rows, using fill where a level is < maxd."""
    if maxd != 1: defs = list(map(maxd.__eq__, defs))
    src = [fill]; src += vals
    return list(map(src.__getitem__, map(_op.mul, accumulate(defs), defs)))

class _Null:
    """Null at definition level d. Nested assembly uses d to tell a null struct
    from a struct with a null field; it becomes None in the output."""
    __slots__ = ("d",)
    def __init__(self, d): self.d = d

_NULLS = [_Null(d) for d in range(256)]

def _expand_nulls(vals, defs, maxd):
    it = iter(vals)
    return [next(it) if d == maxd else _NULLS[d] for d in defs]

def _lists(vals, defs, reps, rds, maxd, stack):
    """Rebuild nested lists from repetition and definition levels. rds[j] is the
    definition level of the j-th repeated node. `stack` holds the open list at each
    depth and carries over to the next page, since rows may span pages."""
    out, it, depth = [], iter(vals), len(rds)
    if not stack: stack += [None] * (depth + 1)
    if depth == 1: return _lists1(vals, defs, reps, rds[0], maxd, stack)
    stack[0] = out                              # new rows of this page
    for d, r in zip(defs or repeat(maxd), reps):
        for j in range(r, depth):              # open new lists below depth r
            if d < rds[j]:                     # null (marker) or empty list at this depth
                stack[j].append(_NULLS[d] if d < rds[j] - 1 else []); break
            new = []; stack[j].append(new); stack[j + 1] = new
        else:
            stack[depth].append(next(it) if d == maxd else _NULLS[d])
    return out

def _lists1(vals, defs, reps, rd, maxd, stack):
    """_lists for one level of lists (the common case), slicing rows out at C speed."""
    n = len(reps)
    starts = list(compress(range(n), map(_op.not_, reps)))     # entries that begin a row
    ev = vals if defs is None else _expand_nulls(vals, defs, maxd)
    if starts and starts[0] or not starts:      # leading entries continue the previous page's row
        stack[1].extend(ev[:starts[0] if starts else n])
    if not starts: return []
    ends = starts[1:] + [n]
    if defs is None: out = list(map(ev.__getitem__, map(slice, starts, ends)))
    else:
        out = []
        for a, b in zip(starts, ends):
            d = defs[a]
            out.append(ev[a:b] if d >= rd else _NULLS[d] if d < rd - 1 else [])
    stack[1] = out[-1]
    return out

# ---- Nested assembly: schema nodes -> Python values (dicts, lists, tuples) ----

class _Node:
    __slots__ = ("name", "path", "children", "leaf", "repeated", "d", "r", "ann")

def _fin(x): return None if type(x) is _Null else x

def _deep_fin(x, k):                            # markers above the slot depth -> None
    if type(x) is _Null: return None
    if k == 0 or not isinstance(x, list): return x
    return [_deep_fin(e, k - 1) for e in x]

def _mapdown(x, k, f):                          # apply f to the lists found at depth k
    if not isinstance(x, list): return x
    if k == 0: return f(x)
    return [_mapdown(e, k - 1, f) for e in x]

def _zipdown(xs, k, comb):                      # walk sibling streams together to depth k
    if k == 0: return comb(xs)
    x0 = xs[0]
    if not isinstance(x0, list): return x0
    return [_zipdown([x[i] for x in xs], k - 1, comb) for i in range(len(x0))]

def _fin_list(xs): return [None if type(x) is _Null else x for x in xs]

def _list_element(node):
    """The element node of a LIST group, following the spec's backward-compatibility rules."""
    rep = node.children[0]
    if rep.leaf is not None or len(rep.children) != 1 or rep.name in ("array", node.name + "_tuple"):
        return rep
    return rep.children[0]

def _struct_comb(node):
    names, d = [c.name for c in node.children], node.d
    def comb(slots):
        s0 = slots[0]
        if type(s0) is _Null and s0.d < d: return s0      # this struct (or an ancestor) is null
        return dict(zip(names, map(_fin, slots)))
    return comb

def _pair(slots): return (_fin(slots[0]), _fin(slots[1]))

def _value(node, leaves):
    """Rows of node values (as nested lists down to the node's own slot depth)."""
    if node.repeated:                           # a list: its content at depth r, the list at r - 1
        return [_mapdown(x, node.r - 1, _fin_list) for x in _inner(node, leaves)]
    return _inner(node, leaves)

def _inner(node, leaves):
    kids = node.children
    if node.ann == "list" and kids and kids[0].repeated:
        el = _list_element(node)
        if el is kids[0]: return _value(el, leaves)
        return [_mapdown(x, node.r, _fin_list) for x in _value(el, leaves)]
    if node.ann == "map" and kids and kids[0].repeated and kids[0].leaf is None:
        kv = kids[0]
        if len(kv.children) == 1:               # keys only: a list of keys
            return [_mapdown(x, node.r, _fin_list) for x in _value(kv.children[0], leaves)]
        ks, vs = (_value(c, leaves) for c in kv.children[:2])
        return [_zipdown(xs, kv.r, _pair) for xs in zip(ks, vs)]
    return _content(node, leaves)

def _content(node, leaves):
    if node.leaf is not None: return leaves[node.leaf]
    kids = [_value(c, leaves) for c in node.children]
    comb = _struct_comb(node)
    return [_zipdown(xs, node.r, comb) for xs in zip(*kids)]

def _leaves(node):
    return [node.leaf] if node.leaf is not None else [i for c in node.children for i in _leaves(c)]

# ---- Logical types ----

_EPOCH = _dt.datetime(1970, 1, 1)
_EPOCH_UTC = _EPOCH.replace(tzinfo=_dt.timezone.utc)
_DEC = _dec.Context(prec=1000)
_TD = {1: lambda v: _dt.timedelta(0, 0, 0, v), 2: lambda v: _dt.timedelta(0, 0, v),
       3: lambda v: _dt.timedelta(0, 0, v // 1000)}           # millis, micros, nanos
_Z = repeat(0)

def _timedeltas(unit, v):                       # C-level map over the timedelta constructor
    if unit == 1: return map(_dt.timedelta, _Z, _Z, _Z, v)
    if unit == 3: v = map(_op.floordiv, v, repeat(1000))
    return map(_dt.timedelta, _Z, _Z, v)

def _timestamp(unit, utc):
    e = _EPOCH_UTC if utc else _EPOCH
    return lambda v: list(map(_op.add, repeat(e), _timedeltas(unit, v)))

def _time(unit):
    td = _TD[unit]
    return lambda v: [(_dt.datetime.min + td(x)).time() for x in v]

def _decimal(scale, from_bytes):
    def conv(v):
        if from_bytes: v = [int.from_bytes(b, "big", signed=True) for b in v]
        return [_dec.Decimal(x).scaleb(-scale, _DEC) for x in v]
    return conv

def _int96(v):
    out = []
    for b in v:
        ns, jd = struct.unpack("<qi", b)
        out.append(_EPOCH + _dt.timedelta(jd - 2440588, 0, ns // 1000))
    return out

_PTYPES = ["BOOLEAN", "INT32", "INT64", "INT96", "FLOAT", "DOUBLE", "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"]
_UNITS = {1: "ms", 2: "us", 3: "ns"}

class Column:
    """One leaf column: name, physical/logical type, nullability, value conversion."""
    def __init__(self, el, path, max_def, max_rep, rep_defs=()):
        self.path, self.max_def, self.max_rep, self.rep_defs = path, max_def, max_rep, rep_defs
        self.name = ".".join(path)
        self.ptype, self.type_length = el.get(1), el.get(2)
        self.nullable = el.get(3, 0) != 0
        lt, ct, t = el.get(10, {}), el.get(6), self.ptype
        self.tc = {1: "i", 2: "q", 4: "f", 5: "d"}.get(t, "")  # array typecode
        self.width = {1: 4, 2: 8, 4: 4, 5: 8, 3: 12}.get(t, self.type_length)
        self.conv, self.is_str, self.logical = None, False, ""
        self.isfloat = t in (4, 5) or (15 in lt and t == 7)
        if (1 in lt or 4 in lt or 12 in lt or ct in (0, 4, 19)) and t in (6, 7):
            self.is_str, self.logical = True, "STRING"
            self.conv = lambda v: list(map(bytes.decode, v))
        elif (5 in lt or ct == 5) and t in (1, 2, 6, 7):
            scale = lt[5].get(1, 0) if 5 in lt else el.get(7, 0)
            self.logical = "DECIMAL(%d,%d)" % (lt[5].get(2, 0) if 5 in lt else el.get(8, 0), scale)
            self.conv = _decimal(scale, t in (6, 7))
        elif (6 in lt or ct == 6) and t == 1:
            self.logical = "DATE"
            self.conv = lambda v: list(map(_dt.date.fromordinal, map(_op.add, v, repeat(719163))))
        elif (8 in lt or ct in (9, 10)) and t == 2:
            unit, utc = (next(iter(lt[8][2])), lt[8].get(1)) if 8 in lt else (ct - 8, True)
            self.logical = "TIMESTAMP(%s%s)" % (_UNITS[unit], ",UTC" if utc else "")
            self.conv = _timestamp(unit, utc)
        elif (7 in lt or ct in (7, 8)) and t in (1, 2):
            unit = next(iter(lt[7][2])) if 7 in lt else ct - 6
            self.logical, self.conv = "TIME(%s)" % _UNITS[unit], _time(unit)
        elif (10 in lt and not lt[10].get(2, True)) or ct in (11, 12, 13, 14):
            self.logical = "UINT"
            if self.tc: self.tc = self.tc.upper()   # 'I' / 'Q': read as unsigned
        elif 14 in lt and t == 7 and self.type_length == 16:
            self.logical, self.conv = "UUID", lambda v: [uuid.UUID(bytes=b) for b in v]
        elif 15 in lt and t == 7 and self.type_length == 2:
            self.logical = "FLOAT16"
            self.conv = lambda v: list(struct.unpack("<%de" % len(v), b"".join(v)))
        elif t == 3:
            self.logical, self.conv = "TIMESTAMP(ns)", _int96

    def __repr__(self):
        return "Column(%r, %s%s%s%s%s)" % (self.name, "LIST<" * self.max_rep, _PTYPES[self.ptype],
                                       " " + self.logical if self.logical else "", ">" * self.max_rep,
                                       ", nullable" if self.nullable else "")

    def convert(self, v):
        try: return self.conv(v)
        except (OverflowError, ValueError) as e:
            raise ValueError("column %r: cannot convert %s values (%s)"
                             % (self.name, self.logical, e) + ("; arrays=True gives raw numbers" if self.tc else "")) from None

    def stat(self, raw):
        """Decode a min/max statistic into a comparable value (None if unusable)."""
        t = self.ptype
        try:
            if self.tc: v = array(self.tc, raw[:struct.calcsize(self.tc)])[0]
            elif t == 0: v = raw[0] != 0
            elif t in (6, 7): v = bytes(raw)
            else: return None
        except (ValueError, IndexError):
            return None
        if v != v: return None                  # NaN bound: unusable
        if self.is_str: return v                # strings compare as UTF-8 bytes
        return self.conv([v])[0] if self.conv else v

class DictionaryColumn:
    """Dictionary-encoded column: indices (array of int, -1 for null) into dictionary."""
    def __init__(self, indices, dictionary):
        self.indices, self.dictionary = indices, dictionary

    def __len__(self): return len(self.indices)
    def __iter__(self): return iter(self.to_list())
    def to_list(self): return list(map((self.dictionary + [None]).__getitem__, self.indices))
    def __repr__(self): return "DictionaryColumn(%d values, %d distinct)" % (len(self.indices), len(self.dictionary))

# ---- Byte sources ----

class _BufferSource:
    def __init__(self, buf):
        try: self.buf = memoryview(buf).cast("B")
        except TypeError: self.buf = memoryview(memoryview(buf).tobytes())   # non-contiguous views
        self.size = len(self.buf)
    def read(self, off, n):                     # a negative offset would silently wrap around
        if off < 0 or n < 0 or off > self.size: raise ParquetError("offset %d outside the file" % off)
        return self.buf[off:off + n]

def _check_read(off, n, size):
    if off < 0 or n < 0 or off > size: raise ParquetError("offset %d outside the file" % off)
    return min(n, size - off)

class _LocalSource:
    """A local file read with os.pread (seek + read where pread is missing). Not
    memory-mapped: a mapped file truncated by another process kills Python with
    SIGBUS, while a short read here raises ParquetError."""
    def __init__(self, path):
        import stat
        if not stat.S_ISREG(os.stat(path).st_mode):        # before open(): FIFOs would block, devices never end
            raise ParquetError("%r is not a regular file" % (path,))
        self.f = open(path, "rb")
        self.size = os.fstat(self.f.fileno()).st_size
        self.pread = getattr(os, "pread", None)
        if self.pread is None:
            import threading; self.lock = threading.Lock()
    def read(self, off, n):
        n = _check_read(off, n, self.size); parts, got = [], 0
        while got < n:
            if self.pread: b = self.pread(self.f.fileno(), n - got, off + got)
            else:
                with self.lock: self.f.seek(off + got); b = self.f.read(n - got)
            if not b: raise ParquetError("file %r shrank while it was being read" % self.f.name)
            parts.append(b); got += len(b)
        return memoryview(parts[0] if len(parts) == 1 else b"".join(parts))
    def close(self): self.f.close()

class _FileObjSource:
    def __init__(self, f):
        if not isinstance(f.read(0), bytes): raise TypeError("file object must be opened in binary mode ('rb')")
        self.f = f; f.seek(0, 2); self.size = f.tell()
    def read(self, off, n):                     # loop: raw streams may return fewer bytes than asked
        n = _check_read(off, n, self.size); self.f.seek(off); parts, got = [], 0
        while got < n:
            b = self.f.read(n - got)
            if not b: raise ParquetError("file object returned %d of %d bytes at offset %d" % (got, n, off))
            parts.append(b); got += len(b)
        return memoryview(b"".join(parts))

class _CheckedSource:
    """Wraps a user object with read(offset, size) and size, checking what it returns."""
    def __init__(self, src):
        self.src, self.size = src, src.size
        if not isinstance(self.size, int) or self.size < 0: raise TypeError("source.size must be a non-negative int")
    def read(self, off, n):
        n = _check_read(off, n, self.size); b = self.src.read(off, n)
        try: b = memoryview(b).cast("B")
        except TypeError: raise TypeError("source.read must return bytes, got %s" % type(b).__name__) from None
        if len(b) != n: raise ParquetError("source returned %d bytes, expected %d, at offset %d" % (len(b), n, off))
        return b

import re as _re
_CONTENT_RANGE = _re.compile(r"bytes (\d+)-(\d+)/(\d+|\*)")

class HTTPSource:
    """Reads an http(s) URL with range requests. Caches what it fetches; `prefetch`
    coalesces nearby ranges and fetches them in parallel.

    Every response is checked: status, Content-Range (start, end, total) and body
    length must match the request; the file size and ETag/Last-Modified may not
    change while reading; bodies are read with an overall deadline of `timeout`
    seconds per request; a server that ignores ranges may send at most
    `max_download` bytes. Redirects must stay on http(s), may not downgrade https to
    http, and drop all caller headers (Authorization, cookies, API keys) when they
    change host or scheme."""
    def __init__(self, url, headers=None, timeout=60, gap=32 * 1024, workers=8, max_download=512 << 20):
        if not isinstance(url, str) or not url.lower().startswith(("http://", "https://")):
            raise ValueError("HTTPSource needs an http:// or https:// URL, got %r" % (url,))
        from urllib.parse import urlsplit
        if "@" in urlsplit(url).netloc:
            raise ValueError("credentials in the URL are not supported; pass them in headers")
        self.url, self.headers, self.timeout, self.gap, self.workers = url, dict(headers or {}), timeout, gap, workers
        self.max_download = max_download
        self.size, self.validator, self.blocks, self.requests, self.bytes_read = None, None, [], 0, 0

    def _get(self, start=None, stop=None, suffix=None):
        """Fetch [start, stop) or the last `suffix` bytes -> (start, data) after checking
        the response. (None, None) if a suffix request got a whole big file back."""
        import http.client, time
        from urllib.request import Request
        rng = "-%d" % suffix if suffix else "%d-%d" % (start, stop - 1)
        h = dict(self.headers); h["Range"] = "bytes=" + rng
        deadline = time.monotonic() + self.timeout
        try:
            with _opener().open(Request(self.url, headers=h), timeout=self.timeout) as r:
                status, cr = r.status, (r.headers.get("Content-Range") or "").strip()
                validator = r.headers.get("ETag") or r.headers.get("Last-Modified")
                if status == 200:               # range ignored: the whole file
                    n = int(r.headers.get("Content-Length") or -1)
                    if suffix and n > 4 * 1024 * 1024: self._learn(n, validator); return None, None
                    data = _read_body(r, self.max_download, deadline, exact=False)
                    self._learn(len(data), validator); self.blocks = [(0, data)]
                    self._count(data); return 0, data
                if status != 206: raise ParquetError("unexpected HTTP status %d for a range request" % status)
                m = _CONTENT_RANGE.fullmatch(cr)
                if not m: raise ParquetError("bad or missing Content-Range %r" % cr)
                a, b, total = int(m[1]), int(m[2]), m[3]
                total = self.size if total == "*" else int(total)
                if total is None: raise ParquetError("server does not report the file size")
                want_a = max(0, total - suffix) if suffix else start
                want_b = total - 1 if suffix else min(stop - 1, total - 1)
                if (a, b) != (want_a, want_b): raise ParquetError("server sent bytes %d-%d for request %s" % (a, b, rng))
                data = _read_body(r, b - a + 1, deadline, exact=True)
                self._learn(total, validator); self._count(data); return a, data
        except (http.client.HTTPException, ValueError) as e:
            if isinstance(e, ParquetError): raise
            raise ParquetError("bad response from %s: %s: %s" % (self.url, type(e).__name__, e)) from e

    def _learn(self, size, validator):          # the file must not change while we read it
        if self.size is None: self.size = size
        elif size != self.size: raise ParquetError("file size changed on the server (%d -> %d)" % (self.size, size))
        if validator:
            if self.validator is None: self.validator = validator
            elif validator != self.validator: raise ParquetError("file changed on the server while reading")

    def _count(self, data): self.requests += 1; self.bytes_read += len(data)

    def _find(self, off, n):
        for start, data in self.blocks:
            if start <= off and off + n <= start + len(data):
                return memoryview(data)[off - start:off - start + n]

    def tail(self, n):
        from urllib.error import HTTPError
        try: start, data = self._get(suffix=n)
        except HTTPError as e:                  # some servers reject suffix ranges
            if e.code == 404: raise
            start = None
        if start is None:
            if self.size is None: self._get(0, 1)                    # learns the size
            if self.blocks and self.blocks[0][0] == 0 and len(self.blocks[0][1]) == self.size:
                return memoryview(self.blocks[0][1])[max(0, self.size - n):]
            start, data = self._get(max(0, self.size - n), self.size)
        if self._find(start, len(data)) is None: self.blocks.append((start, data))
        return memoryview(data)

    def read(self, off, n):
        if self.size is None: raise ParquetError("read before the file size is known")
        n = _check_read(off, n, self.size)
        if not n: return memoryview(b"")
        v = self._find(off, n)
        if v is None:
            self.blocks.append(self._get(off, off + n)); v = self._find(off, n)
        if v is None: raise ParquetError("short read from %s at %d" % (self.url, off))
        return v

    def prefetch(self, spans):
        need = [(a, min(b, self.size)) for a, b in sorted(spans) if b > a and a < self.size and self._find(a, min(b, self.size) - a) is None]
        merged = []
        for a, b in need:
            if merged and a - merged[-1][1] <= self.gap: merged[-1][1] = max(b, merged[-1][1])
            else: merged.append([a, b])
        if len(merged) == 1: self.read(merged[0][0], merged[0][1] - merged[0][0])
        elif merged:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(min(self.workers, len(merged))) as ex:
                self.blocks += ex.map(lambda s: self._get(s[0], s[1]), merged)

    def clear(self):                            # keep only the footer block
        self.blocks = self.blocks[:1]

def _read_body(r, limit, deadline, exact):
    """Read a response body: at most `limit` bytes (exactly `limit` if exact), and
    before `deadline` even if the server trickles bytes (socket timeouts alone don't)."""
    import time
    parts, got = [], 0
    read1 = getattr(r, "read1", None) or r.read
    while True:
        if time.monotonic() > deadline: raise TimeoutError("HTTP response took longer than the timeout")
        chunk = read1(min(1 << 16, limit + 1 - got))
        if not chunk: break
        parts.append(chunk); got += len(chunk)
        if got > limit:
            raise ParquetError("response larger than requested" if exact else
                               "server ignores Range requests and the file is larger than max_download")
    if exact and got != limit: raise ParquetError("short response: %d of %d bytes" % (got, limit))
    return b"".join(parts)

_OPENER = None
def _opener():
    """urllib opener for HTTPSource: http(s) only; redirects may not leave http(s) or
    downgrade https to http; on a change of host or scheme every caller header except
    Range is dropped (plain urllib forwards Authorization, cookies and API keys to any
    host); redirect bodies are not read."""
    global _OPENER
    if _OPENER is None:
        import urllib.request
        from urllib.parse import urlsplit
        class Redirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                old, new_ = urlsplit(req.full_url), urlsplit(newurl)
                if new_.scheme.lower() not in ("http", "https"):
                    raise ParquetError("refusing redirect to non-HTTP URL %r" % newurl)
                if old.scheme.lower() == "https" and new_.scheme.lower() == "http":
                    raise ParquetError("refusing redirect from https to http: %r" % newurl)
                if "@" in new_.netloc: raise ParquetError("refusing redirect to a URL with credentials")
                new = super().redirect_request(req, fp, code, msg, headers, newurl)
                if new is not None and (new_.netloc.lower(), new_.scheme.lower()) != (old.netloc.lower(), old.scheme.lower()):
                    new.headers = {k: v for k, v in new.headers.items() if k.lower() == "range"}
                    new.unredirected_hdrs = {}
                return new
        def no_drain(name):                     # the stdlib reads the whole redirect body first
            base = getattr(urllib.request.HTTPRedirectHandler, name)
            def handler(self, req, fp, code, msg, headers):
                fp.read = lambda *a, **k: b""
                return base(self, req, fp, code, msg, headers)
            return handler
        for code in (301, 302, 303, 307, 308):
            if hasattr(urllib.request.HTTPRedirectHandler, "http_error_%d" % code):
                setattr(Redirect, "http_error_%d" % code, no_drain("http_error_%d" % code))
        _OPENER = urllib.request.OpenerDirector()
        for h in (urllib.request.ProxyHandler(), urllib.request.HTTPHandler(), urllib.request.HTTPSHandler(),
                  urllib.request.HTTPDefaultErrorHandler(), Redirect(), urllib.request.HTTPErrorProcessor()):
            _OPENER.add_handler(h)
    return _OPENER

def _open(src):
    """Turn a source into something with read(offset, size) and size. Strings and
    path objects are always local files: a remote file needs an explicit
    HTTPSource (or open_url), so a path can never cause a network request."""
    if isinstance(src, (bytes, bytearray, memoryview)): return _BufferSource(src)
    if isinstance(src, (str, os.PathLike)):
        try: return _LocalSource(src)
        except FileNotFoundError as e:
            if str(src).lower().startswith(("http://", "https://")):
                raise FileNotFoundError("no local file %r; to read a URL use microparquet.open_url(url) "
                                        "or ParquetFile(HTTPSource(url))" % str(src)) from e
            raise
    if isinstance(src, (HTTPSource, _BufferSource, _LocalSource, _FileObjSource)): return src
    if hasattr(src, "seek") and hasattr(src, "read"): return _FileObjSource(src)
    if hasattr(src, "read") and hasattr(src, "size"): return _CheckedSource(src)   # read(offset, size) protocol
    raise TypeError("expected a path, bytes, a binary file object, HTTPSource, or an object "
                    "with read(offset, size) and size; got %s" % type(src).__name__)

# ---- Row ranges: sorted lists of disjoint (start, stop) ----

def _isect(a, b):
    out, i, j = [], 0, 0
    while i < len(a) and j < len(b):
        lo, hi = max(a[i][0], b[j][0]), min(a[i][1], b[j][1])
        if lo < hi: out.append((lo, hi))
        if a[i][1] < b[j][1]: i += 1
        else: j += 1
    return out

def _union(rs):
    out = []
    for a, b in sorted(rs):
        if out and a <= out[-1][1]: out[-1] = (out[-1][0], max(b, out[-1][1]))
        else: out.append((a, b))
    return out

def _overlaps(ranges, ends, a, b):              # parts of [a, b) inside ranges
    i, out = bisect_right(ends, a), []
    while i < len(ranges) and ranges[i][0] < b:
        out.append((max(a, ranges[i][0]), min(b, ranges[i][1]))); i += 1
    return out

# ---- Predicates ----

_OPS = {"=": _op.eq, "==": _op.eq, "!=": _op.ne, "<": _op.lt, "<=": _op.le, ">": _op.gt, ">=": _op.ge}

def _may_match(op, x, lo, hi):
    """Can any value in [lo, hi] satisfy `op x`?"""
    if lo is None or hi is None: return True
    try:
        if op in ("=", "=="): return lo <= x <= hi
        if op == "!=": return not lo == hi == x
        if op == "<": return lo < x
        if op == "<=": return lo <= x
        if op == ">": return hi > x
        if op == ">=": return hi >= x
        if op == "in": return any(lo <= v <= hi for v in x if v is not None)
        if op == "not in": return not (lo == hi and lo in x)
    except TypeError:
        return True
    return True

def _null_ok(op, x):
    """Do null values satisfy the predicate? As in pyarrow: only `in` a set holding
    None, and `not in` a set without None."""
    return op in ("in", "not in") and (op == "in") == (None in x)

def _can_match(op, x, lo, hi, nulls, count, isfloat):
    """Can a chunk or page with these statistics hold a row satisfying `op x`?
    nulls and count may be None (unknown)."""
    if _null_ok(op, x) and (nulls is None or nulls > 0): return True
    if nulls is not None and count is not None and nulls >= count: return False
    if isfloat and (op in ("!=", "not in") or op == "in" and any(v != v for v in x)):
        return True                             # NaN satisfies these but is left out of statistics
    return _may_match(op, x, lo, hi)

def _fkey(v):                                   # float set membership as in Arrow: NaN == NaN, -0.0 != 0.0
    if v != v: return "nan"
    if v == 0 and isinstance(v, float): return (0.0, str(v))
    return v

def _mask(vals, op, x, isfloat=False):
    if op in ("in", "not in"):                  # nulls handled like any other member (see _null_ok)
        if isfloat:
            keys, keep = frozenset(map(_fkey, x)), op == "in"
            return [(_fkey(v) in keys) == keep for v in vals]
        m = map(x.__contains__, vals)
        return list(m if op == "in" else map(_op.not_, m))
    f = _OPS[op]
    if None not in vals: return list(map(f, vals, repeat(x)))
    return [v is not None and f(v, x) for v in vals]

# ---- The file ----

class ParquetFile:
    """A Parquet file opened from a local path, bytes, a binary file object, an
    HTTPSource (see open_url), or any object
    with `read(offset, size)` and `size`."""

    def __init__(self, source, tail=64 * 1024):
        self._src = src = _open(source)
        if hasattr(src, "tail"): buf = src.tail(tail); size = src.size
        else: size = src.size; buf = src.read(max(0, size - tail), min(tail, size))
        if len(buf) < 12 or bytes(buf[-4:]) != b"PAR1":
            hint = "; bytes are read as file contents, pass a str or Path for a file name" \
                if isinstance(source, (bytes, bytearray)) and len(source) < 4096 and source[:1] in (b"/", b".", b"~") else ""
            raise ParquetError("not a parquet file (or encrypted footer)" + hint)
        mlen = int.from_bytes(buf[-8:-4], "little")
        if mlen + 12 > size: raise ParquetError("footer length %d larger than the file" % mlen)
        if mlen + 8 > len(buf): buf = src.read(size - 8 - mlen, mlen + 8)
        try: self._parse_footer(bytes(buf[len(buf) - 8 - mlen:len(buf) - 8]))
        except (ParquetError, NotImplementedError, MemoryError): raise
        except Exception as e: raise ParquetError("corrupt footer: %s: %s" % (type(e).__name__, e)) from e
        self._pi = {}                           # page index cache: (rg, col, kind) -> parsed

    def _parse_footer(self, footer):
        meta, _ = _tstruct(footer, 0)
        self.num_rows, self.created_by = meta.get(3, 0), meta.get(6, b"").decode(errors="replace")
        self.metadata = {kv[1].decode(): kv.get(2, b"").decode(errors="replace") for kv in meta.get(5, [])}
        self._rgs, self.columns = meta.get(4, []), []
        it = iter(meta[2]); root = next(it)
        def walk(n, parent, d, r, rds):
            for _ in range(n):
                el = next(it); rt = el.get(3, 0)
                node = _Node(); node.name = el[4].decode(); node.path = parent.path + [node.name]
                node.repeated, node.children, node.leaf = rt == 2, [], None
                node.d, node.r = d + (rt != 0), r + (rt == 2)
                rds2 = rds + (node.d,) if rt == 2 else rds
                lt, ct = el.get(10, {}), el.get(6)
                node.ann = "list" if ct == 3 or 3 in lt else "map" if ct in (1, 2) or 2 in lt else None
                parent.children.append(node); self._nodes[".".join(node.path)] = node
                if 1 in el:
                    node.leaf = len(self.columns); self.columns.append(Column(el, node.path, node.d, node.r, rds2))
                else: walk(el.get(5, 0), node, node.d, node.r, rds2)
        self._root = _Node(); self._root.path, self._root.children, self._nodes = [], [], {}
        walk(root.get(5, 0), self._root, 0, 0, ())
        for rg in self._rgs:                    # every row group must describe every column
            if len(rg.get(1, ())) != len(self.columns) or any(3 not in cc for cc in rg[1]) or not isinstance(rg.get(3), int):
                raise ParquetError("row group metadata does not match the schema")

    schema = property(lambda self: list(self.columns))
    column_names = property(lambda self: [n.name for n in self._root.children])

    def close(self):
        if hasattr(self._src, "close"): self._src.close()
        self._src = None
    def __enter__(self): return self
    def __exit__(self, *exc): self.close()
    num_row_groups = property(lambda self: len(self._rgs))

    def __repr__(self):
        return "<ParquetFile %d rows, %d row groups, %d columns>" % (self.num_rows, len(self._rgs), len(self.columns))

    def statistics(self, column):
        """Per row group (min, max, null_count) of a column; None where missing."""
        c = self.columns[self._find(column)]; out = []
        for rg in self._rgs:
            lo, hi, nulls = self._chunk_stats(rg, self._find(column))
            if c.is_str: lo, hi = [v.decode(errors="replace") if v is not None else None for v in (lo, hi)]
            out.append((lo, hi, nulls))
        return out

    def _find(self, name):                      # leaf column index (for filters)
        node = self._nodes.get(name)
        if node is None: raise KeyError("no column %r" % name)
        if node.leaf is None: raise ValueError("%r is not a leaf column; filter on one of its fields" % name)
        return node.leaf

    # -- metadata helpers --

    def _chunk_stats(self, rg, c):
        return self._stats(self.columns[c], rg[1][c][3].get(12))

    def _stats(self, col, st):
        if not st: return None, None, None
        lo, hi = st.get(6), st.get(5)
        if lo is None and col.tc in ("i", "q", "f", "d"): lo, hi = st.get(2), st.get(1)  # legacy fields
        return (col.stat(lo) if lo is not None else None, col.stat(hi) if hi is not None else None, st.get(3))

    def _load_page_index(self, rgs, cols):
        spans, want = [], []
        for i in rgs:
            for c, kind in cols:
                cc = self._rgs[i][1][c]; off, ln = cc.get(6 if kind else 4), cc.get(7 if kind else 5)
                if off and ln and (i, c, kind) not in self._pi:
                    spans.append((off, off + ln)); want.append((i, c, kind, off, ln))
        if hasattr(self._src, "prefetch") and len(spans) > 1: self._src.prefetch(spans)
        for i, c, kind, off, ln in want:
            try:
                ix, _ = _thrift(self._src.read(off, ln))
                if not kind:                    # OffsetIndex -> [(offset, size, first_row)]
                    ix = [(p[1], p[2], p[3]) for p in ix[1]]
                elif not (len(ix[1]) == len(ix[2]) == len(ix[3])): raise ValueError("lengths differ")
            except ParquetError: raise
            except Exception:                   # a broken page index only costs pruning
                ix = None
            if ix is not None and kind == 0 and (not ix or any(b[0] <= a[0] or b[2] < a[2] for a, b in zip(ix, ix[1:]))):
                ix = None                       # offsets must increase; otherwise fall back to scanning
            self._pi[i, c, kind] = ix

    def _page_ranges(self, i, c, preds):
        """Row ranges of row group i that the ColumnIndex says may match."""
        ci, oi = self._pi.get((i, c, 1)), self._pi.get((i, c, 0))
        n = self._rgs[i][3]
        if not ci or not oi: return [(0, n)]
        col, out = self.columns[c], []
        for k, (null, lo, hi) in enumerate(zip(ci[1], ci[2], ci[3])):
            first, last = oi[k][2], oi[k + 1][2] if k + 1 < len(oi) else n
            nulls = ci[5][k] if 5 in ci and k < len(ci[5]) else None
            lo, hi = (None, None) if null else (col.stat(lo), col.stat(hi))
            if null: nulls = last - first
            if all(_can_match(op, x, lo, hi, nulls, last - first, col.isfloat) for op, x in preds):
                out.append((first, last))
        return _union(out)

    def _chunk_start(self, cm):
        d = cm.get(11)
        return d if d and d < cm[9] else cm[9]

    def _spans(self, i, c, ranges):
        cm, oi = self._rgs[i][1][c][3], self._pi.get((i, c, 0))
        start = self._chunk_start(cm)
        n, ends = self._rgs[i][3], [b for a, b in ranges]
        if not oi: return [(start, start + cm[7])] if ranges == [(0, n)] else []  # partial: fetched lazily
        out = [(start, oi[0][0])] if start < oi[0][0] else []
        for k, (off, size, first) in enumerate(oi):
            last = oi[k + 1][2] if k + 1 < len(oi) else n
            if _overlaps(ranges, ends, first, last): out.append((off, off + size))
        return out

    # -- reading a column chunk --

    def _pages(self, i, c, ranges, ends):
        """Yield (header, compressed bytes, first row, row count) for the dictionary
        page (first row None) and every data page overlapping `ranges`."""
        cm, oi, src = self._rgs[i][1][c][3], self._pi.get((i, c, 0)), self._src
        start, n = self._chunk_start(cm), self._rgs[i][3]
        if oi:
            if start < oi[0][0]:
                buf = src.read(start, oi[0][0] - start); ph, p = _thrift(buf)
                if ph[1] == 2: yield ph, buf[p:p + ph[3]], None, 0, start
            for k, (off, size, first) in enumerate(oi):
                last = oi[k + 1][2] if k + 1 < len(oi) else n
                if _overlaps(ranges, ends, first, last):
                    buf = src.read(off, size); ph, p = _thrift(buf)
                    yield ph, buf[p:p + ph[3]], first, last - first, off
            return
        rep, size = self.columns[c].max_rep, cm[7]
        # Remote reads of part of a chunk fetch it a piece at a time; otherwise all at once.
        lazy = hasattr(src, "prefetch") and not rep and ranges != [(0, n)]
        buf, p, row = src.read(start, min(size, 1 << 18) if lazy else size), 0, 0
        def grow(m):                            # make buf cover m bytes of the chunk
            if lazy: return memoryview(bytes(buf) + bytes(src.read(start + len(buf), max(m, 2 * len(buf)) - len(buf))))
            return src.read(start, m)           # also covers an understated chunk size
        while p < size and row < n:
            if not rep and row >= ends[-1]: break           # past the last wanted row
            try: ph, q = _thrift(buf, p)
            except IndexError: buf = grow(p + 4096); ph, q = _thrift(buf, p)
            if q + ph[3] > len(buf): buf = grow(q + ph[3])
            if ph[3] < 0: raise ParquetError("corrupt page header")
            key = start + p
            raw, p = buf[q:q + ph[3]], q + ph[3]
            if ph[1] == 2: yield ph, raw, None, 0, key; continue
            if rep and ph[1] in (0, 3): yield ph, raw, -1, None, key; continue   # rows known after decoding
            k = ph[5][1] if ph[1] == 0 else ph[8][3] if ph[1] == 3 else 0
            if k < 0: raise ParquetError("corrupt page header")
            if k and _overlaps(ranges, ends, row, row + k): yield ph, raw, row, k, key
            row += k

    def _read_chunk(self, i, c, ranges, mode=0, preds=None, marks=False, dcache=None):
        try: return self._read_chunk_(i, c, ranges, mode, preds, marks, dcache)
        except (ParquetError, ImportError, NotImplementedError, MemoryError): raise
        except Exception as e:                  # corrupt data surfaces as many exception types
            raise ParquetError("cannot read column %r in row group %d: %s: %s"
                               % (self.columns[c].name, i, type(e).__name__, e)) from e

    def _read_chunk_(self, i, c, ranges, mode=0, preds=None, marks=False, dcache=None):
        """Values of column c for the rows in `ranges` of row group i, as a list of
        (first row, values). mode 0: Python values, 1: raw arrays, 2: dictionary
        indices. With preds, pages and dictionaries that cannot match are skipped."""
        col, cc = self.columns[c], self._rgs[i][1][c]
        if cc.get(1): raise NotImplementedError("column %r is stored in another file (%r)" % (col.name, cc[1]))
        cm = cc[3]
        if col.max_rep: mode = 0
        if hasattr(self._src, "prefetch"): self._src.prefetch(self._spans(i, c, ranges))
        codec, maxd, ends = cm[4], col.max_def, [b for a, b in ranges]
        dictionary, out, plain_pages, row, maxr, stack = None, [], False, 0, col.max_rep, []
        look = None                             # set by the dictionary page, which comes first
        if marks: mode = 0
        nsel, nrows = sum(b - a for a, b in ranges), self._rgs[i][3]
        # Decoded pages are kept while iterating batches of one row group, so a page
        # that spans batches is decoded once; list state is restored with it.
        pcache = dcache.setdefault(("pages", c, mode, marks), {}) if dcache is not None else None
        for ph, raw, first, k, key in self._pages(i, c, ranges, ends):
            if pcache is not None and key in pcache:
                first0, vals, is_ix, k, snap = pcache[key]
                if first == -1: first = first0; row = first + k
                if snap is not None: stack[:] = snap
                if vals is None: continue           # passed earlier: only its row count was kept
                if mode == 2 and not is_ix: plain_pages = True
                for a, b in _overlaps(ranges, ends, first, first + k):
                    v = vals if b - a == len(vals) else vals[a - first:b - first]
                    if is_ix and mode != 2: v = list(map(look, v))
                    out.append((a, v, is_ix and mode == 2))
                continue
            if first is None:                   # dictionary page
                if dcache is not None and (c, mode) in dcache:
                    dictionary, look = dcache[c, mode]; continue
                if (col.ptype == 6 or col.conv and mode == 0) and mode != 2 and nsel * 4 < ph[7][1]:
                    dictionary = _LazyDict(_decompress(codec, raw, ph[2]), ph[7][1], col)
                    look = dictionary.__getitem__
                    if dcache is not None: dcache[c, mode] = dictionary, look
                    continue
                d = _plain(_decompress(codec, raw, ph[2]), 0, col, ph[7][1])
                if mode != 1 or not col.tc:
                    if isinstance(d, array): d = d.tolist()
                    if col.conv: d = col.convert(d)
                dictionary = d
                look = ((d.tolist() if isinstance(d, array) else d) + [None]).__getitem__  # [-1] -> None
                if dcache is not None: dcache[c, mode] = dictionary, look
                es = cm.get(13)
                if preds and es and all(e[2] in (2, 8) for e in es if e[1] in (0, 3)) \
                        and not any(_null_ok(op, x) for op, x in preds):
                    mask = [True] * len(d)      # fully dictionary-encoded: test the dictionary
                    for op, x in preds: mask = list(map(_op.and_, mask, _mask(d, op, x, col.isfloat)))
                    if not any(mask): return []
                continue
            st = (ph.get(5) or ph.get(8) or {}).get(5 if ph[1] == 0 else 8)
            if preds and st:                    # page-header statistics
                lo, hi, nulls = self._stats(col, st)
                if not all(_can_match(op, x, lo, hi, nulls, k, col.isfloat) for op, x in preds): continue
            if ph[1] == 0:
                dh = ph[5]; n, enc = dh[1], dh[2]
                if maxd and dh.get(3) == 4 or maxr and dh.get(4) == 4:   # writers often *declare* it unused
                    raise NotImplementedError("BIT_PACKED levels (very old writers) in column %r" % col.name)
                data, p, defs = _decompress(codec, raw, ph[2]), 0, None
                if maxr:
                    ln = int.from_bytes(data[:4], "little"); reps = _rle(data, 4, 4 + ln, maxr.bit_length(), n)
                    data = data[4 + ln:]
                if maxd:
                    ln = int.from_bytes(data[:4], "little"); defs = _levels(data, 4, 4 + ln, maxd, n); p = 4 + ln
            else:
                dh = ph[8]; n, enc = dh[1], dh[4]; dl, rl = dh.get(5, 0), dh.get(6, 0)
                defs = _levels(raw, rl, rl + dl, maxd, n) if maxd and (dh.get(2, 1) or maxr) else None
                if maxr: reps = _rle(raw, 0, rl, maxr.bit_length(), n)
                data, p = raw[rl + dl:], 0
                if dh.get(7, True): data = _decompress(codec, data, ph[2] - rl - dl)
            if not maxr and n > nrows: raise ParquetError("page holds more values than its row group")
            whole = True
            if pcache is None and not maxr and enc != 9 and first + k > ends[-1]:   # only a prefix is wanted
                n = k = ends[-1] - first; whole = False
                if defs is not None: defs = defs[:n]
            if defs is not None and len(defs) < n or maxr and len(reps) < n:
                raise ParquetError("page has fewer levels than its header's %d values" % n)
            present = n if defs is None else sum(defs) if maxd == 1 else defs.count(maxd)
            is_ix = enc in (2, 8)
            if is_ix:                           # dictionary indices: mapped after slicing
                if dictionary is None: raise ValueError("dictionary page missing")
                vals = _rle(data, p + 1, len(data), data[p], present)
            else:
                vals = _values(data, p, col, enc, present, dh[1])   # cap: the page's value count
                if mode != 1 or not col.tc:
                    if isinstance(vals, array): vals = vals.tolist()
                    if col.conv: vals = col.convert(vals)
            if len(vals) < present: raise ParquetError("page has %d values, expected %d" % (len(vals), present))
            plain_pages = plain_pages or (mode == 2 and not is_ix)
            if maxr:
                if is_ix: vals, is_ix = list(map(look, vals)), False
                vals = _lists(vals, defs, reps, col.rep_defs, maxd, stack)
            elif defs is not None and present < n:
                if marks:
                    if is_ix: vals, is_ix = list(map(look, vals)), False
                    vals = _expand_nulls(vals, defs, maxd)
                else: vals = _expand(vals, defs, maxd, -1 if is_ix else None)
            if first == -1: first, k = row, len(vals); row += k
            if pcache is not None and whole:
                pcache[key] = (first, vals, is_ix, k, list(stack) if maxr else None)
            for a, b in _overlaps(ranges, ends, first, first + k):
                v = vals if b - a == len(vals) else vals[a - first:b - first]
                if is_ix and mode != 2: v = list(map(look, v))
                out.append((a, v, is_ix and mode == 2))
        if preds is None and sum(len(v) for a, v, _ in out) != nsel:   # every requested row must be there
            raise ParquetError("column chunk yields %d of the %d requested rows" % (sum(len(v) for a, v, _ in out), nsel))
        if pcache:                              # batches move forward: drop pages already passed
            for key, e in pcache.items():
                if e[1] is not None and e[0] + e[3] <= ends[-1]: pcache[key] = (e[0], None, e[2], e[3], e[4])
        if mode == 2 and not plain_pages and dictionary is not None:
            return [(a, v) for a, v, _ in out], dictionary
        if mode == 2 and dictionary is not None:    # chunk fell back to plain pages part-way
            return [(a, list(map(look, v)) if ix else v) for a, v, ix in out]
        return [(a, v) for a, v, _ in out]

    # -- filters --

    def _normalize(self, filters):
        if not filters: return []
        if isinstance(filters[0], tuple) and isinstance(filters[0][0], str): filters = [filters]
        dnf = []
        for conj in filters:
            terms = []
            for name, op, x in conj:
                if self.columns[self._find(name)].max_rep: raise NotImplementedError("filter on list column %r" % name)
                if op not in _OPS and op not in ("in", "not in"): raise ValueError("bad op %r" % op)
                if op in ("in", "not in"): x = frozenset(x)
                terms.append((self._find(name), op, x))
            dnf.append(terms)
        return dnf

    def _stat_literal(self, c, x):              # string literals compare with byte stats
        if not self.columns[c].is_str: return x
        if isinstance(x, frozenset): return frozenset(v.encode() if isinstance(v, str) else v for v in x)
        return x.encode() if isinstance(x, str) else x

    def _rg_may_match(self, i, dnf):
        rg = self._rgs[i]
        for conj in dnf:
            ok = True
            for c, op, x in conj:
                lo, hi, nulls = self._chunk_stats(rg, c)
                if not _can_match(op, self._stat_literal(c, x), lo, hi, nulls, rg[3], self.columns[c].isfloat):
                    ok = False; break
            if ok: return True
        return False

    def _select(self, i, dnf, cols=()):
        """Rows of row group i matching dnf, as ranges."""
        full, result, plans = [(0, self._rgs[i][3])], [], []
        for conj in dnf:
            bycol = {}
            for c, op, x in conj: bycol.setdefault(c, []).append((op, x))
            cand = full
            for c, preds in bycol.items():      # page index pruning (no data read)
                cand = _isect(cand, self._page_ranges(i, c, [(op, self._stat_literal(c, x)) for op, x in preds]))
                if not cand: break
            plans.append((bycol, cand))
        if hasattr(self._src, "prefetch"):      # remote: fetch candidate pages of every column at once
            cand = _union([r for _, cd in plans for r in cd])
            fc = {c for b, _ in plans for c in b}   # filter columns always; others only page-precise
            need = fc | {c for c in cols if (i, c, 0) in self._pi}
            self._src.prefetch([s for c in need for s in self._spans(i, c, cand)])
        for bycol, cand in plans:
            for c, preds in bycol.items():      # exact evaluation, narrowing as we go
                if not cand: break
                hits = []
                for a, vals in self._read_chunk(i, c, cand, 0, preds):
                    m = [True] * len(vals)
                    for op, x in preds: m = list(map(_op.and_, m, _mask(vals, op, x, self.columns[c].isfloat)))
                    if all(m): hits.append((a, a + len(vals))); continue
                    for r in compress(range(a, a + len(vals)), m):
                        if hits and hits[-1][1] == r: hits[-1] = (hits[-1][0], r + 1)
                        else: hits.append((r, r + 1))
                cand = _union(hits)
            result += cand
        return _union(result)

    # -- public reading API --

    def _plan(self, columns):
        names = list(columns) if columns is not None else self.column_names
        plan = []
        for n in names:
            node = self._nodes.get(n)
            if node is None: raise KeyError("no column %r" % n)
            flat = node.leaf is not None and not node.repeated and self.columns[node.leaf].max_rep == 0
            plan.append((n, node, flat, _leaves(node)))
        return plan

    def _iter(self, columns, filters, row_groups, arrays, dictionaries, limit, batch_size):
        plan, dnf = self._plan(columns), self._normalize(filters)
        cols = list(dict.fromkeys(c for _, _, _, ls in plan for c in ls))
        rgs = [i for i in (range(len(self._rgs)) if row_groups is None else row_groups)
               if self._rgs[i][3] and (not dnf or self._rg_may_match(i, dnf))]
        fcols = {c for conj in dnf for c, _, _ in conj}
        if dnf: self._load_page_index(rgs, [(c, k) for c in fcols for k in (0, 1)] + [(c, 0) for c in cols])
        for i in rgs:
            if limit is not None and limit <= 0: return
            sel = self._select(i, dnf, cols) if dnf else [(0, self._rgs[i][3])]
            if limit is not None: sel = _take(sel, limit)
            if not sel: continue
            dcache = {} if batch_size else None     # page/dictionary cache shared by the batches
            for part in _split(sel, batch_size) if batch_size else [sel]:
                if hasattr(self._src, "prefetch"):
                    self._src.prefetch([s for c in cols for s in self._spans(i, c, part)])
                yield self._read_rows(i, plan, part, arrays, dictionaries, dcache)
                if limit is not None: limit -= sum(b - a for a, b in part)
            if hasattr(self._src, "clear"): self._src.clear()

    def _read_rows(self, i, plan, sel, arrays, dictionaries, dcache):
        out = {}
        for name, node, flat, leaves in plan:
            if not flat:                        # nested: assemble from the leaves
                data = {c: [x for a, v in self._read_chunk(i, c, sel, marks=True, dcache=dcache) for x in v] for c in leaves}
                k = node.r - 1 if node.repeated else node.r
                try: out[name] = [_deep_fin(x, k) for x in _value(node, data)]
                except (IndexError, TypeError, AttributeError) as e:   # leaves disagree: corrupt levels
                    raise ParquetError("inconsistent nested column %r in row group %d: %s" % (name, i, e)) from e
                continue
            c = node.leaf; col = self.columns[c]
            r = self._read_chunk(i, c, sel, 2 if dictionaries else 1 if arrays and col.tc else 0, dcache=dcache)
            if isinstance(r, tuple):            # (index pieces, dictionary)
                idx = array("i")
                for a, v in r[0]: idx.fromlist(v)
                out[name] = DictionaryColumn(idx, r[1])
            else: out[name] = _concat(col, [v for a, v in r], arrays)
        return out

    def iter_row_groups(self, columns=None, filters=None, *, row_groups=None, arrays=False, dictionaries=False, limit=None):
        """Yield {column: values} for each row group with matching rows."""
        return self._iter(columns, filters, row_groups, arrays, dictionaries, limit, None)

    def iter_batches(self, columns=None, filters=None, *, batch_size=65536, row_groups=None,
                     arrays=False, dictionaries=False, limit=None):
        """Yield {column: values} for at most batch_size rows at a time (bounded memory)."""
        return self._iter(columns, filters, row_groups, arrays, dictionaries, limit, batch_size)

    def head(self, n=10, columns=None, filters=None):
        """The first n (matching) rows, reading as little as possible."""
        return self.read(columns, filters, limit=n)

    def read(self, columns=None, filters=None, *, row_groups=None, arrays=False, dictionaries=False, limit=None):
        """Read columns (all by default) of the rows matching filters into a dict."""
        plan = self._plan(columns)
        parts = {n: [] for n, _, _, _ in plan}
        for rg in self._iter([n for n, _, _, _ in plan], filters, row_groups, arrays, dictionaries, limit, None):
            for n in parts: parts[n].append(rg[n])
        out = {}
        for n, node, flat, _ in plan:
            ps, col = parts[n], self.columns[node.leaf] if flat else None
            if len(ps) == 1: out[n] = ps[0]
            elif ps and all(isinstance(p, DictionaryColumn) for p in ps): out[n] = _merge_dicts(ps)
            else: out[n] = _concat(col, [p.to_list() if isinstance(p, DictionaryColumn) else p for p in ps], arrays)
        return out

    def rows(self, columns=None, filters=None, *, limit=None, batch_size=65536):
        """Yield one tuple per matching row, holding at most batch_size rows in memory."""
        names = list(columns) if columns is not None else self.column_names
        for b in self.iter_batches(names, filters, batch_size=batch_size, limit=limit):
            yield from zip(*(b[n] for n in names))

def _take(ranges, k):                           # the first k rows of ranges
    out = []
    for a, b in ranges:
        if k <= 0: break
        out.append((a, min(b, a + k))); k -= b - a
    return out

def _split(ranges, size):                       # ranges cut into pieces of at most size rows
    out, cur, room = [], [], size
    for a, b in ranges:
        while a < b:
            e = min(b, a + room); cur.append((a, e)); room -= e - a; a = e
            if not room: out.append(cur); cur, room = [], size
    if cur: out.append(cur)
    return out

def _concat(col, pieces, arrays):
    if len(pieces) == 1 and isinstance(pieces[0], list) and not (arrays and col is not None and col.tc):
        return pieces[0]
    if arrays and col is not None and col.tc:
        out = array(col.tc)
        try:
            for v in pieces:
                if isinstance(v, array): out += v
                else: out.fromlist(v)           # TypeError if it holds a None
            return out
        except TypeError: pass                  # nulls: fall back to a list
    out = []
    for v in pieces: out += v
    return out

def _merge_dicts(parts):
    where, base, idx = {}, [], array("i")
    for p in parts:
        remap = []
        for v in p.dictionary:
            k = where.get(v)
            if k is None: k = where[v] = len(base); base.append(v)
            remap.append(k)
        remap.append(-1)                        # nulls stay -1
        idx.fromlist(list(map(remap.__getitem__, p.indices)))
    return DictionaryColumn(idx, base)

def read(source, columns=None, filters=None, **kw):
    """Read a Parquet file into {column name: list of values}. source: a local path,
    bytes, a binary file object, or an HTTPSource."""
    return ParquetFile(source).read(columns, filters, **kw)

def open_url(url, headers=None, **kw):
    """ParquetFile for an http(s) URL, read with range requests. headers (e.g.
    Authorization) are sent only to that host; kw go to HTTPSource."""
    return ParquetFile(HTTPSource(url, headers, **kw))

# ==== Writing ====
#
# The writer covers the common cases and lays files out the way the reader
# above likes them: row groups of moderate size, small pages, min/max
# statistics, a page index (ColumnIndex + OffsetIndex), encoding stats and
# column orders (so pruning is trusted), dictionary encoding only where it
# pays, and an optional sort key recorded as sorting_columns. Data pages are
# v1, the most widely readable kind. Every column is nullable.

# -- Thrift compact protocol encoder. A struct is a list of (field id, type, value);
#    a list value is (element type, items). None values are skipped. --

_T_BOOL, _T_BYTE, _T_I32, _T_I64, _T_BIN, _T_LIST, _T_STRUCT = 1, 3, 5, 6, 8, 9, 12

def _wvar(out, n):
    while n > 0x7F: out.append((n & 0x7F) | 0x80); n >>= 7
    out.append(n)

def _wstruct(out, fields):
    last = 0
    for fid, t, v in fields:
        if v is None: continue
        ct = (1 if v else 2) if t == _T_BOOL else t
        d = fid - last
        if 0 < d <= 15: out.append(d << 4 | ct)
        else: out.append(ct); _wvar(out, (fid << 1) ^ (fid >> 63))
        last = fid
        if t != _T_BOOL: _wval(out, t, v)
    out.append(0)

def _wval(out, t, v):
    if t in (_T_I32, _T_I64): _wvar(out, (v << 1) ^ (v >> 63))
    elif t == _T_BIN:
        if isinstance(v, str): v = v.encode()
        _wvar(out, len(v)); out += v
    elif t == _T_STRUCT: _wstruct(out, v)
    elif t == _T_BYTE: out.append(v & 0xFF)
    elif t == _T_LIST:
        et, items = v; n = len(items)
        out.append(n << 4 | et if n < 15 else 0xF0 | et)
        if n >= 15: _wvar(out, n)
        for x in items:
            if et == _T_BOOL: out.append(1 if x else 2)
            else: _wval(out, et, x)
    else: raise ValueError("thrift type %d" % t)

def _thrift_bytes(fields):
    out = bytearray(); _wstruct(out, fields); return bytes(out)

# -- RLE / bit-packed hybrid encoder (levels and dictionary indices) --

def _bitpack(vals, w):
    """Pack values (a multiple of 8 of them) LSB first, w bits each."""
    if w == 0 or not vals: return b""
    if w in (8, 16, 32):
        a = array(_UINT[w], vals)
        if _BIG: a.byteswap()
        return a.tobytes()
    out, step = bytearray(), 256                # chunks keep the big-int work linear
    shifts = range(0, step * w, w)
    for i in range(0, len(vals), step):
        part = vals[i:i + step]; x = 0
        for s, v in zip(shifts, part): x |= v << s
        out += x.to_bytes(len(part) * w // 8, "little")
    return bytes(out)

def _rle_encode(vals, w):
    """RLE runs for repeats of 8 or more, bit-packed groups of 8 otherwise.

    Run boundaries are found with C-level map/compress, so the Python loop only
    visits the long runs; everything between them is bit-packed in one go."""
    n = len(vals)
    if not n: return b""
    out, nb = bytearray(), (w + 7) // 8
    if vals[0] == vals[-1] and vals.count(vals[0]) == n:      # one value throughout
        if n >= 8 or w == 0:
            _wvar(out, n << 1); out += vals[0].to_bytes(nb, "little"); return bytes(out)
    starts = [0]; starts += compress(range(1, n), map(_op.ne, vals[1:], vals[:-1])); starts.append(n)
    lens = list(map(_op.sub, starts[1:], starts[:-1]))
    lit = 0                                                    # start of pending literal values
    for i in compress(range(len(lens)), map((7).__lt__, lens)):
        s, e = starts[i], starts[i + 1]
        s += -(s - lit) % 8                                    # complete the literal's last group of 8
        if e - s < 8: continue                                 # what is left is too short: stays literal
        if s > lit:
            _wvar(out, ((s - lit) // 8) << 1 | 1); out += _bitpack(vals[lit:s], w)
        _wvar(out, (e - s) << 1); out += vals[s].to_bytes(nb, "little")
        lit = e
    if lit < n:
        tail = list(vals[lit:]); tail += [0] * (-len(tail) % 8)  # padding at the end is allowed
        _wvar(out, (len(tail) // 8) << 1 | 1); out += _bitpack(tail, w)
    return bytes(out)

# -- Types: inference from Python values, or declared with a short string --

class _WType:
    """How one leaf column is stored: physical type, annotations, conversion."""
    def __init__(self, kind, ptype, type_length=None, logical=None, converted=None, scale=None,
                 precision=None, unit=None, utc=False, bits=64, signed=True):
        self.kind, self.ptype, self.type_length, self.logical = kind, ptype, type_length, logical
        self.converted, self.scale, self.precision = converted, scale, precision
        self.unit, self.utc, self.bits, self.signed = unit, utc, bits, signed
        self.tc = {1: "i", 2: "q", 4: "f", 5: "d"}.get(ptype)
        self.isfloat = ptype in (4, 5)

    def __repr__(self): return "<%s>" % self.kind

_UNIT = {"ms": (1, _dt.timedelta(milliseconds=1)), "us": (2, _dt.timedelta(microseconds=1)), "ns": (3, None)}

def _decimal_type(p, s):
    if not 0 < p <= 76 or not 0 <= s <= p: raise ValueError("decimal(%d,%d) not supported" % (p, s))
    lg = [(5, _T_STRUCT, [(1, _T_I32, s), (2, _T_I32, p)])]
    if p <= 9: return _WType("decimal", 1, logical=lg, converted=5, scale=s, precision=p, bits=32)
    if p <= 18: return _WType("decimal", 2, logical=lg, converted=5, scale=s, precision=p)
    n = 1
    while 10 ** p > 1 << (8 * n - 1): n += 1
    return _WType("decimal", 7, type_length=n, logical=lg, converted=5, scale=s, precision=p)

def _time_logical(code, utc, unit):
    return [(code, _T_STRUCT, [(1, _T_BOOL, bool(utc)), (2, _T_STRUCT, [(_UNIT[unit][0], _T_STRUCT, [])])])]

def _split_top(s):
    """Split s at commas that are not inside <>, () or []."""
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch in "<([": depth += 1
        elif ch in ">)]": depth -= 1
        if ch == "," and depth == 0: out.append(cur); cur = ""
        else: cur += ch
    if depth: raise ValueError("unbalanced brackets in type %r" % s)
    return out + [cur]

def _named_type(spec):
    """'int32', 'string', 'decimal(10,2)', 'timestamp[ms,UTC]', 'list<string>',
    'struct<name: string, tags: list<string>>', ..."""
    if not isinstance(spec, str): raise TypeError("type names are strings, got %r" % (spec,))
    raw = spec.strip()
    if raw.lower().startswith("list<") and raw.endswith(">"): return ("list", _named_type(raw[5:-1]))
    if raw.lower().startswith("struct<") and raw.endswith(">"):
        fields = []
        for f in _split_top(raw[7:-1]):
            name, sep, t = f.partition(":")
            if not sep or not name.strip(): raise ValueError("struct field %r needs the form name: type" % f.strip())
            fields.append((name.strip(), _named_type(t)))
        if len({n for n, _ in fields}) < len(fields): raise ValueError("duplicate field names in %r" % raw)
        return ("struct", fields)
    s = raw.lower().replace(" ", "")
    m = _re.fullmatch(r"decimal\((\d+),(\d+)\)", s)
    if m: return _decimal_type(int(m[1]), int(m[2]))
    m = _re.fullmatch(r"timestamp(?:\[(ms|us|ns)(,utc)?\])?", s)
    if m:
        unit, utc = m[1] or "us", bool(m[2])
        return _WType("timestamp", 2, logical=_time_logical(8, utc, unit),
                      converted={"ms": 9, "us": 10}.get(unit) if utc else None, unit=unit, utc=utc)
    m = _re.fullmatch(r"time(?:\[(ms|us|ns)\])?", s)
    if m:
        unit = m[1] or "us"
        return _WType("time", 1 if unit == "ms" else 2, logical=_time_logical(7, False, unit), unit=unit,
                      bits=32 if unit == "ms" else 64)
    ints = {"int8": (8, True), "int16": (16, True), "int32": (32, True), "int64": (64, True),
            "uint8": (8, False), "uint16": (16, False), "uint32": (32, False), "uint64": (64, False)}
    if s in ints:
        bits, signed = ints[s]
        lg = [(10, _T_STRUCT, [(1, _T_BYTE, bits), (2, _T_BOOL, signed)])] if (bits, signed) != (64, True) and (bits, signed) != (32, True) else None
        conv = {8: 15, 16: 16, 32: 17, 64: 18}[bits] if signed else {8: 11, 16: 12, 32: 13, 64: 14}[bits]
        return _WType(s, 2 if bits == 64 else 1, logical=lg, converted=conv if lg else None, bits=bits, signed=signed)
    simple = {
        "bool": lambda: _WType("bool", 0), "boolean": lambda: _WType("bool", 0),
        "float32": lambda: _WType("float32", 4), "float": lambda: _WType("float64", 5),
        "float64": lambda: _WType("float64", 5), "double": lambda: _WType("float64", 5),
        "string": lambda: _WType("string", 6, logical=[(1, _T_STRUCT, [])], converted=0),
        "str": lambda: _WType("string", 6, logical=[(1, _T_STRUCT, [])], converted=0),
        "json": lambda: _WType("json", 6, logical=[(12, _T_STRUCT, [])], converted=19),
        "binary": lambda: _WType("binary", 6), "bytes": lambda: _WType("binary", 6),
        "date": lambda: _WType("date", 1, logical=[(6, _T_STRUCT, [])], converted=6, bits=32),
        "uuid": lambda: _WType("uuid", 7, type_length=16, logical=[(14, _T_STRUCT, [])]),
        "null": lambda: _WType("null", 1, logical=[(11, _T_STRUCT, [])], bits=32),
    }
    if s in simple: return simple[s]()
    raise ValueError("unknown column type %r" % spec)

def _infer(values, name):
    """Type of a column from its values: a _WType, ("list", t) or ("struct", [(name, t)])."""
    exact = {bool: "bool", int: "int", float: "float", str: "string", bytes: "binary", bytearray: "binary",
             memoryview: "binary", _dt.date: "date", _dt.time: "time", _dec.Decimal: "decimal",
             uuid.UUID: "uuid", dict: "struct", list: "list", tuple: "list", array: "list"}
    kinds = set()
    for t in set(map(type, values)):                 # one C-level pass over the values
        if t is type(None): continue
        if t in exact: kinds.add(exact[t]); continue
        if t is _dt.datetime or issubclass(t, _dt.datetime):
            kinds.update("tsz" if v.tzinfo is not None else "ts" for v in values if type(v) is t)
            continue
        for base, k in (((bytes, bytearray, memoryview), "binary"), (_dt.date, "date"), (_dt.time, "time"),
                        (_dec.Decimal, "decimal"), (uuid.UUID, "uuid"), (dict, "struct"),
                        ((list, tuple, array), "list"), (bool, "bool"), (int, "int"), (float, "float"), (str, "string")):
            if issubclass(t, base): kinds.add(k); break
        else: raise TypeError("column %r: cannot store %s values" % (name, t.__name__))
    if kinds == {"int", "float"}: kinds = {"float"}
    if len(kinds) > 1:
        raise TypeError("column %r mixes %s values; convert them or pass types={%r: ...}" % (name, " and ".join(sorted(kinds)), name))
    k = kinds.pop() if kinds else "null"
    if k == "int":
        nn = values if None not in values else [v for v in values if v is not None]
        lo, hi = min(nn), max(nn)
        if -(1 << 63) <= lo and hi < 1 << 63: return _named_type("int64")
        if lo >= 0 and hi < 1 << 64: return _named_type("uint64")
        raise ValueError("column %r has integers beyond 64 bits; use types={%r: 'decimal(p,0)'}" % (name, name))
    if k == "decimal":
        s = p = 1
        for v in values:
            if v is None: continue
            if not v.is_finite(): raise ValueError("column %r: cannot store %s" % (name, v))
            sign, digits, exp = v.as_tuple()
            s = max(s if s else 0, -exp)
        s = max(0, s)
        for v in values:
            if v is not None: p = max(p, len(str(abs(int(v.scaleb(s, _DEC))))))
        return _decimal_type(max(p, s, 1), s)
    if k == "list":
        return ("list", _infer([x for v in values if v is not None for x in v], name + ".element"))
    if k == "struct":
        keys = list(dict.fromkeys(key for v in values if v is not None for key in v))
        for key in keys:
            if not isinstance(key, str): raise TypeError("column %r: struct keys must be strings" % name)
        return ("struct", [(key, _infer([v.get(key) for v in values if v is not None], name + "." + key)) for key in keys])
    return _named_type({"ts": "timestamp", "tsz": "timestamp[us,utc]", "float": "float64"}.get(k, k))

# -- Conversion of Python values to physical values, and their encodings --

_EPOCH_TS = {False: _EPOCH, True: _EPOCH_UTC}

_BYTES_LIKE = (bytes, bytearray, memoryview)

def _check(vals, types, what):
    """Every value an instance of types (exact type matches take the fast path)."""
    if not all(map(isinstance, vals, repeat(types))):
        bad = next(v for v in vals if not isinstance(v, types))
        raise TypeError("%s value %r in a %s column" % (type(bad).__name__, bad, what))

def _to_phys(t, vals, name):
    """Python values (no None) -> the physical values the encoders take, with strict type checks."""
    k = t.kind
    try:
        if k == "bool": _check(vals, bool, k); return vals
        if t.tc and k not in ("date", "timestamp", "time", "decimal", "null"):
            if k == "float32" or k == "float64":
                _check(vals, (float, int), k)
                if any(map(isinstance, vals, repeat(bool))): raise TypeError("bool value in a %s column" % k)
                out = list(map(float, vals))
                if k == "float32" and out:
                    big = [v for v in out if abs(v) > 3.4028234663852886e38 and v == v and abs(v) != float("inf")]
                    if big: raise OverflowError("%r does not fit in float32" % big[0])
                return out
            _check(vals, int, k)
            if any(map(isinstance, vals, repeat(bool))): raise TypeError("bool value in a %s column" % k)
            lo, hi = (-(1 << t.bits - 1), 1 << t.bits - 1) if t.signed else (0, 1 << t.bits)
            out = list(map(int, vals))              # int subclasses (IntEnum) become plain ints
            if out and (min(out) < lo or max(out) >= hi):
                raise ValueError("value out of range for %s" % k)
            if not t.signed and t.bits >= 32:     # stored as the signed type of the same width
                half = 1 << t.bits - 1; full = 1 << t.bits
                out = [v - full if v >= half else v for v in out]
            return out
        if k in ("string", "json"): _check(vals, str, k); return [v.encode("utf-8") for v in vals]
        if k == "binary": _check(vals, _BYTES_LIKE, k); return list(map(bytes, vals))
        if k == "date":
            _check(vals, _dt.date, k)
            if any(isinstance(v, _dt.datetime) for v in vals): raise TypeError("datetime value in a date column")
            return [v.toordinal() - 719163 for v in vals]
        if k == "timestamp":
            _check(vals, _dt.datetime, k)
            if any((v.tzinfo is not None) != t.utc for v in vals):
                raise TypeError("timestamps must all be %s" % ("timezone-aware" if t.utc else "naive"))
            e = _EPOCH_TS[t.utc]
            if t.unit == "ns": return [(v - e) // _dt.timedelta(microseconds=1) * 1000 for v in vals]
            return [(v - e) // _UNIT[t.unit][1] for v in vals]
        if k == "time":
            _check(vals, _dt.time, k)
            f = {"ms": 1000, "us": 1, "ns": 1}[t.unit]; m = 1000 if t.unit == "ns" else 1
            return [((v.hour * 60 + v.minute) * 60 + v.second) * 1000000 // f * m + v.microsecond // f * m for v in vals]
        if k == "decimal":
            _check(vals, (_dec.Decimal, int), k)
            out = []
            for v in vals:
                if isinstance(v, bool): raise TypeError("bool value in a decimal column")
                n = int(_dec.Decimal(v).scaleb(t.scale, _DEC))
                if _dec.Decimal(n).scaleb(-t.scale, _DEC) != v: raise ValueError("%s has more than %d decimal places" % (v, t.scale))
                if len(str(abs(n))) > t.precision: raise ValueError("%s exceeds precision %d" % (v, t.precision))
                out.append(n)
            return out
        if k == "uuid":
            _check(vals, (uuid.UUID,) + _BYTES_LIKE, k)
            out = [v.bytes if isinstance(v, uuid.UUID) else bytes(v) for v in vals]
            if any(len(v) != 16 for v in out): raise ValueError("uuid bytes must be 16 long")
            return out
        if k == "null":
            if vals: raise TypeError("a value in a column that holds only nulls (its type was inferred from a "
                                     "batch with no values; give its type in types=)")
            return []
    except (TypeError, ValueError, AttributeError, OverflowError) as e:
        cls = type(e) if type(e) in (TypeError, ValueError, OverflowError) else ValueError if isinstance(e, ValueError) else TypeError
        raise cls("column %r (%s): %s" % (name, k, e)) from None
    raise TypeError("column %r: unsupported type %s" % (name, k))

_P32 = struct.Struct("<I").pack
_PD = struct.Struct("<d").pack

def _plain_encode(t, phys):
    if t.ptype == 0:                            # booleans, bit-packed LSB first
        n = len(phys)
        x = int("".join("1" if v else "0" for v in reversed(phys)) or "0", 2)
        return x.to_bytes((n + 7) // 8, "little")
    if t.tc:
        a = array(t.tc, phys)
        if _BIG: a.byteswap()
        return a.tobytes()
    if t.ptype == 6: return b"".join(chain.from_iterable(zip(map(_P32, map(len, phys)), phys)))
    if t.ptype == 7:
        if t.kind == "decimal": return b"".join(v.to_bytes(t.type_length, "big", signed=True) for v in phys)
        if any(len(v) != t.type_length for v in phys): raise ValueError("value of the wrong length for %s" % t.kind)
        return b"".join(phys)
    raise ValueError("cannot encode %s" % t.kind)

def _stat_key(t, v):                            # values in the column's sort order
    if not t.signed and t.bits >= 32 and v < 0: return v + (1 << t.bits)
    return v

def _stat_bytes(t, v):
    if t.ptype == 0: return b"\x01" if v else b"\x00"
    if t.tc:
        tc = t.tc.upper() if not t.signed and t.bits >= 32 else t.tc
        return struct.pack("<" + {"i": "i", "q": "q", "f": "f", "d": "d", "I": "I", "Q": "Q"}[tc], v)
    if t.kind == "decimal" and t.ptype == 7: return v.to_bytes(t.type_length, "big", signed=True)
    return v

def _minmax(t, phys):
    """(min, max) in the column's order as raw statistic bytes, with exactness flags;
    None if there is nothing to describe (no values, or only NaN)."""
    if t.kind == "null" or not phys: return None
    if t.isfloat:
        vals = [v for v in phys if v == v]
        if not vals: return None
        lo, hi = min(vals), max(vals)
        if lo == 0: lo = -0.0                   # the spec's rule for signed zeros
        if hi == 0: hi = 0.0
    elif not t.signed and t.bits >= 32:
        keys = [_stat_key(t, v) for v in phys]; lo, hi = min(keys), max(keys)
        return _stat_bytes(t, lo), _stat_bytes(t, hi), True, True
    else: lo, hi = min(phys), max(phys)
    lo_b, hi_b, lo_x, hi_x = _stat_bytes(t, lo), _stat_bytes(t, hi), True, True
    if t.ptype == 6 and len(lo_b) > 64: lo_b, lo_x = lo_b[:64], False         # a prefix is <= the value
    if t.ptype == 6 and len(hi_b) > 64:
        p = hi_b[:64].rstrip(b"\xff")                                         # a bigger prefix is >= the value
        if p: hi_b, hi_x = p[:-1] + bytes([p[-1] + 1]), False
    return lo_b, hi_b, lo_x, hi_x

# -- Schema tree for writing --

class _WNode:
    __slots__ = ("name", "kind", "children", "type", "leaf", "leaves", "d", "r")

def _build_nodes(name, spec, d, r, leaves):
    """Node for spec (a _WType, ("list", t) or ("struct", [...])); all fields optional."""
    n = _WNode(); n.name, n.d, n.r = name, d + 1, r
    if isinstance(spec, _WType):
        n.kind, n.type, n.children, n.leaf = "prim", spec, [], len(leaves); leaves.append(n)
        n.leaves = [n.leaf]
    elif spec[0] == "list":
        n.kind, n.type = "list", None
        el = _build_nodes("element", spec[1], d + 2, r + 1, leaves)   # optional group, repeated "list", element
        n.children, n.leaves = [el], el.leaves
    else:
        n.kind, n.type = "struct", None
        n.children = [_build_nodes(k, v, d + 1, r, leaves) for k, v in spec[1]]
        n.leaves = [l for c in n.children for l in c.leaves]
    return n

def _schema_elements(node, out):
    if node.kind == "prim":
        t = node.type
        out.append([(1, _T_I32, t.ptype), (2, _T_I32, t.type_length), (3, _T_I32, 1), (4, _T_BIN, node.name),
                    (6, _T_I32, t.converted), (7, _T_I32, t.scale), (8, _T_I32, t.precision),
                    (10, _T_STRUCT, t.logical)])
    elif node.kind == "list":
        out.append([(3, _T_I32, 1), (4, _T_BIN, node.name), (5, _T_I32, 1), (6, _T_I32, 3), (10, _T_STRUCT, [(3, _T_STRUCT, [])])])
        out.append([(3, _T_I32, 2), (4, _T_BIN, "list"), (5, _T_I32, 1)])
        _schema_elements(node.children[0], out)
    else:
        out.append([(3, _T_I32, 1), (4, _T_BIN, node.name), (5, _T_I32, len(node.children))])
        for c in node.children: _schema_elements(c, out)

def _leaf_paths(node, prefix, out):
    if node.kind == "prim": out.append(prefix + [node.name])
    elif node.kind == "list": _leaf_paths(node.children[0], prefix + [node.name, "list"], out)
    else:
        for c in node.children: _leaf_paths(c, prefix + [node.name], out)

def _shred(node, values, sinks):
    """Split a column of nested values into (definition levels, repetition levels,
    values) per leaf, plus where each row starts."""
    def emit(n, v, r, d):
        if v is None:
            for l in n.leaves: s = sinks[l]; s[0].append(d); s[1].append(r)
            return
        if n.kind == "prim":
            s = sinks[n.leaf]; s[0].append(n.d); s[1].append(r); s[2].append(v)
        elif n.kind == "struct":
            if not isinstance(v, dict): raise TypeError("field %r: expected a dict, got %s" % (n.name, type(v).__name__))
            for c in n.children: emit(c, v.get(c.name), r, n.d)
        else:
            if isinstance(v, (str, bytes, dict)): raise TypeError("field %r: expected a list, got %s" % (n.name, type(v).__name__))
            if len(v) == 0:
                for l in n.leaves: s = sinks[l]; s[0].append(n.d); s[1].append(r)
                return
            el, rr = n.children[0], n.children[0].r
            for i, e in enumerate(v): emit(el, e, r if i == 0 else rr, n.d + 1)
    for v in values:
        for l in node.leaves: s = sinks[l]; s[3].append(len(s[0]))
        emit(node, v, 0, 0)

# -- Compression for writing --

def _compressor(name, level):
    name = (name or "none").lower()
    if name in ("none", "uncompressed"): return 0, None
    if name == "gzip":                          # compressobj: zlib.compress has no wbits before 3.11
        lv = 6 if level is None else level
        def gz(b):
            c = zlib.compressobj(lv, zlib.DEFLATED, 31); return c.compress(b) + c.flush()
        return 2, gz
    if name == "zstd":
        try:
            from compression.zstd import compress as c
            return 6, lambda b: c(b, 3 if level is None else level)
        except ImportError: pass
        if _cramjam: return 6, lambda b: bytes(_cramjam.zstd.compress(b, 3 if level is None else level))
        zs = _optional("zstandard")
        if zs: return 6, lambda b: zs.ZstdCompressor(level=3 if level is None else level).compress(b)
        raise ImportError("zstd writing needs Python 3.14+, cramjam or zstandard; use compression='gzip'")
    if name == "snappy":
        if _cramjam: return 1, lambda b: bytes(_cramjam.snappy.compress_raw(b))
        if _pysnappy: return 1, _pysnappy.compress
        raise ImportError("snappy writing needs cramjam or python-snappy; use 'zstd' or 'gzip'")
    if name in ("lz4", "lz4_raw"):
        if _cramjam: return 7, lambda b: bytes(_cramjam.lz4.compress_block(b, store_size=False))
        if _lz4: return 7, lambda b: _lz4.block.compress(b, store_size=False)
        raise ImportError("lz4 writing needs cramjam or lz4")
    if name == "brotli":
        if _cramjam: return 4, lambda b: bytes(_cramjam.brotli.compress(b, 5 if level is None else level))
        if _brotli: return 4, lambda b: _brotli.compress(b, quality=5 if level is None else level)
        raise ImportError("brotli writing needs cramjam or brotli")
    raise ValueError("unknown compression %r" % name)

# -- The writer --

def _columns(data):
    """dict of columns, or a list of row dicts -> {name: list of values}."""
    if isinstance(data, dict):
        out = {}
        for k, v in data.items():
            if not isinstance(k, str): raise TypeError("column names must be strings")
            if not isinstance(v, (list, array, DictionaryColumn)):
                v = v.tolist() if hasattr(v, "tolist") else list(v)   # numpy/pandas: Python scalars
            out[k] = v                          # lists, array.array and DictionaryColumn are taken as they are
        n = {len(v) for v in out.values()}
        if len(n) > 1: raise ValueError("columns have different lengths: %s" % {k: len(v) for k, v in out.items()})
        return out
    rows = list(data)
    if rows and not all(isinstance(r, dict) for r in rows): raise TypeError("expected a dict of columns or a list of row dicts")
    keys = list(dict.fromkeys(k for r in rows for k in r))
    return {k: [r.get(k) for r in rows] for k in keys}

_ARRAY_KIND = {"b": "int8", "B": "uint8", "h": "int16", "H": "uint16", "f": "float32", "d": "float64"}

def _array_type(tc, name):
    """Column type for an array.array, from its typecode."""
    if tc in _ARRAY_KIND: return _named_type(_ARRAY_KIND[tc])
    if tc in "ilqILQ":
        bits = 8 * array(tc).itemsize
        return _named_type(("int%d" if tc.islower() else "uint%d") % bits)
    raise TypeError("column %r: array typecode %r is not supported" % (name, tc))

def _array_direct(tc, t):
    """Can an array with this typecode be stored in column type t without checks?"""
    if t.kind in ("float32", "float64"): return tc == "f" or (tc == "d" and t.kind == "float64")
    if not t.kind.startswith(("int", "uint")) or tc not in "bBhHiIlLqQ": return False
    bits, signed = 8 * array(tc).itemsize, tc.islower()
    if not t.signed and t.bits >= 32: return False          # stored as the signed type: needs remapping
    if signed: return t.signed and bits <= t.bits
    return bits < t.bits if t.signed else bits <= t.bits

def _check_spec(spec, name):
    """A type given as an object rather than a name: a _WType, ("list", spec) or ("struct", [(name, spec)])."""
    if isinstance(spec, _WType): return spec
    if isinstance(spec, tuple) and len(spec) == 2 and spec[0] == "list": _check_spec(spec[1], name); return spec
    if isinstance(spec, tuple) and len(spec) == 2 and spec[0] == "struct" and isinstance(spec[1], list):
        for f in spec[1]:
            if not (isinstance(f, tuple) and len(f) == 2 and isinstance(f[0], str)): break
            _check_spec(f[1], name)
        else: return spec
    raise TypeError("types[%r]: expected a type name such as 'int64' or 'list<string>', got %r" % (name, spec))

def _value_bytes(t, phys):
    """Rough plain-encoded size of physical values (for row_group_bytes)."""
    if t.ptype == 6: return sum(map(len, phys)) + 4 * len(phys)
    if t.ptype == 7: return t.type_length * len(phys)
    if t.ptype == 0: return len(phys) // 8 + 1
    return len(phys) * (4 if t.ptype in (1, 4) else 8)

class ParquetWriter:
    """Writes a Parquet file in one or more batches.

        with ParquetWriter("out.parquet", sort_by="id") as w:
            w.write({"id": [3, 1, 2], "name": ["c", "a", "b"]})
            w.write(more_rows)                  # same columns

    The file appears only when the writer closes without an error (it is written
    to a temporary file and renamed). Types are inferred from the first batch with
    rows unless given in types, e.g. {"id": "int32", "price": "decimal(10,2)",
    "tags": "list<string>", "at": "timestamp[ms,UTC]",
    "user": "struct<name: string, roles: list<string>>"}. Give types for columns
    that may hold only nulls in the first batch.

    Columns may be lists (or tuples, or anything with tolist(), such as numpy
    arrays), array.array, or DictionaryColumn, so read(dictionaries=True) output
    writes straight back. Arrays skip the per-value checks; dictionary values are
    converted once per entry. (read(arrays=True) gives dates and timestamps as
    raw numbers: name their types when writing that output back.)

    Batches are buffered: rows go out in row groups of row_group_size rows, or
    fewer when they reach row_group_bytes (estimated, uncompressed), so many
    small batches still make full row groups. Data pages are about page_size
    bytes before compression and at most page_rows rows. Files carry statistics
    and a page index; zstd level 3 is the default codec (gzip if zstd is
    unavailable; compression_level=9 or more gives noticeably smaller files of
    long text for slower writes). Dictionary encoding is used for columns with
    many repeats (dictionary="auto" decides per column on the first row group
    and keeps that choice; or True/False, or a list of column names). sort_by
    sorts the buffered rows before each row group is written (NaN, then nulls,
    last) and records the order in the file: each row group is sorted, and the
    file as a whole is sorted when the rows arrive in one batch.

    Every batch is validated as it is written: a value of the wrong type or out
    of range raises TypeError/ValueError and the batch is undone, as is a batch
    whose row groups fail to write (for file objects that cannot seek, the
    writer refuses further batches instead)."""

    def __init__(self, dest, types=None, *, sort_by=None, compression="zstd", compression_level=None,
                 row_group_size=65536, row_group_bytes=128 << 20, page_size=65536, page_rows=20000,
                 dictionary="auto", metadata=None):
        if compression == "zstd" and _zstd is None and not _cramjam and not _optional("zstandard"):
            compression = "gzip"
        self.codec, self.compress = _compressor(compression, compression_level)
        if min(row_group_size, row_group_bytes, page_size, page_rows) < 1:
            raise ValueError("row_group_size, row_group_bytes, page_size and page_rows must be positive")
        self.row_group_bytes = row_group_bytes
        self.page_rows = page_rows
        if not (dictionary in (True, False, "auto") or isinstance(dictionary, (list, tuple, set, frozenset))):
            raise ValueError("dictionary must be True, False, 'auto' or a list of column names")
        self.types = {k: (_named_type(v) if isinstance(v, str) else _check_spec(v, k)) for k, v in (types or {}).items()}
        self.sort_by = [sort_by] if isinstance(sort_by, str) else list(sort_by or [])
        self.row_group_size, self.page_size, self.dictionary = row_group_size, page_size, dictionary
        self._dict_choice = {}
        self.metadata = dict(metadata or {})
        self.nodes = self.leaves = self._buf = self._empty = None
        self.row_groups, self.indexes, self.num_rows = [], [], 0
        self._tmp = None
        if isinstance(dest, (str, os.PathLike)):
            import tempfile
            path = os.path.abspath(os.fspath(dest))
            if os.path.isdir(path): raise IsADirectoryError("%s is a directory" % path)
            fd, self._tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix="." + os.path.basename(path) + ".", suffix=".tmp")
            self.f, self.dest = os.fdopen(fd, "wb"), path
            # a writer dropped without close() or abort() still removes its temporary file
            self._finalizer = weakref.finalize(self, _discard, self.f, self._tmp)
        elif hasattr(dest, "write"): self.f, self.dest = dest, None
        else: raise TypeError("dest must be a path or a binary file object")
        try: self._base = self.f.tell()          # lets a failed batch be cut off again
        except (OSError, AttributeError, ValueError): self._base = None
        self._broken = False
        self.f.write(b"PAR1"); self.pos = 4

    def __enter__(self): return self
    def __exit__(self, exc, *rest):
        if exc is None: self.close()
        else: self.abort()

    def _schema(self, cols):
        specs = []
        if self.nodes is not None: return
        for name, vals in cols.items():
            spec = self.types.get(name)
            if spec is None and isinstance(vals, array): spec = _array_type(vals.typecode, name)
            elif spec is None and isinstance(vals, DictionaryColumn): spec = _infer(list(vals.dictionary), name)
            if spec is None: spec = _infer(vals, name)
            specs.append((name, spec))
        unknown = set(self.types) - set(cols)
        if unknown: raise KeyError("types given for missing columns: %s" % sorted(unknown))
        leaves, paths = [], []
        nodes = [_build_nodes(name, spec, 0, 0, leaves) for name, spec in specs]
        for n in nodes: _leaf_paths(n, [], paths)
        dotted = [".".join(p) for p in paths]
        if len(set(dotted)) < len(dotted):         # e.g. a column "a.b" next to a struct "a" with a field "b"
            raise ValueError("column paths are ambiguous: %s" % sorted({d for d in dotted if dotted.count(d) > 1}))
        for s in self.sort_by:
            n = next((n for n in nodes if n.name == s), None)
            if n is None or n.kind != "prim": raise ValueError("sort_by %r must be a top-level, non-nested column" % s)
        self.leaves, self.nodes, self.paths = leaves, nodes, paths

    def write(self, data):
        """Append a batch: a dict of columns (lists, array.array, DictionaryColumn, or
        read() output) or a list of row dicts. Rows are buffered and written out in
        row groups of row_group_size rows (or row_group_bytes), not one per batch."""
        if self.f is None: raise ValueError("the writer is closed")
        if self._broken: raise ValueError("an earlier batch failed part way and could not be undone; abort() and start again")
        cols = _columns(data)
        state = (self.pos, len(self.row_groups), len(self.indexes), self.num_rows, dict(self._dict_choice), self.nodes,
                 self._buf_mark())
        try:
            self._buffer(cols)
            self._flush(final=False)
        except BaseException:
            self._undo(state); raise

    def _undo(self, state):
        """Forget a failed batch: cut its bytes off the file, drop its buffered rows
        and restore the bookkeeping."""
        pos, nrg, nidx, nrows, choice, nodes, mark = state
        if self.pos != pos:
            try:
                if self._base is None: raise OSError
                self.f.seek(self._base + pos); self.f.truncate()
            except (OSError, AttributeError, ValueError):
                self._broken = True; return
        self.pos, self.num_rows, self._dict_choice = pos, nrows, choice
        del self.row_groups[nrg:]; del self.indexes[nidx:]
        if nodes is None: self.nodes = self.leaves = None; self._buf = None
        elif self._buf is not None: self._buf_truncate(mark)

    # -- the row buffer: per leaf (defs, reps, physical values, row starts), plus the
    #    Python values of the sort_by columns, the row count and an estimated size --

    def _new_buf(self):
        return {"leaves": [([], [], [], []) for _ in self.leaves], "keys": {s: [] for s in self.sort_by},
                "rows": 0, "bytes": 0}

    def _buf_mark(self):
        b = self._buf
        if b is None: return None
        return ([tuple(map(len, s)) for s in b["leaves"]], {k: len(v) for k, v in b["keys"].items()}, b["rows"], b["bytes"])

    def _buf_truncate(self, mark):
        b = self._buf
        if mark is None: self._buf = self._new_buf(); return
        lens, klens, b["rows"], b["bytes"] = mark
        for s, ls in zip(b["leaves"], lens):
            for lst, n in zip(s, ls): del lst[n:]
        for k, n in klens.items(): del b["keys"][k][n:]

    def _buffer(self, cols):
        """Validate a batch and convert it to levels and physical values. Nothing is
        buffered unless the whole batch converts."""
        n = len(next(iter(cols.values()))) if cols else 0
        if self.nodes is None:
            if not n and cols:                  # nothing to infer types from yet: wait for rows
                if self._empty is None: self._empty = cols
                elif list(cols) != list(self._empty):
                    raise ValueError("batch columns %s differ from the file's %s" % (list(cols), list(self._empty)))
                return
            if self._empty is not None and list(cols) != list(self._empty):
                raise ValueError("batch columns %s differ from the file's %s" % (list(cols), list(self._empty)))
            self._schema(cols)
        if self._buf is None: self._buf = self._new_buf()
        names = [n.name for n in self.nodes]
        if list(cols) != names:
            raise ValueError("batch columns %s differ from the file's %s" % (list(cols), names))
        sinks = [([], [], [], []) for _ in self.leaves]
        for node in self.nodes:
            vals = cols[node.name]
            if node.kind == "prim":             # flat column: fast paths, no recursion
                s, t, name = sinks[node.leaf], node.type, node.name
                if isinstance(vals, array) and _array_direct(vals.typecode, t):
                    s[0].extend(repeat(1, n)); s[2].extend(vals.tolist())      # values are in range by construction
                elif isinstance(vals, DictionaryColumn) and None not in vals.dictionary:
                    idx = vals.indices
                    if len(idx) and (min(idx) < -1 or max(idx) >= len(vals.dictionary)):
                        raise ValueError("column %r: dictionary index out of range" % name)
                    dphys = _to_phys(t, list(vals.dictionary), name)            # each distinct value converted once
                    present = list(map((-1).__lt__, idx))
                    s[0].extend(present); s[2].extend(map(dphys.__getitem__, compress(idx, present)))
                else:
                    if not isinstance(vals, list): vals = list(vals.to_list() if isinstance(vals, DictionaryColumn) else vals)
                    present = list(map(_op.is_not, vals, repeat(None)))
                    s[0].extend(present); s[2].extend(_to_phys(t, list(compress(vals, present)), name))
                s[1].extend(repeat(0, n)); s[3].extend(range(n))
            else:
                vals = vals.to_list() if isinstance(vals, DictionaryColumn) else vals
                tmp = [([], [], [], []) for _ in self.leaves]
                _shred(node, vals, tmp)
                for l in node.leaves:
                    d, r, v, st = tmp[l]
                    leaf = self.leaves[l]
                    sinks[l] = (d, r, _to_phys(leaf.type, v, ".".join(self.paths[l])), st)
        keys = {}
        for s in self.sort_by:
            v = cols[s]
            keys[s] = v.to_list() if isinstance(v, DictionaryColumn) else v.tolist() if isinstance(v, array) else list(v)
            if next(nd for nd in self.nodes if nd.name == s).type.ptype in (6, 7):   # memoryview does not sort; copy bytearrays
                keys[s] = [x if x is None or isinstance(x, (str, bytes, _dec.Decimal, uuid.UUID)) else bytes(x) for x in keys[s]]
        # everything converted: append to the buffer (or adopt the lists when it is empty)
        b = self._buf
        nbytes = sum(_value_bytes(leaf.type, v) + len(d) for (d, r, v, st), leaf in zip(sinks, self.leaves))
        if not b["rows"]:
            b["leaves"] = [tuple(map(list, x)) if any(type(y) is not list for y in x) else x for x in sinks]
            b["keys"] = {s: keys[s] if keys[s] is not cols[s] else list(keys[s]) for s in self.sort_by}
        else:
            for (bd, br, bv, bs), (d, r, v, st) in zip(b["leaves"], sinks):
                off = len(bd); bs.extend(map(off.__add__, st)); bd.extend(d); br.extend(r); bv.extend(v)
            for s in self.sort_by: b["keys"][s].extend(keys[s])
        b["rows"] += n; b["bytes"] += nbytes

    def _sorted_buf(self, b):
        """A copy of buffer b with its rows in sort_by order (stable; NaN, then nulls, last)."""
        n = b["rows"]; order = list(range(n))
        for s in reversed(self.sort_by):
            k = b["keys"][s]
            good = [i for i in order if k[i] is not None and k[i] == k[i]]
            if len(good) < n:
                rest = [i for i in order if k[i] is None or k[i] != k[i]]
                rest = [i for i in rest if k[i] is not None] + [i for i in rest if k[i] is None]
            else: rest = []
            good.sort(key=k.__getitem__); order = good + rest
        if order == list(range(n)): return b
        out = {"leaves": [], "keys": {s: list(map(v.__getitem__, order)) for s, v in b["keys"].items()},
               "rows": n, "bytes": b["bytes"]}
        for (d, r, v, st), leaf in zip(b["leaves"], self.leaves):
            if not leaf.r and len(d) == n:      # flat leaf: one level per row
                vi = list(accumulate(map(leaf.d.__eq__, d), initial=0))
                d2 = list(map(d.__getitem__, order))
                v2 = [v[vi[i]] for i in order if d[i] == leaf.d]
                out["leaves"].append((d2, [0] * n, v2, list(range(n))))
                continue
            ends = st[1:] + [len(d)]
            vi = list(accumulate(map(leaf.d.__eq__, d), initial=0))
            d2, r2, v2, s2 = [], [], [], []
            for i in order:
                a, e = st[i], ends[i]
                s2.append(len(d2)); d2 += d[a:e]; r2 += r[a:e]; v2 += v[vi[a]:vi[e]]
            out["leaves"].append((d2, r2, v2, s2))
        return out

    def _flush(self, final):
        """Write out full row groups (all buffered rows when final). The buffer is
        replaced only after every row group of this flush was written."""
        b = self._buf
        if b is None or not b["rows"]: return
        if not final and b["rows"] < self.row_group_size and b["bytes"] < self.row_group_bytes: return
        if self.sort_by: b = self._sorted_buf(b)
        n, per_row = b["rows"], max(1, b["bytes"] // max(1, b["rows"]))
        k = max(1, min(self.row_group_size, self.row_group_bytes // per_row))
        # value offset at each level position, per leaf (values exist only at max definition level)
        vis = [list(accumulate(map(leaf.d.__eq__, d), initial=0)) for (d, r, v, st), leaf in zip(b["leaves"], self.leaves)]
        r0 = 0
        while n - r0 >= k or (final and r0 < n):
            r1 = min(n, r0 + k); self._row_group(b, vis, r0, r1); r0 = r1
        rest = self._new_buf()                  # rows left over wait for the next batch
        for (d, r, v, st), (rd, rr, rv, rs), vi in zip(b["leaves"], rest["leaves"], vis):
            if r0 >= n: break
            e0 = st[r0]
            rd += d[e0:]; rr += r[e0:]; rv += v[vi[e0]:]; rs += [x - e0 for x in st[r0:]]
        for s, v in b["keys"].items(): rest["keys"][s] += v[r0:]
        rest["rows"] = n - r0; rest["bytes"] = b["bytes"] * (n - r0) // n if n else 0
        self._buf = rest

    def _row_group(self, b, vis, r0, r1):
        nrows = r1 - r0
        chunks, start, ub, cb = [], self.pos, 0, 0
        for (d, r, v, st), vi, leaf in zip(b["leaves"], vis, self.leaves):
            e0 = st[r0]; e1 = st[r1] if r1 < len(st) else len(d)
            v0, v1 = vi[e0], vi[e1]
            meta, ci, oi, u, c = self._chunk(leaf, d[e0:e1], r[e0:e1], v[v0:v1], [x - e0 for x in st[r0:r1]], nrows)
            chunks.append(meta); self.indexes.append((len(self.row_groups), len(chunks) - 1, ci, oi)); ub += u; cb += c
        sorting = [[(1, _T_I32, next(n.leaf for n in self.nodes if n.name == s)), (2, _T_BOOL, False), (3, _T_BOOL, False)]
                   for s in self.sort_by]
        self.row_groups.append([(1, _T_LIST, (_T_STRUCT, chunks)), (2, _T_I64, ub), (3, _T_I64, nrows),
                                (4, _T_LIST, (_T_STRUCT, sorting) if sorting else None), (5, _T_I64, start),
                                (6, _T_I64, cb), (7, _T_I32, len(self.row_groups))])
        self.num_rows += nrows

    def _out(self, b):
        self.f.write(b); self.pos += len(b)

    def _page(self, page_type, body, header_fields):
        comp = self.compress(body) if self.compress else body
        hdr = _thrift_bytes([(1, _T_I32, page_type), (2, _T_I32, len(body)), (3, _T_I32, len(comp))] + header_fields)
        off = self.pos; self._out(hdr); self._out(comp)
        return off, len(hdr) + len(body), len(hdr) + len(comp)

    def _chunk(self, leaf, defs, reps, phys, starts, nrows):
        t, name = leaf.type, ".".join(self.paths[leaf.leaf])
        maxd, maxr = leaf.d, leaf.r
        dw, rw = maxd.bit_length(), maxr.bit_length()
        # dictionary or plain
        dset = self.dictionary
        use_dict = dset is True or dset == "auto" or (isinstance(dset, (list, tuple, set)) and (name in dset or name.split(".")[0] in dset))
        idx = dpage = None; distinct = []
        if use_dict and t.ptype != 0 and t.kind != "null" and phys:
            keys = list(map(_PD, phys)) if t.ptype == 5 else list(map(struct.Struct("<f").pack, phys)) if t.ptype == 4 else phys
            distinct = list(dict.fromkeys(keys))
            # "auto" decides on a column's first row group and sticks to it, so
            # every row group is encoded the same way (fastparquet, for one,
            # misreads strings when dictionary and plain row groups are mixed);
            # an oversized dictionary switches the column to plain for good.
            chosen = self._dict_choice.get(name) if dset == "auto" else True
            if chosen is None: chosen = len(distinct) <= len(phys) // 2
            if chosen and dset == "auto" and len(distinct) >= 1 << 16: chosen = False
            if chosen:
                pos_of = {k: i for i, k in enumerate(distinct)}
                idx = list(map(pos_of.__getitem__, keys))
                dvals = [struct.unpack("<d" if t.ptype == 5 else "<f", k)[0] for k in distinct] if t.ptype in (4, 5) else distinct
                dpage = _plain_encode(t, dvals)
                if len(dpage) > 1 << 20 and dset == "auto": idx = dpage = None; chosen = False
            if dset == "auto": self._dict_choice[name] = chosen
        iw = (len(distinct) - 1).bit_length() if idx is not None else 0
        # pages: split rows so pages come out near page_size bytes before compression
        if t.ptype == 6: vbytes = sum(map(len, phys)) + 4 * len(phys)
        elif t.ptype == 7: vbytes = t.type_length * len(phys)
        elif t.ptype == 0: vbytes = len(phys) // 8 + 1
        else: vbytes = len(phys) * (4 if t.ptype in (1, 4) else 8)
        if idx is not None: vbytes = len(phys) * max(iw, 1) // 8 + 1
        est = vbytes + len(defs) * (dw + rw) // 8 + 1
        npages = max(1, -(-est // self.page_size))
        rpp = min(self.page_rows, max(1, -(-nrows // npages)))
        present = list(map(maxd.__eq__, defs))
        vcum = list(accumulate(present, initial=0))
        starts = starts + [len(defs)]
        u_total, c_total = 0, 0
        dict_off = None
        if dpage is not None:
            dict_off, u, c = self._page(2, dpage, [(7, _T_STRUCT, [(1, _T_I32, len(distinct)), (2, _T_I32, 0)])])
            u_total += u; c_total += c
        data_off = self.pos
        ci_null, ci_min, ci_max, ci_nulls, locs, page_stats = [], [], [], [], [], []
        enc = 8 if idx is not None else 0
        for r0 in range(0, nrows, rpp):
            r1 = min(nrows, r0 + rpp); e0, e1 = starts[r0], starts[r1]; v0, v1 = vcum[e0], vcum[e1]
            body = bytearray()
            if maxr:
                lv = _rle_encode(reps[e0:e1], rw); body += _P32(len(lv)); body += lv
            lv = _rle_encode(defs[e0:e1], dw); body += _P32(len(lv)); body += lv
            if idx is not None: body.append(iw); body += _rle_encode(idx[v0:v1], iw)
            else: body += _plain_encode(t, phys[v0:v1])
            off, u, c = self._page(0, bytes(body), [(5, _T_STRUCT, [(1, _T_I32, e1 - e0), (2, _T_I32, enc), (3, _T_I32, 3), (4, _T_I32, 3)])])
            u_total += u; c_total += c
            mm = _minmax(t, phys[v0:v1])
            ci_null.append(v1 == v0); ci_nulls.append((e1 - e0) - (v1 - v0))
            ci_min.append(mm[0] if mm else b""); ci_max.append(mm[1] if mm else b"")
            page_stats.append(mm); locs.append([(1, _T_I64, off), (2, _T_I32, c), (3, _T_I64, r0)])
        mm = _minmax(t, phys)
        nulls = len(defs) - len(phys)
        stats = [(3, _T_I64, nulls)]
        if mm:
            stats += [(5, _T_BIN, mm[1]), (6, _T_BIN, mm[0]), (7, _T_BOOL, mm[3]), (8, _T_BOOL, mm[2])]
            if t.tc and t.signed and t.kind not in ("decimal",): stats += [(1, _T_BIN, mm[1]), (2, _T_BIN, mm[0])]
        stats.sort(key=lambda f: f[0])
        npages_written = len(locs)
        encodings = [0, 3] + ([8] if idx is not None else [])
        es = ([[(1, _T_I32, 2), (2, _T_I32, 0), (3, _T_I32, 1)]] if idx is not None else []) + \
             [[(1, _T_I32, 0), (2, _T_I32, enc), (3, _T_I32, npages_written)]]
        meta = [(1, _T_I32, t.ptype), (2, _T_LIST, (_T_I32, sorted(set(encodings)))),
                (3, _T_LIST, (_T_BIN, self.paths[leaf.leaf])), (4, _T_I32, self.codec), (5, _T_I64, len(defs)),
                (6, _T_I64, u_total), (7, _T_I64, c_total), (9, _T_I64, data_off),
                (11, _T_I64, dict_off), (12, _T_STRUCT, stats), (13, _T_LIST, (_T_STRUCT, es))]
        order = _boundary_order(t, page_stats)   # lets readers binary-search the column index
        ci = [(1, _T_LIST, (_T_BOOL, ci_null)), (2, _T_LIST, (_T_BIN, ci_min)), (3, _T_LIST, (_T_BIN, ci_max)),
              (4, _T_I32, order), (5, _T_LIST, (_T_I64, ci_nulls))]
        oi = [(1, _T_LIST, (_T_STRUCT, locs))]
        chunk = [(2, _T_I64, dict_off if dict_off is not None else data_off), (3, _T_STRUCT, meta)]
        return chunk, ci, oi, u_total, c_total

    def close(self):
        """Write the page index and footer, and move the file into place."""
        if self.f is None: return
        if self._broken: self.abort(); raise ValueError("an earlier batch failed part way; no file was written")
        try:
            if self.nodes is None:              # no rows written: the columns named so far, or in types=
                self._schema(self._empty or {k: [] for k in self.types})
            self._flush(final=True)
            ci_pos, oi_pos = {}, {}
            for rg, c, ci, oi in self.indexes:
                b = _thrift_bytes(ci); ci_pos[rg, c] = (self.pos, len(b)); self._out(b)
            for rg, c, ci, oi in self.indexes:
                b = _thrift_bytes(oi); oi_pos[rg, c] = (self.pos, len(b)); self._out(b)
            for i, rg in enumerate(self.row_groups):
                chunks = rg[0][2][1]
                for c, chunk in enumerate(chunks):
                    o, l = oi_pos[i, c]; co, cl = ci_pos[i, c]
                    chunk += [(4, _T_I64, o), (5, _T_I32, l), (6, _T_I64, co), (7, _T_I32, cl)]
            schema = [[(4, _T_BIN, "schema"), (5, _T_I32, len(self.nodes))]]
            for n in self.nodes: _schema_elements(n, schema)
            kv = [[(1, _T_BIN, str(k)), (2, _T_BIN, str(v))] for k, v in self.metadata.items()]
            footer = _thrift_bytes([
                (1, _T_I32, 2), (2, _T_LIST, (_T_STRUCT, schema)), (3, _T_I64, self.num_rows),
                (4, _T_LIST, (_T_STRUCT, self.row_groups)), (5, _T_LIST, (_T_STRUCT, kv) if kv else None),
                (6, _T_BIN, "microparquet version %s" % __version__),
                (7, _T_LIST, (_T_STRUCT, [[(1, _T_STRUCT, [])] for _ in self.leaves]))])
            self._out(footer); self._out(_P32(len(footer))); self._out(b"PAR1")
            if self._tmp:
                self.f.close()
                mask = os.umask(0); os.umask(mask)
                os.chmod(self._tmp, 0o666 & ~mask)
                os.replace(self._tmp, self.dest); self._tmp = None; self._finalizer.detach()
            else: self.f.flush()
        except BaseException:
            self.abort(); raise
        self.f = None

    def abort(self):
        """Stop without producing a file."""
        if self._tmp:
            self._finalizer.detach(); _discard(self.f, self._tmp); self._tmp = None
        self.f = None

def _discard(f, path):
    try: f.close()
    finally:
        try: os.remove(path)
        except OSError: pass

def _boundary_order(t, page_stats):
    """1 ascending, 2 descending, 0 unordered (null pages ignored)."""
    ks = [(s[0], s[1]) for s in page_stats if s]
    if len(ks) < 2: return 1 if ks else 0
    def key(b):
        if t.ptype == 6 or (t.ptype == 7 and t.kind != "decimal"): return b
        if t.kind == "decimal" and t.ptype == 7: return int.from_bytes(b, "big", signed=True)
        if t.ptype == 0: return b[0]
        tc = t.tc.upper() if not t.signed and t.bits >= 32 else t.tc
        return struct.unpack("<" + tc, b)[0]
    lo = [key(a) for a, b in ks]; hi = [key(b) for a, b in ks]
    if all(x <= y for x, y in zip(lo, lo[1:])) and all(x <= y for x, y in zip(hi, hi[1:])): return 1
    if all(x >= y for x, y in zip(lo, lo[1:])) and all(x >= y for x, y in zip(hi, hi[1:])): return 2
    return 0

def write(dest, data, types=None, **kw):
    """Write data (a dict of columns or a list of row dicts) to a Parquet file.
    Keyword arguments are those of ParquetWriter (sort_by, compression, ...)."""
    with ParquetWriter(dest, types, **kw) as w:
        w.write(data)
