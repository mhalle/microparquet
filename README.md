# microparquet

A Parquet reader and writer in one pure-Python file. It has no dependencies, uses only the standard library on Python 3.14+, and runs on Python 3.9+. Apache-2.0.

"micro" refers to dependencies, not MicroPython. It needs CPython (or Pyodide) modules such as `array`, `struct` and `itertools`.

```
pip install microparquet                 # first release pending; until then:
pip install git+https://github.com/mhalle/microparquet
```

You can also copy `microparquet.py` into your project. It is the whole library, about 120 KB.

## Reading

```python
import microparquet as mpq

mpq.read("code.parquet")                          # {column: list of values}

pf = mpq.ParquetFile("device.parquet")            # a local path, bytes, or a binary file object
pf.schema, pf.column_names, pf.num_rows, pf.metadata
pf.statistics("model")                            # (min, max, null_count) per row group
pf.head(5)                                        # reads only the pages it needs
pf.read(["model", "codeset_id"], filters=[("manufacturer", "=", "Sony")])
pf.read(filters=[[("id", "<", 10)], [("id", "in", {99, 100})]])   # OR of ANDs
pf.read(limit=1000)                               # the first 1000 matching rows
for batch in pf.iter_batches(batch_size=50_000): ...   # bounded memory
for row in pf.rows(["id", "model"]): ...          # tuples
```

Values match pyarrow's `Table.to_pydict()`. Structs come back as dicts, lists as lists, and maps as lists of `(key, value)` tuples, at any nesting depth. A dotted path such as `"bbox.xmin"` names a field inside a struct. `arrays=True` returns numeric columns as `array.array`. `dictionaries=True` returns dictionary-encoded columns as `DictionaryColumn(indices, dictionary)`, without building a value per row.

**Filters** take pyarrow's form and follow pyarrow's null and NaN semantics. Reads stay small in five ways:

- Row groups are skipped using column statistics.
- Pages are skipped using the page index, or page-header statistics when there is no index.
- Whole chunks are skipped when no dictionary entry can match.
- Filter columns are read one at a time, and only for the rows still in play.
- Other columns are read only for the pages that hold matching rows.

**Remote files** are read with HTTP range requests. The reader fetches the footer first, then only the pages it needs, merging nearby ranges and fetching in parallel:

```python
pf = mpq.open_url("https://example.org/data.parquet")
pf = mpq.ParquetFile(mpq.HTTPSource(url, headers={"Authorization": "Bearer ..."}))
```

A path (`str` or `Path`) is always a local file, even when it looks like a URL. Remote reads have to be asked for. `HTTPSource` accepts only http(s), refuses https→http redirects, and drops caller headers when a redirect changes host. It also checks every response: status, `Content-Range`, length, file size and ETag. Each request has an overall deadline.

## Writing

```python
mpq.write("out.parquet", {"id": [3, 1, 2], "tags": [["a"], [], None]}, sort_by="id")
mpq.write("out.parquet", [{"a": 1}, {"b": "x"}])                  # a list of row dicts works too

types = {"price": "decimal(10,2)", "at": "timestamp[ms,UTC]", "user": "struct<name: string, roles: list<string>>"}
with mpq.ParquetWriter("big.parquet", types) as w:
    for batch in batches:
        w.write(batch)                          # buffered: many small batches still make full row groups

mpq.write("copy.parquet", mpq.ParquetFile("in.parquet").read(dictionaries=True))
```

The writer covers the common cases. Its files read well in microparquet itself and in pyarrow, polars, DuckDB and fastparquet.

- **Types are inferred** from the first batch that has rows. Recognized Python values: bool, int, float, str, bytes, date, datetime (naive or timezone-aware), time, Decimal, UUID, lists, and dicts as structs, nested to any depth. You can also name types in `types=`: `int8`…`uint64`, `float32`, `float64`, `string`, `json`, `binary`, `bool`, `date`, `time[ms|us|ns]`, `timestamp[ms|us|ns(,UTC)]`, `decimal(p,s)`, `uuid`, `null`, `list<…>`, `struct<name: type, …>`. Name the type of any column that may hold only nulls in the first batch.
- **Columns** can be lists, tuples, `array.array`, `DictionaryColumn`, or anything with a `tolist()` method, such as a numpy array. Arrays skip the per-value checks. Dictionary values are converted once per entry rather than once per row, so `read(dictionaries=True)` output writes back quickly. `read(arrays=True)` gives dates and timestamps as raw numbers, so name their types when writing that output back.
- **Rows are buffered into row groups** of `row_group_size` rows (65,536 by default), or fewer once they reach `row_group_bytes` (128 MB by default, estimated). `sort_by` sorts each row group and records the order in the file. A single `write()` call gives a fully sorted file.
- **Files are laid out for fast reads.** Each file gets statistics, a page index, and dictionary encoding where it pays. The defaults are zstd level 3 compression (gzip where zstd isn't available) and 64 KB pages.
- **Values are checked strictly.** A value of the wrong type or out of range raises an error. The rejected batch is undone, and so is a batch whose row groups fail to write. The file appears atomically, only when the writer closes cleanly.
- **Not written:** maps, INT96, the delta and byte-stream-split encodings, v2 data pages, bloom filters.

## Optional accelerators

Nothing is required. When these packages are installed, microparquet uses them:

- **`cramjam`:** C-speed snappy, LZ4, Brotli and zstd, in one small wheel.
- **`python-snappy`, `brotli`, `lz4`:** the same codecs, one package each.
- **`zstandard`:** zstd on Python versions before 3.14. Python 3.14 has zstd in the standard library as `compression.zstd`.

Set `MICROPARQUET_PURE=1` to ignore all of them.

## Coverage

- **Reading:**
  - Encodings: PLAIN, dictionary, RLE, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT, in v1 and v2 data pages.
  - Codecs: uncompressed, snappy, gzip, zstd, LZ4 and LZ4_RAW. Brotli needs `cramjam` or `brotli`.
  - Logical types: STRING, ENUM, JSON, DECIMAL, DATE, TIME, TIMESTAMP, INT96, unsigned INT, UUID and FLOAT16.
  - Not supported: LZO, the ALP encoding, encryption, bloom filters (they are ignored) and filters on list elements.
- **Corrupt input** raises `ParquetError`, a subclass of `ValueError`, naming the column and row group. Counts and sizes read from the file are bounded, so bad input fails instead of hanging or exhausting memory. Page checksums are not verified.

## Performance

The figures below were measured while developing microparquet, on one machine; see `docs/`. Python objects are much slower to build than Arrow arrays. If you need a columnar engine, use pyarrow, polars or DuckDB.

- **Point lookups on sorted files** take tens of milliseconds, without importing a 100 MB engine. On a 2.1M-row join benchmark, a lookup took 20–27 ms, against 47–102 ms for polars and DuckDB.
- **Full reads to Python objects:**
  - With `cramjam` installed, microparquet was faster than pyarrow's `to_pydict()` on 9 of 10 real-world files and used about half the memory.
  - Without accelerators, snappy-compressed text is the slow case.
  - Deeply nested data is 2–5× slower than pyarrow.

## Tests

```
pip install -e . --group test
pytest -m unit          # standard library only: the writer and reader against each other, HTTP, filters
pytest -m interop       # against pyarrow, polars, DuckDB and fastparquet, plus fuzzing (filters, malformed files, writer faults)
PARQUET_TESTING=../parquet-testing/data pytest -m external
```

Each suite in `tests/` is also a standalone script with a usage line at the top. `tools/` holds the benchmark and data-building scripts used during development.

## License and trademarks

Apache License 2.0; see `LICENSE` and `NOTICE`.

microparquet is an independent implementation of the Apache Parquet file format. It is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation. Apache Parquet, Parquet, and the Parquet logo are trademarks of the Apache Software Foundation.
