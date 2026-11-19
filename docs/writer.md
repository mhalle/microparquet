> Development notes, kept as written during development (November 2026). Script names in them refer to the files now in `tests/` and `tools/`: test_corpus.py → tests/corpus.py, test_writer.py → tests/writer_roundtrip.py, test_writer_adversarial.py → tests/writer_adversarial.py, test_real.py → tests/real_files.py, test_parquet_testing.py → tests/parquet_testing.py.

# microparquet writer (session 6, 2026-11-19)

`mpq.write()` and `mpq.ParquetWriter` add about 650 lines to `microparquet.py` (now 2,279 lines). They cover the common cases, and their output reads well with microparquet's pushdown and with other engines. Like the reader, the writer is standard library only on 3.14+. On 3.9–3.13 it uses `zstandard` or `cramjam` for zstd when either is installed, and otherwise falls back to gzip.

```python
mpq.write("out.parquet", {"id": [3, 1, 2], "name": ["c", "a", None]}, sort_by="id")
mpq.write("out.parquet", [{"a": 1}, {"b": "x"}])             # list of row dicts
mpq.write("out.parquet", data, {"price": "decimal(10,2)", "at": "timestamp[ms,UTC]", "tags": "list<string>"})
with mpq.ParquetWriter("big.parquet", sort_by=["codeset_id", "ord"]) as w:
    for batch in batches: w.write(batch)                       # same columns each time
```

Options: `types`, `sort_by`, `compression` (zstd default, gzip, none; snappy/lz4/brotli with accelerators), `compression_level`, `row_group_size=65536`, `page_size=65536`, `page_rows=20000`, `dictionary="auto"|True|False|[names]`, `metadata={...}`.

## What it writes

- **Types are inferred** from Python values: bool, int (int64, or uint64 when values need it), float, str, bytes, date, datetime (naive or UTC-aware), time, Decimal (precision and scale from the data), UUID, lists, and dicts as structs, nested to any depth. int and float mixed together give float. Any other mix is an error that says to pass `types=`. Type names that can be given: `int8…uint64, float32/64, string, json, binary, bool, date, time[ms|us|ns], timestamp[ms|us|ns(,UTC)], decimal(p,s), uuid, null, list<…>`.
- **Layout:** v1 data pages and 3-level LIST. All columns are optional. Dictionary encoding is used when a column's first row group has at most 50% distinct values; the choice then holds for the whole file. The dictionary falls back to plain when it would exceed 64K entries or 1 MB.
- **Metadata:** chunk statistics (new min/max fields with exactness flags, and deprecated fields where they are safe), plus a ColumnIndex/OffsetIndex for every chunk with its boundary order. Also written: encoding_stats, column_orders, sorting_columns (when `sort_by` is given), key/value metadata, and `created_by "microparquet version 0.1.0"`. Statistics leave out NaN, follow the -0.0/+0.0 rule, use unsigned order for uint, and truncate byte arrays over 64 bytes with the exactness flags set.
- **Sorting:** `sort_by` does a stable sort of each batch by one or more top-level columns, with NaN and then nulls last.
- **Safety:** the file is written to a temp file in the same directory and renamed only on a clean `close()`. An exception inside `with` removes it, and so does a writer that is dropped without closing (via `weakref.finalize`). Every batch is checked strictly: wrong types (str in an int column, int in a string column, bool in a numeric column, bytes in a string column), out-of-range ints, float32 overflow, and decimals beyond their precision or scale all raise. A rejected batch is truncated back off the file and the writer stays usable. With a non-seekable file object, the writer refuses further batches instead. Paths that become ambiguous once dotted (column `"a.b"` next to struct `a` with field `b`) are rejected.
- **Not written:** maps, INT96, delta and byte-stream-split encodings, v2 pages, bloom filters, page CRCs.

## Verification

- **Round trips** (`test_writer.py`). About 1,300 random tables over 14 seeds during development; seeds 21, 22 and 31 (200 tables), run after the strict-typing and rollback changes, gave 0 failures. They cover every type, 0–5,000 rows, all-null and nested columns, NaN, ±0.0, ±inf, extreme ints, Unicode and `\x00`, and random compression, page size, row group size, dictionary mode and `sort_by`. Each table is read back by **pyarrow, polars, DuckDB, fastparquet (flat columns) and microparquet**, with point filters through microparquet. pyarrow also confirms the page index is present.
- **Statistics and page index** (`fuzz_filters.py … micro`). The same differential filter fuzzer, on files written by microparquet with random settings and multi-column `sort_by`. Results: 5,639 checks in seven runs, 0 failures. pyarrow's own pruned reads agree, except for its known float NaN/`!=` pruning bug, which shows up identically on pyarrow-written copies of the same tables. Separately, polars `scan_parquet().filter()` and DuckDB `WHERE` on `= < >=` predicates matched unpruned results in 2,189 of 2,189 checks.
- **Adversarial inputs** (`test_writer_adversarial.py`). 35 rejected-input cases, each leaving no file or temp file. Also covered: a failed overwrite keeps the old file, the streaming writer rejects a bad batch and keeps going (path and BytesIO), a failed first batch lets the next batch choose the schema, `with` aborts, abandoned writers clean up, and write-after-close errors. Plus 21 odd-but-legal tables, from empty tables up to a 5 MB string. All pass.
- **Python 3.9:** files written there read correctly in pyarrow with zstd, gzip and none. This check caught `zlib.compress(wbits=)`, which only exists from 3.11; it was fixed with `compressobj`.
- **Reader regressions** after the shared changes: corpus `FAILURES: none`, parquet-testing unchanged, the 37 real-world files match, malformed fuzz only raises `ParquetError`/`NotImplementedError`.

## Findings and fixes during the writer work

| Finding | Fix |
|---|---|
| fastparquet reads strings as `None` when some row groups of a column are dictionary-encoded and others plain. The files are legal, but they break a common reader. | `dictionary="auto"` decides on a column's first row group and keeps that choice |
| A bad value in a later batch (int 5 in a string column) was written as 5 zero bytes, and `bool()`/`int()`/`float()` coerced anything | Strict type checks on every value before encoding |
| A batch that failed part way left orphan pages, and the writer kept going | The batch is undone (file truncated, bookkeeping restored); unseekable outputs refuse further batches |
| A UnicodeEncodeError was re-raised with the wrong constructor (TypeError about 5 arguments) | Errors are normalized to TypeError/ValueError/OverflowError with column context |
| Writers dropped without `close()` left `.tmp` files behind | `weakref.finalize` cleanup |
| Bit-packing used one big int for a whole run, which was quadratic. A 2M-value high-entropy dictionary index run hung. | Packing in 256-value chunks; the RLE encoder now finds run boundaries with C-level `map`/`compress`, and the Python loop visits only runs of 8 or more |
| Writes were slow: codeset_cmd (2.1M rows, 4 int columns) took 26 s under profile, 9.3 s plain | Faster RLE, single-pass type inference via `set(map(type, …))`, LSD stable sorts per key: now 6.5 s |
| Not a writer bug: fastparquet fails with an `NAType` error on nullable int columns in files that also hold nested columns. pyarrow-written copies fail the same way. | The test treats it as fastparquet's bug when the pyarrow copy also fails |

## Default tuning (Harmony tables)

Page size had the biggest effect. Text columns compress far better in larger pages: device.meta is 151 MB uncompressed and holds a JSON blob per row. pyarrow's "4 KB" pages are really about 550 KB, because pyarrow checks the page size only every 1,024 rows. That is why the pyarrow-tuned tables looked small.

| device.meta | 8 KB pages | 64 KB | 1 MB |
|---|---|---|---|
| zstd 3 | 18.2 MB | 9.0 MB | 7.1 MB |
| zstd 9 | 16.1 MB | 6.8 MB | 4.7 MB |
| zstd 19 | 15.9 MB | 6.5 MB | 4.1 MB |

| Harmony tables written by microparquet | total size | lookup | full read |
|---|---|---|---|
| 8 KB pages | 35.1 MB | 21 ms | 2.97 s |
| **64 KB pages (default)** | **23.4 MB** | **27 ms** | **1.73 s** |
| 64 KB, zstd 9 | 19.7 MB | 28 ms | 1.84 s |
| 256 KB | 21.9 MB | 33 ms | 1.66 s |
| 1 MB | 21.3 MB | 45 ms | 1.56 s |
| pyarrow lookup-tuned (zstd 19), for reference | 17.6 MB | 18 ms | 1.71 s |

Chosen defaults: 64 KB pages capped at 20,000 rows (parquet-java's row limit, which keeps the page index useful for narrow columns), 64K-row groups, and zstd level 3. `compression_level=9` gives about 16% smaller files for about 20% slower writes.

**Default output in every reader** (Harmony lookup / full read to Python objects): microparquet 27 ms / 1.75 s, polars 55 ms / 0.62 s, DuckDB 102 ms / 1.58 s, pyarrow 253 ms / 0.90 s. All four return identical lookup results. Writing all six tables (2.6M rows) takes 8.7 s.

## Naming

The name is a trademark question, not copyright. The ASF lists "Parquet" and "Apache Parquet" as trademarks. Its policy bars using ASF product names as product names and bars confusingly similar names. In practice, format libraries commonly use descriptive compounds (fastparquet, hyparquet, parquet-go) without problems. Plan for the README: never use "Apache" in the name or the Parquet logo; state that this is an independent implementation, not affiliated with or endorsed by the ASF; add the standard attribution line. As of 2026-11-19, `microparquet` and `micro-parquet` are free on PyPI; GitHub has one undescribed repo, azam-data/MicroParquet. This is not legal advice; trademarks@apache.org can confirm.

## Next

- Optional writer extras: maps, DELTA_BINARY_PACKED for sorted ints, v2 pages, page CRCs, bloom filters (shared xxhash64 with the reader).
- Faster writes: the remaining time is spread across sorting, RLE and Python value conversion.
