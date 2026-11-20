# Changelog

## 0.1.0 — unreleased

First release, grown from a 178-line prototype ("tinyparquet").

### Reading
- Full Parquet reader in one pure-Python file: PLAIN, dictionary, RLE,
  DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY and
  BYTE_STREAM_SPLIT encodings; v1 and v2 data pages; snappy, gzip, zstd, LZ4,
  LZ4_RAW and (with an accelerator) Brotli; all common logical types; nested
  structs, lists and maps with values matching pyarrow's `to_pydict()`.
- Filters with pyarrow's form and null/NaN semantics, pushed down through
  row-group statistics, the page index, page-header statistics and
  dictionaries. `limit`, `head()`, `iter_batches()`, `iter_row_groups()`,
  `rows()`, `arrays=True` and `dictionaries=True`.
- Remote reads over HTTP range requests (`open_url`, `HTTPSource`) that fetch
  only the footer and the needed pages, coalesced and in parallel.
- Hardened against corrupt files and hostile servers after two adversarial
  reviews; see `SECURITY.md`.

### Writing
- `write()` and `ParquetWriter`: type inference or named types, nesting,
  statistics, page index, dictionary encoding, zstd/gzip, `sort_by` with
  sorting metadata, strict value checks, undo of rejected batches, and atomic
  file creation. Checked against pyarrow, polars, DuckDB and fastparquet.
- Streaming: batches are buffered into full row groups (`row_group_size`, plus
  a `row_group_bytes` cap, 128 MB by default) instead of one row group per
  `write()`; `sort_by` sorts each row group. A batch whose row groups fail to
  write (an I/O error, an interrupt) is undone like a rejected one.
- Columnar input: `array.array`, `DictionaryColumn` and objects with `tolist()`
  (numpy, pandas), so `read(dictionaries=True)` output writes straight back.
- `struct<name: type, ...>` type names; empty first batches no longer fix the
  column types; a writer with `types=` and no rows writes an empty file with
  those columns.
