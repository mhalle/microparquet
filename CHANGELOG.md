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
