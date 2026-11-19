> Development notes, kept as written during development (November 2026). Script names in them refer to the files now in `tests/` and `tools/`: test_corpus.py → tests/corpus.py, test_writer.py → tests/writer_roundtrip.py, test_writer_adversarial.py → tests/writer_adversarial.py, test_real.py → tests/real_files.py, test_parquet_testing.py → tests/parquet_testing.py.

# microparquet: status (2026-11-19)

Renamed from tinyparquet on 2026-11-19, to match micromcp (github.com/mhalle/micromcp). "micro" means few dependencies, not MicroPython. `microparquet` was free on PyPI and has no meaningful GitHub collisions as of that date. For trademark notes on the name, see `writer.md` § Naming.

`microparquet.py` is a general-purpose Parquet reader **and writer** in one file (about 2,280 lines; the writer is about 650 of them). It needs only the standard library on Python 3.14+ and runs on 3.9+, where zstd files need `zstandard` or `cramjam`. Import takes ~10 ms. If `cramjam` is installed (one 4 MB wheel with no dependencies), the reader uses it for C-speed snappy/LZ4/Brotli; it is never required. It grew from a 178-line prototype, which is not part of this repository.

**The writer** (session 6: `mpq.write()`, `mpq.ParquetWriter`) is documented in `writer.md`, with its verification, default tuning, findings and test scripts.

## API

```python
import microparquet as mpq
mpq.read("code.parquet")                       # {column: list}, as in the prototype

pf = mpq.ParquetFile(src)      # local path (str or Path), bytes, binary file, HTTPSource, or any object with read(offset, size) and .size
pf = mpq.open_url(url, headers=None)           # remote: same as ParquetFile(HTTPSource(url, headers))
pf.schema                     # leaf columns: physical and logical type, nullability
pf.column_names, pf.num_rows, pf.num_row_groups, pf.metadata, pf.created_by
pf.statistics("bbox.xmin")    # per row group (min, max, null_count)
pf.read(columns=None, filters=None, *, row_groups=None, limit=None, arrays=False, dictionaries=False)
pf.head(n=10, columns=None, filters=None)
pf.iter_batches(..., batch_size=65536)        # bounded memory
pf.iter_row_groups(...)
pf.rows(columns, filters, limit=None)         # tuples

mpq.write(dest, data, types=None, *, sort_by=None, compression="zstd", ...)   # see microparquet-writer.md
with mpq.ParquetWriter(dest, types=None, ...) as w: w.write(batch)
```

- **Values** match pyarrow's `Table.to_pydict()`: structs are dicts, lists are lists, maps are lists of `(key, value)` tuples, at any depth. That includes the legacy list layouts (2-level lists, `array`, `_tuple`, unannotated repeated fields).
- **Column names** are top-level fields. A dotted path selects any field inside, e.g. `"bbox.xmin"` or `"answers.text"`.
- **Filters** use pyarrow's form: a list of `(column, op, value)` that are ANDed, or a list of lists that are ORed. Ops: `= == != < <= > >= in, not in`. Columns may be struct fields given by dotted path. Nulls never match.
- **`arrays=True`** returns numeric columns as `array.array`. **`dictionaries=True`** returns `DictionaryColumn(indices, dictionary)`.

## How reads stay small

1. Row groups are skipped using statistics.
2. Pages are skipped using the page index, or page-header statistics when there is no index.
3. A chunk is skipped when no dictionary entry can match.
4. Filter columns are read one at a time, each only for rows still in play. Output columns are read only for the pages holding matching rows.
5. `limit`/`head` stop at the last wanted row and decode only the needed prefix of the last page.
6. Large string dictionaries are decoded lazily when only a few rows need them.

**Paths are always local.** A string or `Path` never causes a network request, even if it looks like a URL. Remote files must be asked for explicitly with `mpq.open_url(url)` or `mpq.HTTPSource(url)`. These accept only http(s) URLs, follow only http(s) redirects, and drop `Authorization`/`Cookie` when a redirect changes host. A URL-looking path with no local file raises `FileNotFoundError` saying how to read it remotely.

**Remote reads** use HTTP range requests. The reader fetches the footer, then the candidate pages of all needed columns in one parallel batch, merging ranges less than 32 KB apart. Without a page index, partial reads fetch column chunks a piece at a time. It works with servers that reject suffix ranges (GitHub raw) or ignore them (jsdelivr).

## Verification (all against pyarrow 26)

- **Synthetic corpus** (`test_corpus.py`): 358 checks, 0 failures, both with cramjam and with `MICROPARQUET_PURE=1`.
- **apache/parquet-testing:** 65 files match exactly and none crash.
  - Two files are ones pyarrow reads wrongly and we read correctly (`empty_data_pages_v1/v2`).
  - Two files only we can read (`flba12_timestamp`, `incorrect_map_schema`).
  - Two hold dates outside Python's datetime range. Both readers raise on them.
  - The ALP encoding is unsupported.
  - `large_string_map.brotli` (two 1 GB strings) is skipped for lack of RAM on the test machine.
- **Real-world files** (`test_real.py`): 37 files, every one matching exactly.
  - Hugging Face datasets: squad, ultrachat, mnist, imdb, gsm8k, oasst1, alpaca.
  - NYC taxi yellow/green/fhv 2024-01, and DuckDB's 7.4M-row taxi sample.
  - The same data rewritten by fastparquet, pandas, polars, DuckDB, arrow-rs and Spark-style pyarrow settings.
- **Overture Maps places** (Spark/Arrow 17, zstd, maps inside structs inside lists, 5.6M rows): 12 sampled row groups match.
- **Python versions:** results are identical on 3.9, 3.13 and 3.14.
- **Re-run after the writer work (session 6):** corpus `FAILURES: none`; parquet-testing unchanged; all 37 real-world files match; Harmony tables written by microparquet match pyarrow; malformed fuzz raises only `ParquetError`/`NotImplementedError`.

## Benchmarks

**Real-world files, full read to Python objects** (fresh process, import included):

| File | rows | microparquet (pure) | microparquet + cramjam | pyarrow `to_pydict` |
|---|---|---|---|---|
| HF alpaca (snappy text) | 52K | 2.83 s / 98 MB | 0.23 s / 100 MB | 0.62 s / 332 MB |
| HF imdb | 25K | 2.77 s | 0.11 s / 72 MB | 0.54 s / 264 MB |
| HF squad (struct of lists) | 88K | 1.87 s | 0.56 s / 110 MB | 0.71 s / 462 MB |
| HF mnist (image structs) | 60K | 0.39 s | 0.18 s / 66 MB | 0.50 s / 220 MB |
| HF ultrachat (list of structs) | 23K | 10.05 s | 0.91 s / 314 MB | 0.95 s / 685 MB |
| HF oasst1 (nested) | 84K | 5.61 s | 2.57 s / 280 MB | 1.15 s / 684 MB |
| NYC green 2024-01 | 57K | 0.25 s | 0.22 s / 37 MB | 0.61 s / 188 MB |
| NYC fhv 2024-01 (zstd) | 1.3M | 2.30 s | 1.98 s / 304 MB | 3.82 s / 585 MB |
| NYC yellow 2024-01 (zstd) | 3.0M | 9.52 s | 7.89 s / 1.2 GB | 8.25 s / 2.3 GB |
| DuckDB taxi 2019-04 (snappy) | 7.4M | 17.67 s | 13.57 s / 2.4 GB | 20.46 s / 4.8 GB |

With cramjam, microparquet is faster than pyarrow `to_pydict` on 9 of 10 files and uses about half the memory. Without accelerators, snappy-compressed text is the weak spot: pure-Python snappy runs at ~20 MB/s, which is the floor for a per-token loop. Deeply nested data (oasst1, Overture) is 2–5× slower than pyarrow's C++ assembly.

**Remote reads (no download):**

| Query | Transferred | Time | Requests |
|---|---|---|---|
| Overture places part, 743 MB on S3: bbox filter on `bbox.*` | 3.2 MB footer + 0.55 MB query | 1.5 s open + 1.2 s query | 2 + 4 |
| Fineweb-edu shard, 2.2 GB on Hugging Face: `head(3)` | 3.1 MB footer + 5.9 MB | 1.4 s open + 2.4 s | 2 + 6 |
| Harmony lookup on tuned tables (local range server) | 564 KB of 17 MB | 0.12 s; 0.47 s at 30 ms/request | 18 |

For the Overture query, 1 of 512 row groups survives statistics, 67 rows are returned, and the result matches pyarrow on the local copy. Running the same query locally takes 58 ms, against 116 ms for pyarrow dataset.

**Other numbers:**

- **`head(5)` on NYC yellow:** 0.13 s.
- **`iter_batches`:** works on files with 1M-row groups, including nested columns and rows that span pages.
- **Harmony lookup:** 20 ms on the tuned layout (pyarrow 63 ms, polars 47 ms, duckdb 86 ms). On tables written by microparquet's own writer with default settings: 27 ms.
- **Full read of all Harmony tables:** 1.65 s, against 2.0 s for the prototype.

## Changes in session 3 (real-world files)

- **Nested values like pyarrow:** structs as dicts, lists of structs, maps anywhere, and the legacy list layouts. Rows that span data pages work; Arrow 9 writes them.
- **Optional accelerators:** cramjam, python-snappy, brotli and lz4. They add Brotli support and speed snappy up about 10×.
- **New methods:** `limit`, `head()`, `iter_batches()`. Dictionaries are cached across batches, and only the needed prefix of a page is decoded.
- **Faster paths:**
  - A flatter Thrift decoder: 3 MB footers parse in 0.3 s instead of 0.45 s.
  - Short bit-packed runs decode in one step.
  - Timestamps convert via `map(timedelta, ...)`.
  - Single-level lists are sliced at C speed.
- **Remote reads:** filter columns are fetched in one batch even without a page index, and partial chunk reads are incremental.

## Adversarial review (session 4)

The review used four methods. Everything found was fixed, except the items under "Known limits".

- **Differential fuzzing:** random tables, writer settings and filters against pyarrow semantics; 4,557 checks in the final runs.
- **Mutated files:** ~16,000 corrupted files read under memory and time limits.
- **Hostile HTTP servers.**
- **A full code read**, plus re-measurement of the benchmark claims.

| # | Severity | Finding | Fix |
|---|---|---|---|
| 1 | Critical: silent wrong data | A page or dictionary where every string was `''` decoded to nothing, so those rows came back as `None`. This was an off-by-one in the fixed-length string fast path. | Fixed; found by the fuzzer |
| 2 | Critical: wrong filter results | `in` / `not in` disagreed with pyarrow on nulls, NaN and ±0.0 (109 of 1,427 checks). In pyarrow, nulls satisfy `not in`, NaN matches NaN in sets, and -0.0 ≠ 0.0 in sets. | Row evaluation now matches pyarrow. Every pruning layer is null- and NaN-aware: statistics omit NaN, so `!=`, `not in` and `in {nan}` never prune float chunks from min/max. |
| 3 | High: silent garbage | A corrupt negative offset wrapped around inside the memory-mapped buffer and read the wrong bytes. | All reads bounds-checked; footer length validated |
| 4 | High: hang / memory | Corrupt RLE run lengths caused multi-GB allocations or multi-second loops. A Thrift map of bools looped forever (container bools take one byte in the compact protocol). Delta headers with zero miniblocks or huge blocks also broke decoding. | Counts clamped to what is needed; sizes validated against the buffer |
| 5 | High: memory | Decompression bombs (gzip, zstd, snappy) could expand without limit. | Output capped at the page header's declared size |
| 6 | Medium: performance | `iter_batches` re-decoded pages for every batch, which was quadratic. NYC yellow took 7.9 s with 10K-row batches and 111 s with 997-row batches. | Per-row-group page cache that also carries list state across batches: 0.54–0.73 s at any batch size |
| 7 | Medium: security | `Authorization` / `Cookie` headers were forwarded on redirects to other hosts (urllib's default). | Dropped when the host changes |
| 8 | Medium: robustness | Short HTTP responses crashed deep inside with `TypeError`. | Clear `ParquetError` |
| 9 | Medium: API | Corrupt input raised 14 different exception types. | `ParquetError` (a `ValueError`) with column and row-group context. Only `ImportError` and `NotImplementedError` remain distinct. |
| 10 | Medium: latent | v2 pages with `num_nulls == 0` skipped definition levels even on list columns, where empty lists need them. | Levels are always read for list columns |
| 11 | Medium: silent garbage | BIT_PACKED definition/repetition levels (very old writers) would be misread. | `NotImplementedError` when the level actually exists. Many writers *declare* BIT_PACKED for absent levels; my first version of this check wrongly rejected 21 good files, and the test suites caught it. |
| 12 | Medium: silent garbage | Column chunks stored in external files (`file_path`) were read from the wrong file. | `NotImplementedError` |
| 14 | Medium: security | Strings starting with `http(s)://` were fetched over the network automatically. An untrusted "path" could make a server request any URL, and urllib would also accept `file://`. | Paths are always local; remote needs `open_url()`/`HTTPSource`, which allow only http(s), including on redirects (session 5) |
| 13 | Low: performance | `head()` converted whole timestamp dictionaries. | Lazy conversion for all converted dictionary types: `head(5)` on NYC yellow went from 0.13 s to 0.024 s |

**Verification after the fixes:**

- Corpus (cramjam and pure): 0 failures.
- parquet-testing: 65 match, 0 crashes.
- Real-world files: all 37 match.
- Overture sample: matches.
- Filter fuzz: 0 failures in 1,427 checks.
- Malformed fuzz: no hangs or memory blowups in about 11,000 cases since the fix, and nothing escapes `ParquetError` in another 5,000 nested-file mutations.
- Python 3.9 gives identical values on all 149 files. One `repr()` differs because of 3.9's older Unicode tables.

**pyarrow bugs found along the way.** In 4 fuzz cases, `pq.read_table(filters=...)` dropped rows that pyarrow's own compute kernel says match: NaN in `in` sets, and ±0.0. pyarrow's statistics pruning ignores NaN. microparquet returns the rows that match.

**Known limits (documented, not fixed):**

- **Checksums:** page CRCs are not verified. About 35% of randomly corrupted files still decode without error, possibly with wrong values.
- **Memory mapping:** fixed in session 5; local files are now read with `os.pread`.
- **Cost of the hardening:** about 5% on full reads (Harmony all tables 1.73 s vs 1.65 s). Lookups are unchanged (21 ms).
- **What the benchmark comparisons mean:**
  - "Faster than pyarrow" means faster than pyarrow's `to_pydict()`, which builds the same Python objects. pyarrow's native columnar read is 10–35× faster when you don't need Python objects (NYC yellow: 0.24 s native vs ~8 s to Python objects in either library).
  - Single-run real-world timings vary by about ±15%.

## Second adversarial review: independent agent, source handling (session 5)

A separate reviewer agent, which hadn't seen the earlier work, attacked how files and URLs are opened and read. It used misbehaving HTTP servers, a TLS downgrade server, a fake proxy and odd local inputs, and reported 19 findings with reproducers. I reproduced the top nine before fixing anything. All are fixed except the two noted at the end; every reproducer now fails safely.

| # | Severity | Finding | Fix |
|---|---|---|---|
| 1 | Critical | Authorization and cookies were re-sent in cleartext when an https URL redirected to http on the same host. | https→http redirects are refused |
| 2 | Critical | Other caller headers (API keys, cloud tokens) went to a different host on redirect. | Every caller header except Range is dropped when the host or scheme changes |
| 3 | Critical | A 206 body longer than its Content-Range was cached as the following bytes. Rows came back with values from another file and no error. | Status, Content-Range start/end/total and exact body length are all checked |
| 4 | Critical | A later response reporting a different file size silently truncated columns. | Size learned once; any change raises |
| 5 | Critical | Short reads from file objects and custom sources silently truncated columns. | Read in a loop; returned lengths checked |
| 6 | Critical | A corrupt *local* file could return short columns of unequal length (pyarrow errors on it). | Levels and values per page must match the header; every column must produce exactly the requested rows |
| 7 | High | A remote file replaced mid-read gave a footer from one version and data from another. | ETag or Last-Modified must not change. Detection needs the server to send one; S3, CloudFront, GitHub and Hugging Face do. |
| 8 | High | A hostile server could send gigabytes in reply to a 64 KB range. | Bodies are read only up to the requested length. A server that ignores ranges is capped by `max_download` (512 MiB default). |
| 9 | High | urllib read whole redirect bodies before following them. | Redirect bodies are not read |
| 10 | High | No overall deadline: a server sending 1 byte per second never timed out. | `timeout` is an overall deadline per request |
| 11 | High | Brotli decompression bomb: a 4 KB file inflated to 1–2 GiB, and cramjam aborted the interpreter. | Brotli and zstd decompress into a buffer of the declared size. Pages claiming more than `MAX_PAGE_SIZE` (1 GiB) are refused. |
| 12, 15 | Medium | `/dev/zero` read until memory ran out. A FIFO path blocked forever in `open()`. | Only regular files are accepted, checked before `open()` |
| 13 | Medium | Malformed responses raised IndexError, TypeError or IncompleteRead. | `ParquetError` |
| 14 | Medium | A memory-mapped file truncated by another process killed Python with SIGBUS. | Local files read with `os.pread` (no mmap); truncation raises `ParquetError`. pread is faster than mmap plus a copy. |
| 16 | Low | Credentials in the URL (`user:pass@host`) were echoed in errors or sent to proxies. | Rejected; pass credentials in headers |
| 17, 18 | Low | Inconsistent out-of-range reads and confusing messages for text-mode files, non-contiguous memoryviews, and bytes that look like a path. | Fixed |

**Regression caught during re-verification.** A real GitHub page-indexed file failed once a filter loaded its page index. A size check from the first review raised `ParquetError` on the 256-byte header prefix, which bypassed the retry with the full buffer. It only shows with more than ~250 pages per column chunk, which no test file had. It's fixed, and the corpus now includes a file with 2,785 pages per chunk; I confirmed that file fails on the old version.

**Not fixed (inherent):**

- A local file rewritten in place while open serves new bytes under the old footer.
- A remote file that changes without the server sending ETag or Last-Modified can't be detected.
- **Untested:** on Windows, a UNC path string (`\\host\share\x.parquet`) is opened by the OS over SMB. This is ordinary OS path behavior. Callers taking untrusted paths should resolve them inside an allowed directory first.

**Verification:**

- Corpus: 373 checks, 0 failures (cramjam and pure).
- Real-world files: all 37 match.
- parquet-testing: 65 match.
- Overture sample: matches.
- Filter fuzz: 0 failures.
- Malformed fuzz: no hangs or memory blowups; every error is a `ParquetError`.
- Remote reads through a Hugging Face cross-host redirect, S3 and GitHub raw work, and ETag tracking is active.
- Benchmarks are unchanged: lookup 19 ms, all tables 1.73 s.

## Not done / next steps

- **Optional checksum verification** (`verify_checksums=True`) for writers that store page CRCs.
- **Writer extras:** maps, DELTA_BINARY_PACKED for sorted ints, v2 pages, page CRCs, bloom filters. The writer itself is done (session 6) and lets the archive pipeline drop pyarrow.
- **Bloom filters:** needs a pure-Python xxhash64, about 40 lines. Useful for point lookups on unsorted, high-cardinality columns, and shared by reader and writer.
- **Reader limit found while testing the writer:** a top-level column whose name contains a dot (`"a.b"`) collides with a struct field path `a.b` in the dotted-name lookup. The writer refuses to create such files; the reader still resolves the name to one of them.
- **Faster nested assembly:** currently 2–5× slower than pyarrow.
- **Arrow-specific types:** durations and large lists via the `ARROW:schema` metadata. Durations come back as raw integers today.
- **Filters on list elements; ALP; LZO.**
- **Parallel open of many remote files.** Footers dominate multi-file remote queries.
- **Publishing:** `microparquet` is free on PyPI (checked 2026-11-19). The README should say "micro" refers to dependencies, not MicroPython, and carry the ASF trademark attribution described in `writer.md`.
