# Security

microparquet reads files and URLs that may come from untrusted sources, so
its trust boundaries are the bytes of a file, the responses of an HTTP server,
and the values handed to the writer.

## What it defends against

- **Corrupt or hostile files.** Counts, sizes and offsets read from a file are
  bounded and checked; decompression stops at the size a page declares
  (pages claiming more than `MAX_PAGE_SIZE`, 1 GiB, are refused). Bad input
  raises `ParquetError` instead of hanging, exhausting memory or returning
  short columns. `tests/fuzz_malformed.py` mutates files and checks this.
- **Hostile or broken servers.** `HTTPSource` accepts only http(s) URLs,
  refuses https→http redirects, drops every caller header except `Range` when
  a redirect changes host or scheme, rejects credentials in URLs, and checks
  status, `Content-Range`, body length, file size and ETag/Last-Modified on
  every response. Bodies are read only up to the requested length, with an
  overall deadline per request; a server that ignores ranges is capped by
  `max_download`.
- **Paths that look like URLs.** A `str` or `Path` is always a local file and
  never causes a network request. Only regular files are opened (no FIFOs or
  devices). Local files are read with `os.pread`, not memory-mapped, so a file
  truncated by another process raises an error instead of crashing Python.
- **Writer input.** Values are type- and range-checked before they are
  encoded; a rejected batch is undone; files appear atomically on `close()`.

## What it does not do

- Page checksums (CRCs) are not verified, so some random corruption decodes
  to wrong values without an error.
- A local file rewritten in place while open serves new bytes under the old
  footer. A remote file that changes without the server sending ETag or
  Last-Modified cannot be detected.
- On Windows, a UNC path string is opened by the operating system over SMB.
  Callers that accept untrusted paths should resolve them inside an allowed
  directory first.

## Reporting

Please report vulnerabilities privately to the maintainers rather than in a
public issue, with a minimal reproduction.
