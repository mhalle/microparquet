"""pytest entry point for the script-style suites.

Each suite is a self-contained script that prints its results and exits
non-zero on any failure, so it also runs on its own (see the Usage line at the
top of each file). Here each becomes a pytest case, run in its own process with
markers so CI can pick what an environment supports:

    pytest -m unit          # standard library only (Python 3.9+, nothing installed)
    pytest -m interop       # against pyarrow, polars, DuckDB and fastparquet
    pytest -m external      # apache/parquet-testing; set PARQUET_TESTING=path/to/parquet-testing/data
"""
import glob
import os
import pathlib
import subprocess
import sys

import pytest

HERE = pathlib.Path(__file__).parent
ROOT = str(HERE.parent)


def run(script, *args, timeout=900):
    p = subprocess.run([sys.executable, "-I", str(HERE / script), ROOT, *map(str, args)],
                       capture_output=True, text=True, timeout=timeout)
    lines = p.stdout.splitlines()
    shown = [ln for ln in lines if "FAIL" in ln or "MISMATCH" in ln or "CRASH" in ln] + lines[-5:]
    assert p.returncode == 0, "\n".join(shown) + "\n" + p.stderr[-3000:]
    return p.stdout


@pytest.mark.unit
def test_stdlib():
    run("test_stdlib.py")


@pytest.mark.interop
def test_corpus(tmp_path):
    run("corpus.py", tmp_path)


@pytest.mark.interop
@pytest.mark.parametrize("seed", [1, 2])
def test_writer_roundtrip(tmp_path, seed):
    run("writer_roundtrip.py", tmp_path, 40, seed)


@pytest.mark.interop
def test_writer_adversarial(tmp_path):
    run("writer_adversarial.py", tmp_path)


@pytest.mark.interop
@pytest.mark.parametrize("writer", ["pyarrow", "micro"])
def test_fuzz_filters(tmp_path, writer):
    run("fuzz_filters.py", tmp_path, 60, 3, writer)


@pytest.mark.interop
def test_fuzz_malformed(tmp_path):
    run("writer_roundtrip.py", tmp_path, 12, 5)          # seed files from the writer...
    run("fuzz_filters.py", tmp_path, 6, 5, "pyarrow")    # ...and from pyarrow
    files = sorted(glob.glob(str(tmp_path / "*.parquet")))[:12]
    run("fuzz_malformed.py", 1500, 7, *files)


@pytest.mark.external
def test_parquet_testing():
    d = os.environ.get("PARQUET_TESTING")
    if not d: pytest.skip("set PARQUET_TESTING to a clone's data/ directory")
    run("parquet_testing.py", d, timeout=1800)
