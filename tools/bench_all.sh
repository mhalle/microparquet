#!/bin/sh
# Usage: bench_all.sh python data_dir -- median of 3 fresh processes per cell
PY=$1; D=$2; H=$(cd "$(dirname "$0")" && pwd); M=$(dirname "$H")
run() { for i in 1 2 3; do $PY -I "$H/bench.py" "$@"; done | sort -t= -k3 -n | sed -n 2p; }
for T in all; do for L in micro pyarrow polars duckdb; do echo "tables        $(run $L $T $D/tables "$M")"; done; done
for X in tables tables_sorted tables_lookup; do for L in micro pyarrow polars duckdb; do echo "$(printf %-13s $X) $(run $L lookup $D/$X "$M")"; done; done
echo "remote (local server, 0 ms)  $(run micro lookup http://127.0.0.1:8765 "$M")"
echo "remote (local server, 30 ms) $(run micro lookup http://127.0.0.1:8766 "$M")"
