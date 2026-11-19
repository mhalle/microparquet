# Usage: make_writers.py real_dir -- rewrite sample tables with other writers into real_dir/writers
import sys, os, pyarrow.parquet as pq
d = sys.argv[1]; o = os.path.join(d, 'writers'); os.makedirs(o, exist_ok=True)
green = pq.read_table(os.path.join(d, 'nyc_green_2024_01.parquet'))
squad = pq.read_table(os.path.join(d, 'hf_squad.parquet')).slice(0, 20000)
chat = pq.read_table(os.path.join(d, 'hf_ultrachat_test_sft.parquet')).slice(0, 3000)
gdf = green.to_pandas()
gdf['store_and_fwd_flag_cat'] = gdf['store_and_fwd_flag'].astype('category')
import fastparquet
for comp in ('SNAPPY', 'GZIP', 'ZSTD', None):
    fastparquet.write(os.path.join(o, f'fastparquet_green_{comp}.parquet'), gdf, compression=comp, row_group_offsets=20000)
fastparquet.write(os.path.join(o, 'fastparquet_green_v2.parquet'), gdf, compression='SNAPPY', stats=True, times='int96')
gdf.to_parquet(os.path.join(o, 'pandas_green.parquet'))                       # pyarrow engine, pandas metadata + index
gdf.set_index('lpep_pickup_datetime').to_parquet(os.path.join(o, 'pandas_green_index.parquet'))
import polars as pl
for t, name in ((green, 'green'), (squad, 'squad'), (chat, 'chat')):
    pl.from_arrow(t).write_parquet(os.path.join(o, f'polars_{name}.parquet'))
    pl.from_arrow(t).write_parquet(os.path.join(o, f'polars_{name}_stats_pi.parquet'), statistics='full', row_group_size=5000, data_page_size=8192)
import duckdb
con = duckdb.connect()
for t, name in ((green, 'green'), (squad, 'squad'), (chat, 'chat')):
    con.register('t', t)
    con.execute(f"copy t to '{o}/duckdb_{name}.parquet' (format parquet)")
    con.execute(f"copy t to '{o}/duckdb_{name}_v2_zstd.parquet' (format parquet, compression zstd, parquet_version v2, row_group_size 4000)")
    con.unregister('t')
import datafusion
ctx = datafusion.SessionContext()
for t, name in ((green, 'green'), (squad, 'squad'), (chat, 'chat')):
    df = ctx.from_arrow(t)
    df.write_parquet(os.path.join(o, f'arrowrs_{name}.parquet'))
    df.write_parquet(os.path.join(o, f'arrowrs_{name}_zstd.parquet'), compression='zstd')
# pyarrow settings that mimic Spark/Hive outputs
pq.write_table(green, os.path.join(o, 'pyarrow_green_spark_style.parquet'), use_deprecated_int96_timestamps=True, flavor='spark', compression='snappy')
pq.write_table(chat, os.path.join(o, 'pyarrow_chat_legacy_lists.parquet'), use_compliant_nested_type=False)
print('\n'.join(sorted(os.listdir(o))))
