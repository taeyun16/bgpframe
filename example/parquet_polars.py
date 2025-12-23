import polars as pl

df = pl.read_parquet("rrc00_latest.parquet")
print(df.head())
print(df.schema)
print(len(df))

