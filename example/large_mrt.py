import bgpframe

bgpframe.mrt_to_parquet(
    "https://data.ris.ripe.net/rrc00/latest-bview.gz",
    "rrc00_latest_bview.parquet",
    batch_size=500_000,   # 메모리 부족하면 더 줄이기
)