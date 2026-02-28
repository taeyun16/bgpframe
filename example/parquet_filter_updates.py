import bgpframe


def main() -> None:
    matched = bgpframe.parquet_filter_updates(
        "rrc00_latest.parquet",
        output="rrc00_latest_updates_filtered.parquet",
        contains_ip="8.8.8.8",
        elem_type="announce",
        origin_asn=15169,
        as_path_contains=3356,
        min_as_path_len=2,
        max_as_path_len=8,
        limit=50_000,
    )
    print("matched rows:", matched)


if __name__ == "__main__":
    main()
