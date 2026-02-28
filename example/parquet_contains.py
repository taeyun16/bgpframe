import bgpframe


def main() -> None:
    matched = bgpframe.parquet_contains_ip(
        "rrc00_latest.parquet",
        "8.8.8.8",
        output="rrc00_latest_match_8_8_8_8.parquet",
    )
    print("matched rows:", matched)


if __name__ == "__main__":
    main()
