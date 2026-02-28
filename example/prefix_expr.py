import bgpframe
import polars as pl


def main() -> None:
    df = pl.read_parquet("rrc00_latest.parquet")

    v6 = df.filter(pl.col("prefix_ver") == 6)
    res_v6 = v6.filter(bgpframe.v6_contains_expr("2804:41f0::"))
    print("v6 matches:", len(res_v6))

    res_any = df.filter(bgpframe.contains_prefix_expr("195.114.123.0"))
    print("any matches:", len(res_any))


if __name__ == "__main__":
    main()
