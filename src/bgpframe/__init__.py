from bgpframe._core import hello_from_bin, mrt_to_parquet


def hello() -> str:
    return hello_from_bin()


__all__ = ["hello", "mrt_to_parquet"]
