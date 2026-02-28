from bgpframe._core import (
    hello_from_bin,
    ip_to_parts,
    mrt_to_parquet,
    parquet_contains_ip,
    v6_prefix_contains,
)
from bgpframe.polars_utils import (
    contains_prefix_expr,
    filter_contains,
    v6_contains_expr,
)


def hello() -> str:
    return hello_from_bin()


__all__ = [
    "hello",
    "mrt_to_parquet",
    "parquet_contains_ip",
    "ip_to_parts",
    "v6_prefix_contains",
    "v6_contains_expr",
    "contains_prefix_expr",
    "filter_contains",
]
