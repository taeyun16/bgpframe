from typing import Any, Literal, overload

from polars import DataFrame, Expr, LazyFrame

IpVersion = Literal[4, 6]


def hello() -> str: ...


def mrt_to_parquet(
    input: str,
    output: str,
    limit: int | None = ...,
    batch_size: int | None = ...,
) -> int: ...


def parquet_contains_ip(
    input: str,
    ip: str,
    output: str | None = ...,
    limit: int | None = ...,
) -> int: ...


def ip_to_parts(ip: str) -> tuple[IpVersion, int | None, int | None, int | None]: ...


def v6_prefix_contains(
    prefix_hi: int | None,
    prefix_lo: int | None,
    prefix_len: int | None,
    ip_hi: int,
    ip_lo: int,
) -> bool: ...


def v6_contains_expr(
    ip: str,
    *,
    prefix_v6_hi_col: str = "prefix_v6_hi",
    prefix_v6_lo_col: str = "prefix_v6_lo",
    prefix_len_col: str = "prefix_len",
) -> Expr: ...


def contains_prefix_expr(
    ip: str,
    *,
    prefix_ver_col: str = "prefix_ver",
    prefix_v4_col: str = "prefix_v4",
    prefix_end_v4_col: str = "prefix_end_v4",
    prefix_v6_hi_col: str = "prefix_v6_hi",
    prefix_v6_lo_col: str = "prefix_v6_lo",
    prefix_len_col: str = "prefix_len",
) -> Expr: ...


@overload
def filter_contains(df_or_lf: DataFrame, ip: str, **expr_kwargs: Any) -> DataFrame: ...


@overload
def filter_contains(df_or_lf: LazyFrame, ip: str, **expr_kwargs: Any) -> LazyFrame: ...
