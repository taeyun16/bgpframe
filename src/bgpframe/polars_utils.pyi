from typing import Any, Literal, overload

from polars import DataFrame, Expr, LazyFrame

IpVersion = Literal[4, 6]


def ip_to_parts(ip: str) -> tuple[IpVersion, int | None, int | None, int | None]: ...


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
