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


def parquet_filter_updates(
    input: str,
    output: str | None = ...,
    limit: int | None = ...,
    contains_ip: str | None = ...,
    exact_prefix: str | None = ...,
    origin_asn: int | None = ...,
    as_path_contains: int | None = ...,
    min_as_path_len: int | None = ...,
    max_as_path_len: int | None = ...,
    elem_type: str | None = ...,
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


def announce_expr(*, elem_type_col: str = "elem_type") -> Expr: ...


def withdraw_expr(*, elem_type_col: str = "elem_type") -> Expr: ...


def origin_asn_expr(origin_asn: int, *, origin_asn_col: str = "origin_asn") -> Expr: ...


def as_path_contains_expr(asn: int, *, as_path_col: str = "as_path") -> Expr: ...


def as_path_len_between_expr(
    *,
    min_len: int | None = None,
    max_len: int | None = None,
    as_path_len_col: str = "as_path_len",
) -> Expr: ...


def prefix_exact_expr(
    prefix: str,
    *,
    prefix_ver_col: str = "prefix_ver",
    prefix_v4_col: str = "prefix_v4",
    prefix_v6_hi_col: str = "prefix_v6_hi",
    prefix_v6_lo_col: str = "prefix_v6_lo",
    prefix_len_col: str = "prefix_len",
) -> Expr: ...


@overload
def filter_bgp_updates(
    df_or_lf: DataFrame,
    *,
    contains_ip: str | None = None,
    exact_prefix: str | None = None,
    origin_asn: int | None = None,
    as_path_contains: int | None = None,
    min_as_path_len: int | None = None,
    max_as_path_len: int | None = None,
    elem_type: str | None = None,
) -> DataFrame: ...


@overload
def filter_bgp_updates(
    df_or_lf: LazyFrame,
    *,
    contains_ip: str | None = None,
    exact_prefix: str | None = None,
    origin_asn: int | None = None,
    as_path_contains: int | None = None,
    min_as_path_len: int | None = None,
    max_as_path_len: int | None = None,
    elem_type: str | None = None,
) -> LazyFrame: ...
