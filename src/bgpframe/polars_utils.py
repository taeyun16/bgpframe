"""Polars expression helpers for prefix containment queries.

Doctest:
    >>> from bgpframe.polars_utils import ip_to_parts
    >>> ip_to_parts("8.8.8.8")
    (4, 134744072, None, None)
    >>> ip_to_parts("2001:db8::1")[0]
    6
"""

from __future__ import annotations

import ipaddress
from typing import Any

from bgpframe._core import ip_to_parts as _ip_to_parts_core


def _polars() -> Any:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError(
            "polars is required for bgpframe.polars utils. "
            "Install with: pip install 'bgpframe[polars]' or pip install polars"
        ) from exc
    return pl


def ip_to_parts(ip: str) -> tuple[int, int | None, int | None, int | None]:
    """Parse IP string into normalized parts.

    Returns `(version, v4, v6_hi, v6_lo)` where only one address family is populated.

    >>> ip_to_parts("1.1.1.1")
    (4, 16843009, None, None)
    """
    return _ip_to_parts_core(ip)


def v6_contains_expr(
    ip: str,
    *,
    prefix_v6_hi_col: str = "prefix_v6_hi",
    prefix_v6_lo_col: str = "prefix_v6_lo",
    prefix_len_col: str = "prefix_len",
) -> Any:
    pl = _polars()
    version, _, ip_hi, ip_lo = ip_to_parts(ip)
    if version != 6 or ip_hi is None or ip_lo is None:
        raise ValueError(f"v6_contains_expr expects an IPv6 address, got: {ip}")

    plen = pl.col(prefix_len_col).cast(pl.Int64)

    # For /1..64, compare high 64 bits after masking shared prefix bits.
    exp_hi = (pl.lit(64) - plen).clip(0, 63).cast(pl.UInt32)
    lo_bits_hi = pl.lit(2, dtype=pl.UInt64).pow(exp_hi) - pl.lit(1, dtype=pl.UInt64)
    mask_hi = pl.lit(18446744073709551615, dtype=pl.UInt64) - lo_bits_hi
    hi_match = (
        (pl.col(prefix_v6_hi_col).cast(pl.UInt64) & mask_hi)
        == (pl.lit(ip_hi, dtype=pl.UInt64) & mask_hi)
    )

    # For /65..128, high 64 bits must match exactly, then compare low-part mask.
    plen_lo = (plen - 64).clip(1, 64)
    exp_lo = (pl.lit(64) - plen_lo).clip(0, 63).cast(pl.UInt32)
    lo_bits_lo = pl.lit(2, dtype=pl.UInt64).pow(exp_lo) - pl.lit(1, dtype=pl.UInt64)
    mask_lo = pl.lit(18446744073709551615, dtype=pl.UInt64) - lo_bits_lo
    lo_match = (
        (pl.col(prefix_v6_lo_col).cast(pl.UInt64) & mask_lo)
        == (pl.lit(ip_lo, dtype=pl.UInt64) & mask_lo)
    )

    return (
        pl.when(pl.col(prefix_len_col) == 0)
        .then(True)
        .when(pl.col(prefix_len_col) <= 64)
        .then(hi_match)
        .otherwise((pl.col(prefix_v6_hi_col) == pl.lit(ip_hi, dtype=pl.UInt64)) & lo_match)
        .fill_null(False)
    )


def contains_prefix_expr(
    ip: str,
    *,
    prefix_ver_col: str = "prefix_ver",
    prefix_v4_col: str = "prefix_v4",
    prefix_end_v4_col: str = "prefix_end_v4",
    prefix_v6_hi_col: str = "prefix_v6_hi",
    prefix_v6_lo_col: str = "prefix_v6_lo",
    prefix_len_col: str = "prefix_len",
) -> Any:
    pl = _polars()
    version, ip_v4, ip_hi, ip_lo = ip_to_parts(ip)

    if version == 4 and ip_v4 is not None:
        return (
            (pl.col(prefix_ver_col) == 4)
            & (pl.col(prefix_v4_col) <= pl.lit(ip_v4))
            & (pl.col(prefix_end_v4_col) >= pl.lit(ip_v4))
        ).fill_null(False)

    if version == 6 and ip_hi is not None and ip_lo is not None:
        return (
            (pl.col(prefix_ver_col) == 6)
            & v6_contains_expr(
                ip,
                prefix_v6_hi_col=prefix_v6_hi_col,
                prefix_v6_lo_col=prefix_v6_lo_col,
                prefix_len_col=prefix_len_col,
            )
        ).fill_null(False)

    raise ValueError(f"unsupported IP address: {ip}")


def filter_contains(df_or_lf: Any, ip: str, **expr_kwargs: Any) -> Any:
    return df_or_lf.filter(contains_prefix_expr(ip, **expr_kwargs))


def announce_expr(*, elem_type_col: str = "elem_type") -> Any:
    pl = _polars()
    return (pl.col(elem_type_col) == pl.lit(1)).fill_null(False)


def withdraw_expr(*, elem_type_col: str = "elem_type") -> Any:
    pl = _polars()
    return (pl.col(elem_type_col) == pl.lit(0)).fill_null(False)


def origin_asn_expr(origin_asn: int, *, origin_asn_col: str = "origin_asn") -> Any:
    pl = _polars()
    return (pl.col(origin_asn_col) == pl.lit(origin_asn)).fill_null(False)


def as_path_contains_expr(asn: int, *, as_path_col: str = "as_path") -> Any:
    pl = _polars()
    return pl.col(as_path_col).list.contains(pl.lit(asn)).fill_null(False)


def as_path_len_between_expr(
    *,
    min_len: int | None = None,
    max_len: int | None = None,
    as_path_len_col: str = "as_path_len",
) -> Any:
    if min_len is None and max_len is None:
        raise ValueError("at least one of min_len or max_len must be provided")

    pl = _polars()
    expr = pl.lit(True)
    if min_len is not None:
        expr = expr & (pl.col(as_path_len_col) >= pl.lit(min_len))
    if max_len is not None:
        expr = expr & (pl.col(as_path_len_col) <= pl.lit(max_len))
    return expr.fill_null(False)


def _prefix_to_parts(prefix: str) -> tuple[int, int | None, int | None, int | None, int]:
    try:
        network = ipaddress.ip_network(prefix, strict=False)
    except ValueError as exc:
        raise ValueError(f"invalid prefix: {prefix}") from exc

    if isinstance(network, ipaddress.IPv4Network):
        return (4, int(network.network_address), None, None, network.prefixlen)

    value = int(network.network_address)
    hi = (value >> 64) & ((1 << 64) - 1)
    lo = value & ((1 << 64) - 1)
    return (6, None, hi, lo, network.prefixlen)


def prefix_exact_expr(
    prefix: str,
    *,
    prefix_ver_col: str = "prefix_ver",
    prefix_v4_col: str = "prefix_v4",
    prefix_v6_hi_col: str = "prefix_v6_hi",
    prefix_v6_lo_col: str = "prefix_v6_lo",
    prefix_len_col: str = "prefix_len",
) -> Any:
    pl = _polars()
    version, v4, v6_hi, v6_lo, prefix_len = _prefix_to_parts(prefix)

    if version == 4 and v4 is not None:
        return (
            (pl.col(prefix_ver_col) == pl.lit(4))
            & (pl.col(prefix_v4_col) == pl.lit(v4))
            & (pl.col(prefix_len_col) == pl.lit(prefix_len))
        ).fill_null(False)

    if version == 6 and v6_hi is not None and v6_lo is not None:
        return (
            (pl.col(prefix_ver_col) == pl.lit(6))
            & (pl.col(prefix_v6_hi_col) == pl.lit(v6_hi, dtype=pl.UInt64))
            & (pl.col(prefix_v6_lo_col) == pl.lit(v6_lo, dtype=pl.UInt64))
            & (pl.col(prefix_len_col) == pl.lit(prefix_len))
        ).fill_null(False)

    raise ValueError(f"unsupported prefix: {prefix}")


def filter_bgp_updates(
    df_or_lf: Any,
    *,
    contains_ip: str | None = None,
    exact_prefix: str | None = None,
    origin_asn: int | None = None,
    as_path_contains: int | None = None,
    min_as_path_len: int | None = None,
    max_as_path_len: int | None = None,
    elem_type: str | None = None,
) -> Any:
    pl = _polars()
    expr = pl.lit(True)

    if contains_ip is not None:
        expr = expr & contains_prefix_expr(contains_ip)
    if exact_prefix is not None:
        expr = expr & prefix_exact_expr(exact_prefix)
    if origin_asn is not None:
        expr = expr & origin_asn_expr(origin_asn)
    if as_path_contains is not None:
        expr = expr & as_path_contains_expr(as_path_contains)
    if min_as_path_len is not None or max_as_path_len is not None:
        expr = expr & as_path_len_between_expr(min_len=min_as_path_len, max_len=max_as_path_len)
    if elem_type is not None:
        normalized = elem_type.strip().lower()
        if normalized in {"announce", "announcement", "a", "1"}:
            expr = expr & announce_expr()
        elif normalized in {"withdraw", "withdrawal", "w", "0"}:
            expr = expr & withdraw_expr()
        else:
            raise ValueError(f"elem_type must be announce/withdraw, got: {elem_type}")

    return df_or_lf.filter(expr.fill_null(False))
