"""Polars expression helpers for prefix containment queries.

Doctest:
    >>> from bgpframe.polars_utils import ip_to_parts
    >>> ip_to_parts("8.8.8.8")
    (4, 134744072, None, None)
    >>> ip_to_parts("2001:db8::1")[0]
    6
"""

from __future__ import annotations

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
