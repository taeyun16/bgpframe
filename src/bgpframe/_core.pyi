from typing import Literal

IpVersion = Literal[4, 6]


def hello_from_bin() -> str: ...
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
