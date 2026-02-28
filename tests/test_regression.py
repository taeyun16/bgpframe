from __future__ import annotations

import ipaddress
import tempfile
import unittest
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import bgpframe

if TYPE_CHECKING:
    import polars as pl
else:  # pragma: no cover
    try:
        import polars as pl
    except ImportError:
        pl = cast(Any, None)

HAS_POLARS = pl is not None

def _to_u32(ip: str) -> int:
    return int(ipaddress.IPv4Address(ip))


def _v6_parts(ip: str) -> tuple[int, int]:
    value = int(ipaddress.IPv6Address(ip))
    return (value >> 64) & ((1 << 64) - 1), value & ((1 << 64) - 1)


@unittest.skipIf(not HAS_POLARS, "polars is required for regression tests")
class RegressionTests(unittest.TestCase):
    def test_ip_to_parts(self) -> None:
        self.assertEqual(bgpframe.ip_to_parts("8.8.8.8"), (4, _to_u32("8.8.8.8"), None, None))
        version, _, hi, lo = bgpframe.ip_to_parts("2001:db8::1")
        self.assertEqual(version, 6)
        self.assertIsNotNone(hi)
        self.assertIsNotNone(lo)
        self.assertRaises(RuntimeError, bgpframe.ip_to_parts, "not_an_ip")

    def test_v6_prefix_contains(self) -> None:
        prefix_hi, prefix_lo = _v6_parts("2804:41f0::")
        in_hi, in_lo = _v6_parts("2804:41f0::1234")
        out_hi, out_lo = _v6_parts("2001:db8::1")

        self.assertTrue(
            bgpframe.v6_prefix_contains(
                prefix_hi,
                prefix_lo,
                32,
                in_hi,
                in_lo,
            )
        )
        self.assertFalse(
            bgpframe.v6_prefix_contains(
                prefix_hi,
                prefix_lo,
                32,
                out_hi,
                out_lo,
            )
        )
        self.assertFalse(bgpframe.v6_prefix_contains(None, prefix_lo, 32, in_hi, in_lo))

    def test_contains_prefix_expr_and_filter_contains(self) -> None:
        v6_a_hi, v6_a_lo = _v6_parts("2804:41f0::")
        v6_b_hi, v6_b_lo = _v6_parts("2001:db8::")

        df = pl.DataFrame(
            {
                "prefix_ver": [4, 4, 6, 6],
                "prefix_v4": [
                    _to_u32("10.0.0.0"),
                    _to_u32("192.168.0.0"),
                    None,
                    None,
                ],
                "prefix_end_v4": [
                    _to_u32("10.255.255.255"),
                    _to_u32("192.168.255.255"),
                    None,
                    None,
                ],
                "prefix_v6_hi": [None, None, v6_a_hi, v6_b_hi],
                "prefix_v6_lo": [None, None, v6_a_lo, v6_b_lo],
                "prefix_len": [8, 16, 32, 32],
            },
            schema={
                "prefix_ver": pl.UInt32,
                "prefix_v4": pl.UInt32,
                "prefix_end_v4": pl.UInt32,
                "prefix_v6_hi": pl.UInt64,
                "prefix_v6_lo": pl.UInt64,
                "prefix_len": pl.UInt32,
            },
        )

        v6 = df.filter(pl.col("prefix_ver") == 6)
        self.assertEqual(len(v6.filter(bgpframe.v6_contains_expr("2804:41f0::1"))), 1)
        self.assertEqual(len(df.filter(bgpframe.contains_prefix_expr("10.1.2.3"))), 1)
        self.assertEqual(len(bgpframe.filter_contains(df.lazy(), "10.1.2.3").collect()), 1)

    def test_parquet_contains_ip(self) -> None:
        v6_a_hi, v6_a_lo = _v6_parts("2804:41f0::")
        v6_b_hi, v6_b_lo = _v6_parts("2001:db8::")

        df = pl.DataFrame(
            {
                "prefix_ver": [4, 4, 4, 6, 6],
                "prefix_v4": [
                    _to_u32("10.0.0.0"),
                    _to_u32("10.0.0.0"),
                    _to_u32("192.168.0.0"),
                    None,
                    None,
                ],
                "prefix_end_v4": [
                    _to_u32("10.255.255.255"),
                    _to_u32("10.255.255.255"),
                    _to_u32("192.168.255.255"),
                    None,
                    None,
                ],
                "prefix_v6_hi": [None, None, None, v6_a_hi, v6_b_hi],
                "prefix_v6_lo": [None, None, None, v6_a_lo, v6_b_lo],
                "prefix_len": [8, 8, 16, 32, 32],
            },
            schema={
                "prefix_ver": pl.UInt32,
                "prefix_v4": pl.UInt32,
                "prefix_end_v4": pl.UInt32,
                "prefix_v6_hi": pl.UInt64,
                "prefix_v6_lo": pl.UInt64,
                "prefix_len": pl.UInt32,
            },
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            input_path = tmp / "input.parquet"
            out_path = tmp / "out.parquet"
            df.write_parquet(input_path)

            count_v4 = bgpframe.parquet_contains_ip(str(input_path), "10.1.1.1")
            self.assertEqual(count_v4, 2)

            count_v6 = bgpframe.parquet_contains_ip(str(input_path), "2804:41f0::abcd")
            self.assertEqual(count_v6, 1)

            count_limited = bgpframe.parquet_contains_ip(
                str(input_path),
                "10.1.1.1",
                output=str(out_path),
                limit=1,
            )
            self.assertEqual(count_limited, 1)

            out_df = pl.read_parquet(out_path)
            self.assertEqual(len(out_df), 1)

    def test_parquet_filter_updates(self) -> None:
        v6_a_hi, v6_a_lo = _v6_parts("2001:db8::")
        v6_b_hi, v6_b_lo = _v6_parts("2001:4860::")

        df = pl.DataFrame(
            {
                "elem_type": [1, 0, 1, 1, 1],
                "prefix_ver": [4, 4, 4, 6, 6],
                "prefix_v4": [
                    _to_u32("10.0.0.0"),
                    _to_u32("10.1.0.0"),
                    _to_u32("192.168.0.0"),
                    None,
                    None,
                ],
                "prefix_end_v4": [
                    _to_u32("10.255.255.255"),
                    _to_u32("10.1.255.255"),
                    _to_u32("192.168.255.255"),
                    None,
                    None,
                ],
                "prefix_v6_hi": [None, None, None, v6_a_hi, v6_b_hi],
                "prefix_v6_lo": [None, None, None, v6_a_lo, v6_b_lo],
                "prefix_len": [8, 16, 16, 32, 32],
                "origin_asn": [64500, 64501, 64500, 64500, None],
                "as_path": [
                    [64512, 64500],
                    [64513, 64501],
                    [64514, 64500],
                    [64515, 64500],
                    [],
                ],
                "as_path_len": [2, 2, 2, 2, 0],
            },
            schema={
                "elem_type": pl.UInt32,
                "prefix_ver": pl.UInt32,
                "prefix_v4": pl.UInt32,
                "prefix_end_v4": pl.UInt32,
                "prefix_v6_hi": pl.UInt64,
                "prefix_v6_lo": pl.UInt64,
                "prefix_len": pl.UInt32,
                "origin_asn": pl.UInt32,
                "as_path": pl.List(pl.UInt32),
                "as_path_len": pl.UInt32,
            },
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            input_path = tmp / "input.parquet"
            output_path = tmp / "out.parquet"
            df.write_parquet(input_path)

            count_v4 = bgpframe.parquet_filter_updates(
                str(input_path),
                contains_ip="10.1.1.1",
                elem_type="announce",
                origin_asn=64500,
                as_path_contains=64500,
                min_as_path_len=2,
                max_as_path_len=2,
            )
            self.assertEqual(count_v4, 1)

            count_v6 = bgpframe.parquet_filter_updates(
                str(input_path),
                exact_prefix="2001:db8::1234/32",
            )
            self.assertEqual(count_v6, 1)

            count_limited = bgpframe.parquet_filter_updates(
                str(input_path),
                output=str(output_path),
                limit=1,
                elem_type="announce",
            )
            self.assertEqual(count_limited, 1)
            self.assertEqual(len(pl.read_parquet(output_path)), 1)

            with self.assertRaises(RuntimeError):
                bgpframe.parquet_filter_updates(
                    str(input_path),
                    min_as_path_len=3,
                    max_as_path_len=2,
                )

    def test_bgp_convenience_filters(self) -> None:
        v6_a_hi, v6_a_lo = _v6_parts("2001:db8::")
        v6_b_hi, v6_b_lo = _v6_parts("2001:4860::")

        df = pl.DataFrame(
            {
                "elem_type": [1, 0, 1, 1],
                "prefix_ver": [4, 4, 6, 6],
                "prefix_v4": [
                    _to_u32("10.0.0.0"),
                    _to_u32("10.1.0.0"),
                    None,
                    None,
                ],
                "prefix_end_v4": [
                    _to_u32("10.255.255.255"),
                    _to_u32("10.1.255.255"),
                    None,
                    None,
                ],
                "prefix_v6_hi": [None, None, v6_a_hi, v6_b_hi],
                "prefix_v6_lo": [None, None, v6_a_lo, v6_b_lo],
                "prefix_len": [8, 16, 32, 32],
                "origin_asn": [64500, 64501, None, 64500],
                "as_path": [
                    [64512, 64500],
                    [64513, 64501],
                    [],
                    [64514, 64515, 64500],
                ],
                "as_path_len": [2, 2, 0, 3],
            },
            schema={
                "elem_type": pl.UInt32,
                "prefix_ver": pl.UInt32,
                "prefix_v4": pl.UInt32,
                "prefix_end_v4": pl.UInt32,
                "prefix_v6_hi": pl.UInt64,
                "prefix_v6_lo": pl.UInt64,
                "prefix_len": pl.UInt32,
                "origin_asn": pl.UInt32,
                "as_path": pl.List(pl.UInt32),
                "as_path_len": pl.UInt32,
            },
        )

        self.assertEqual(len(df.filter(bgpframe.announce_expr())), 3)
        self.assertEqual(len(df.filter(bgpframe.withdraw_expr())), 1)
        self.assertEqual(len(df.filter(bgpframe.origin_asn_expr(64500))), 2)
        self.assertEqual(len(df.filter(bgpframe.as_path_contains_expr(64515))), 1)
        self.assertEqual(len(df.filter(bgpframe.as_path_len_between_expr(min_len=3))), 1)
        self.assertEqual(len(df.filter(bgpframe.prefix_exact_expr("10.23.9.9/8"))), 1)
        self.assertEqual(len(df.filter(bgpframe.prefix_exact_expr("2001:db8::1234/32"))), 1)

        res = bgpframe.filter_bgp_updates(
            df,
            contains_ip="10.1.1.1",
            elem_type="withdraw",
        )
        self.assertEqual(len(res), 1)

        lazy_res = bgpframe.filter_bgp_updates(
            df.lazy(),
            origin_asn=64500,
            as_path_contains=64500,
            min_as_path_len=2,
            elem_type="announce",
        ).collect()
        self.assertEqual(len(lazy_res), 2)

        with self.assertRaises(ValueError):
            bgpframe.as_path_len_between_expr()
        with self.assertRaises(ValueError):
            bgpframe.filter_bgp_updates(df, elem_type="unknown")


if __name__ == "__main__":
    unittest.main()
