from __future__ import annotations

import ipaddress
import tempfile
import unittest
from pathlib import Path

import bgpframe

try:
    import polars as pl
except ImportError:  # pragma: no cover
    pl = None


def _to_u32(ip: str) -> int:
    return int(ipaddress.IPv4Address(ip))


def _v6_parts(ip: str) -> tuple[int, int]:
    value = int(ipaddress.IPv6Address(ip))
    return (value >> 64) & ((1 << 64) - 1), value & ((1 << 64) - 1)


@unittest.skipIf(pl is None, "polars is required for regression tests")
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


if __name__ == "__main__":
    unittest.main()
