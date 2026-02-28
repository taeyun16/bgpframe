# bgpframe

[![Rust 2024 Edition](https://img.shields.io/badge/Rust-2024%20Edition-000000?style=for-the-badge&logo=rust)](https://www.rust-lang.org/)
[![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PyO3 0.27](https://img.shields.io/badge/PyO3-0.27-F9AB00?style=for-the-badge)](https://pyo3.rs/)
[![Polars 1.36+](https://img.shields.io/badge/Polars-1.36%2B-0F172A?style=for-the-badge)](https://pola.rs/)
[![GitHub Repo](https://img.shields.io/badge/GitHub-taeyun16%2Fbgpframe-181717?style=for-the-badge&logo=github)](https://github.com/taeyun16/bgpframe)
[![Default Branch](https://img.shields.io/badge/Default%20branch-main-0969da?style=for-the-badge)](https://github.com/taeyun16/bgpframe/tree/main)
[![Project Scope](https://img.shields.io/badge/Scope-BGP%20MRT%20%2B%20Parquet-0052cc?style=for-the-badge)](https://github.com/taeyun16/bgpframe)
[![Package Type](https://img.shields.io/badge/Package-Python%20%2B%20Rust-2ea44f?style=for-the-badge)](https://github.com/taeyun16/bgpframe)
[![Status](https://img.shields.io/badge/Status-Active-2ea44f?style=for-the-badge)](https://github.com/taeyun16/bgpframe)
[![Rust tests](https://img.shields.io/badge/Rust%20tests-7%20passed-2ea44f?style=for-the-badge)](#testing--quality-gates)
[![Python tests](https://img.shields.io/badge/Python%20tests-6%20passed-2ea44f?style=for-the-badge)](#testing--quality-gates)
[![Doctest](https://img.shields.io/badge/Doctest-4%20passed-2ea44f?style=for-the-badge)](#testing--quality-gates)
[![Coverage](https://img.shields.io/badge/Coverage-93%25-2ea44f?style=for-the-badge)](#testing--quality-gates)
[![Type Check](https://img.shields.io/badge/Type%20check-pyrefly%200%20errors-2ea44f?style=for-the-badge)](#testing--quality-gates)

English documentation.
Korean version: [`docs/README.ko.md`](docs/README.ko.md)

A Rust-based MRT (BGP) parsing + Parquet processing library.
You can build and use it directly from Python with `maturin`, and write concise prefix containment queries with Polars expressions.

## Key Highlights

- Fast parsing: converts MRT to Parquet using `bgpkit-parser` + Rust implementation.
- Memory reuse: reduces allocation/copy overhead with batch buffer swap during flush.
- Rust scan filter: `parquet_filter_updates` for fast direct filtering/writing on large Parquet files.
- Python-friendly API: prefix/IP containment + BGP-specific filters (`announce`, `origin`, `as_path`).
- Typed API: includes `.pyi` stubs (`_core.pyi`, `polars_utils.pyi`, `__init__.pyi`).

## Installation

Install from PyPI:

```bash
pip install bgpframe
```

If you use the Polars helper expressions:

```bash
pip install "bgpframe[polars]"
```

Using `uv`:

```bash
uv add bgpframe
# or
uv add "bgpframe[polars]"
```

### Requirements

- Rust stable toolchain
- Python 3.12+
- `uv`
- `maturin`

### Development Build

```bash
uv venv
source .venv/bin/activate
uv pip install maturin
maturin develop
python -c "import bgpframe; print(bgpframe.hello())"
```

Run without activating the virtual environment:

```bash
uv run -- maturin develop
uv run python -c "import bgpframe; print(bgpframe.hello())"
```

## Examples

### 1) MRT -> Parquet

```python
import bgpframe

bgpframe.mrt_to_parquet(
    "https://data.ris.ripe.net/rrc00/latest-update.gz",
    "rrc00_latest.parquet",
    limit=200_000,      # optional
    batch_size=100_000, # optional
)
```

### 2) Prefix containment query (IPv4/IPv6)

```python
import bgpframe
import polars as pl

df = pl.read_parquet("rrc00_latest.parquet")
res = df.filter(bgpframe.contains_prefix_expr("8.8.8.8"))
print(res.head())
```

### 3) Filter large Parquet and write output

```python
import bgpframe

matched = bgpframe.parquet_contains_ip(
    "rrc00_latest.parquet",
    "2001:4860:4860::8888",
    output="rrc00_latest_match_google_dns_v6.parquet",
    limit=100_000,  # optional
)
print("matched rows:", matched)
```

### 4) Combined BGP convenience filters

```python
import bgpframe
import polars as pl

df = pl.read_parquet("rrc00_latest.parquet")

# announce + origin AS 15169 + AS_PATH includes 3356 + path length 2..5
res = bgpframe.filter_bgp_updates(
    df,
    elem_type="announce",
    origin_asn=15169,
    as_path_contains=3356,
    min_as_path_len=2,
    max_as_path_len=5,
)

# exact prefix match (host bits are normalized with strict=False behavior)
exact = df.filter(bgpframe.prefix_exact_expr("2001:4860:4860::8888/32"))
```

### 5) Rust high-speed scan filter (file -> file)

```python
import bgpframe

matched = bgpframe.parquet_filter_updates(
    "rrc00_latest.parquet",
    output="rrc00_latest_updates_filtered.parquet",
    contains_ip="8.8.8.8",
    elem_type="announce",
    origin_asn=15169,
    as_path_contains=3356,
    min_as_path_len=2,
    max_as_path_len=8,
    limit=50_000,
)
print("matched rows:", matched)
```

The same code is available at `example/parquet_filter_updates.py`.

## Recommended Query Patterns for BGP Data

- Split event types: `announce_expr()`, `withdraw_expr()`
- Analyze route origin: `origin_asn_expr(asn)`
- Track transit/upstream ASN: `as_path_contains_expr(asn)`
- Find policy/risk signals: `as_path_len_between_expr(min_len=..., max_len=...)`
- Exact prefix comparisons: `prefix_exact_expr("x.x.x.x/len")`
- Apply combined filters once: `filter_bgp_updates(...)`
- Direct Parquet processing: `parquet_filter_updates(...)`

## Testing / Quality Gates

Results below are from local runs on **2026-03-01 (Asia/Seoul)**.

- Rust unit tests: `7 passed`
- Rust doc tests: `0 failed`
- Python regression tests (`unittest`): `6 passed`
- Python doctest: `4 passed`
- Coverage (Python): `93%`
- Type check (`pyrefly`): `0 errors`

Run commands:

```bash
# One-time workaround if cargo test has macOS + Homebrew Python framework link issue
mkdir -p /tmp/Python3.framework/Versions/3.9
ln -sf /opt/homebrew/Frameworks/Python.framework/Versions/Current/Python /tmp/Python3.framework/Versions/3.9/Python3

# Rust tests
DYLD_FRAMEWORK_PATH=/tmp cargo test --lib
DYLD_FRAMEWORK_PATH=/tmp cargo test --doc

# Python tests + doctest
uv run python -m unittest -v tests.test_regression
uv run python -m doctest -v src/bgpframe/polars_utils.py

# Coverage
uv run coverage erase
uv run coverage run -m unittest tests.test_regression
uv run coverage run -a -m doctest src/bgpframe/polars_utils.py
uv run coverage report

# Type check
uv run pyrefly check
```

## CI/CD and PyPI Publishing

- CI workflow: `.github/workflows/ci.yml`
  - Trigger: push to `main`, pull requests
  - Runs: Rust tests, Python regression tests, doctest, type checks
- Release workflow: `.github/workflows/publish-pypi.yml`
  - Trigger: GitHub Release (`published`) or manual dispatch
  - Builds: wheels (`ubuntu/macos/windows`) + sdist
  - Publishes: PyPI via Trusted Publishing (OIDC)

### Required setup for PyPI release

1. Configure a PyPI Trusted Publisher for this project.
2. In PyPI Trusted Publisher settings, use:
   - Owner: `taeyun16`
   - Repository: `bgpframe`
   - Workflow filename: `publish-pypi.yml`
   - Environment name: `pypi`
3. In GitHub, create environment `pypi` (Settings -> Environments).
4. Create a GitHub Release (for example tag `v0.1.0`) to trigger publish.

With Trusted Publishing, you do not need a long-lived `PYPI_API_TOKEN` secret.

## Schema Summary

The schema is normalized to numeric/list columns and minimizes string fields.

| Column | Type | Description |
|---|---|---|
| `timestamp` | i64 | Unix timestamp in seconds |
| `elem_type` | u32 | announce=1, withdraw=0 |
| `peer_ip_ver` | u32 | 4 or 6 |
| `peer_ip_v4` | u32? | Present only for IPv4 peers |
| `peer_ip_v6_hi` | u64? | Upper 64 bits of IPv6 |
| `peer_ip_v6_lo` | u64? | Lower 64 bits of IPv6 |
| `peer_asn` | u32 | Peer ASN |
| `prefix_ver` | u32 | 4 or 6 |
| `prefix_v4` | u32? | IPv4 prefix |
| `prefix_v6_hi` | u64? | Upper 64 bits of IPv6 |
| `prefix_v6_lo` | u64? | Lower 64 bits of IPv6 |
| `prefix_len` | u32 | Prefix length |
| `prefix_end_v4` | u32? | IPv4 range end (query acceleration) |
| `next_hop_ver` | u32? | 4 or 6 |
| `next_hop_v4` | u32? | IPv4 next hop |
| `next_hop_v6_hi` | u64? | Upper 64 bits of IPv6 |
| `next_hop_v6_lo` | u64? | Lower 64 bits of IPv6 |
| `as_path` | list<u32> | Flattened AS_PATH |
| `as_path_len` | u32 | Route length |
| `has_as_set` | bool | Contains AS_SET/CONFED_SET |
| `origin_asn` | u32? | Present only for a single origin ASN |
| `local_pref` | u32? | local-pref |
| `med` | u32? | MED |
