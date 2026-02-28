# bgpframe

[![Rust 2024 Edition](https://img.shields.io/badge/Rust-2024%20Edition-000000?style=for-the-badge&logo=rust)](https://www.rust-lang.org/)
[![Python 3.14+](https://img.shields.io/badge/Python-3.14%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PyO3 0.27](https://img.shields.io/badge/PyO3-0.27-F9AB00?style=for-the-badge)](https://pyo3.rs/)
[![Polars 1.36+](https://img.shields.io/badge/Polars-1.36%2B-0F172A?style=for-the-badge)](https://pola.rs/)
[![Stars](https://img.shields.io/github/stars/taeyun16/bgpframe?style=for-the-badge)](https://github.com/taeyun16/bgpframe/stargazers)
[![Forks](https://img.shields.io/github/forks/taeyun16/bgpframe?style=for-the-badge)](https://github.com/taeyun16/bgpframe/network/members)
[![Issues](https://img.shields.io/github/issues/taeyun16/bgpframe?style=for-the-badge)](https://github.com/taeyun16/bgpframe/issues)
[![Last Commit](https://img.shields.io/github/last-commit/taeyun16/bgpframe?style=for-the-badge)](https://github.com/taeyun16/bgpframe/commits/main)
[![Top Language](https://img.shields.io/github/languages/top/taeyun16/bgpframe?style=for-the-badge)](https://github.com/taeyun16/bgpframe)
[![Rust tests](https://img.shields.io/badge/Rust%20tests-5%20passed-2ea44f?style=for-the-badge)](#테스트--품질-게이트)
[![Python tests](https://img.shields.io/badge/Python%20tests-4%20passed-2ea44f?style=for-the-badge)](#테스트--품질-게이트)
[![Doctest](https://img.shields.io/badge/Doctest-4%20passed-2ea44f?style=for-the-badge)](#테스트--품질-게이트)
[![Coverage](https://img.shields.io/badge/Coverage-92%25-2ea44f?style=for-the-badge)](#테스트--품질-게이트)
[![Type Check](https://img.shields.io/badge/Type%20check-pyrefly%200%20errors-2ea44f?style=for-the-badge)](#테스트--품질-게이트)

Rust 기반 MRT(BGP) 파서 + Parquet 처리 라이브러리입니다.  
Python에서 `maturin`으로 바로 개발 빌드해서 사용할 수 있고, prefix 포함 쿼리를 Polars Expr로 간결하게 작성할 수 있습니다.

## 핵심 포인트

- 고속 파싱: `bgpkit-parser` + Rust 구현으로 MRT를 Parquet로 변환
- 메모리 재사용: 배치 플러시 시 버퍼 스왑 방식으로 할당/복사 비용 절감
- Python 친화 API: `contains_prefix_expr`, `v6_contains_expr`, `filter_contains`
- 타입 힌트 지원: `typed .pyi` 포함 (`_core.pyi`, `polars_utils.pyi`, `__init__.pyi`)

## 빠른 시작

### 요구 사항

- Rust stable toolchain
- Python 3.14+
- `uv`
- `maturin`

### 개발 빌드

```bash
uv venv
source .venv/bin/activate
uv pip install maturin
maturin develop
python -c "import bgpframe; print(bgpframe.hello())"
```

가상환경 활성화 없이 실행하려면:

```bash
uv run -- maturin develop
uv run python -c "import bgpframe; print(bgpframe.hello())"
```

## 사용 예시

### 1) MRT -> Parquet

```python
import bgpframe

bgpframe.mrt_to_parquet(
    "https://data.ris.ripe.net/rrc00/latest-update.gz",
    "rrc00_latest.parquet",
    limit=200_000,      # 선택
    batch_size=100_000, # 선택
)
```

### 2) Prefix 포함 쿼리 (IPv4/IPv6 공통)

```python
import bgpframe
import polars as pl

df = pl.read_parquet("rrc00_latest.parquet")
res = df.filter(bgpframe.contains_prefix_expr("8.8.8.8"))
print(res.head())
```

### 3) 대용량 parquet 직접 필터링 후 저장

```python
import bgpframe

matched = bgpframe.parquet_contains_ip(
    "rrc00_latest.parquet",
    "2001:4860:4860::8888",
    output="rrc00_latest_match_google_dns_v6.parquet",
    limit=100_000,  # 선택
)
print("matched rows:", matched)
```

## 테스트 / 품질 게이트

아래 결과는 **2026-03-01(Asia/Seoul) 로컬 실행 기준**입니다.

- Rust unit test: `5 passed`
- Rust doc test: `0 failed`
- Python regression test (`unittest`): `4 passed`
- Python doctest: `4 passed`
- Coverage (Python): `92%`
- Type check (`pyrefly`): `0 errors`

실행 명령:

```bash
# macOS + Homebrew Python 환경에서 cargo test 링크 에러가 있으면 1회 준비
mkdir -p /tmp/Python3.framework/Versions/3.9
ln -sf /opt/homebrew/Frameworks/Python.framework/Versions/3.14/Python /tmp/Python3.framework/Versions/3.9/Python3

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

## 스키마 요약

문자열을 최소화하고 정수/리스트 중심으로 정규화합니다.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `timestamp` | i64 | 초 단위 UNIX 타임스탬프 |
| `elem_type` | u32 | announce=1, withdraw=0 |
| `peer_ip_ver` | u32 | 4 or 6 |
| `peer_ip_v4` | u32? | IPv4일 때만 값 존재 |
| `peer_ip_v6_hi` | u64? | IPv6 상위 64bit |
| `peer_ip_v6_lo` | u64? | IPv6 하위 64bit |
| `peer_asn` | u32 | Peer ASN |
| `prefix_ver` | u32 | 4 or 6 |
| `prefix_v4` | u32? | IPv4 prefix |
| `prefix_v6_hi` | u64? | IPv6 상위 64bit |
| `prefix_v6_lo` | u64? | IPv6 하위 64bit |
| `prefix_len` | u32 | Prefix 길이 |
| `prefix_end_v4` | u32? | IPv4 범위 끝 (쿼리 가속) |
| `next_hop_ver` | u32? | 4 or 6 |
| `next_hop_v4` | u32? | IPv4 next hop |
| `next_hop_v6_hi` | u64? | IPv6 상위 64bit |
| `next_hop_v6_lo` | u64? | IPv6 하위 64bit |
| `as_path` | list<u32> | AS_PATH 평탄화 리스트 |
| `as_path_len` | u32 | route_len 기준 길이 |
| `has_as_set` | bool | AS_SET/CONFED_SET 포함 여부 |
| `origin_asn` | u32? | 단일 origin일 때만 채움 |
| `local_pref` | u32? | local-pref |
| `med` | u32? | MED |
