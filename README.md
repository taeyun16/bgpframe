# bgpframe

Rust 기반 MRT(BGP) 파서와 Parquet 저장을 위한 실험용 모듈입니다.  
Python에서 `maturin`으로 개발 빌드하여 호출할 수 있습니다.

## 현재 설계 (스키마 요약)

문자열을 최소화하고 정수/리스트 중심으로 정규화합니다.  
IPv4/IPv6를 모두 처리할 수 있도록 IP는 버전 + 정수 컬럼으로 분리합니다.

### 핵심 컬럼

- `timestamp` (i64): 초 단위 UNIX 타임스탬프
- `elem_type` (u32): announce=1, withdraw=0
- `peer_ip_ver` (u32): 4 or 6
- `peer_ip_v4` (u32): IPv4일 때만 값 존재
- `peer_ip_v6_hi`, `peer_ip_v6_lo` (u64): IPv6일 때만 값 존재
- `peer_asn` (u32)
- `prefix_ver` (u32): 4 or 6
- `prefix_v4` (u32): IPv4일 때만 값 존재
- `prefix_v6_hi`, `prefix_v6_lo` (u64): IPv6일 때만 값 존재
- `prefix_len` (u32)
- `prefix_end_v4` (u32): IPv4 전용, 범위 쿼리 가속
- `next_hop_ver`, `next_hop_v4`, `next_hop_v6_hi`, `next_hop_v6_lo`: next hop 정보
- `as_path` (list<u32>): AS_PATH를 평탄화한 리스트
- `as_path_len` (u32): route_len 기준 길이
- `has_as_set` (bool): AS_SET / CONFED_SET 포함 여부
- `origin_asn` (u32?): 단일 origin일 때만 채움
- `local_pref` (u32?), `med` (u32?)

### 컬럼 타입 표

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

### IPv4/IPv6 저장 방식

- IPv4: `u32`로 저장 (`peer_ip_v4`, `prefix_v4`)
- IPv6: 128bit를 `u64` 두 개로 분할 (`*_v6_hi`, `*_v6_lo`)

### CIDR 포함 쿼리 유틸리티

기존 `df.filter(...)` 사용 흐름은 유지하면서, 복잡한 비트 연산만 헬퍼 함수로 숨길 수 있습니다.
핵심 계산(`ip_to_parts`, `v6_prefix_contains`)은 Rust 구현을 사용하고, `v6_contains_expr`는 `map_elements` 없이 Polars Expr로 동작합니다.

```python
import bgpframe
import polars as pl

df = pl.read_parquet("rrc00_latest.parquet")

# IPv6만 먼저 필터링한 뒤 포함 조건 적용 (기존 사용 흐름 유지)
v6 = df.filter(pl.col("prefix_ver") == 6)
res = v6.filter(bgpframe.v6_contains_expr("2001:4860:4860::8888"))
print(res.head())
```

IPv4/IPv6 구분 없이 하나의 식으로 처리할 수도 있습니다.

```python
import bgpframe
import polars as pl

df = pl.read_parquet("rrc00_latest.parquet")
res = df.filter(bgpframe.contains_prefix_expr("8.8.8.8"))
print(res.head())
```

`DataFrame` / `LazyFrame`에 바로 적용하는 헬퍼도 제공합니다.

```python
import bgpframe
import polars as pl

df = pl.read_parquet("rrc00_latest.parquet")
res = bgpframe.filter_contains(df, "2001:4860:4860::8888")
print(res.head())
```

대량 parquet를 빠르게 스캔해서 결과 parquet로 저장하려면 Rust 유틸을 그대로 사용할 수 있습니다.

```python
import bgpframe

# input parquet에서 ip를 포함하는 prefix만 output parquet로 저장
matched = bgpframe.parquet_contains_ip(
    "rrc00_latest.parquet",
    "8.8.8.8",
    output="rrc00_latest_match_8_8_8_8.parquet",
)
print("matched rows:", matched)
```

IPv6도 동일하게 동작합니다.

```python
import bgpframe

matched = bgpframe.parquet_contains_ip(
    "rrc00_latest.parquet",
    "2001:4860:4860::8888",
    output="rrc00_latest_match_google_dns_v6.parquet",
    limit=100_000,  # 선택: 상위 N개만 저장
)
print("matched rows:", matched)
```

## 개발 빌드 (dev)

### 요구 사항

- Rust toolchain (stable)
- Python 3.14+ (현재 `pyproject.toml` 기준)
- `uv`
- `maturin` (Python 패키지)

### uv 기반 (권장)

`build-system`에 `maturin`이 있어도 CLI가 자동 설치되지는 않습니다.  
따라서 `maturin`은 별도로 설치해야 합니다.

```bash
uv venv
source .venv/bin/activate
uv pip install maturin
maturin develop
```

빌드 확인:

```bash
python -c "import bgpframe; print(bgpframe.hello())"
```

가상환경 활성화 없이 바로 실행:

```bash
uv run python -c "import bgpframe; print(bgpframe.hello())"
```

가상환경 활성화 없이 빌드까지:

```bash
uv run -- maturin develop
```

### pip 기반 (대안)

```bash
python -m venv .venv
source .venv/bin/activate
pip install maturin
maturin develop
```

## 테스트

현재 리포에는 별도의 테스트가 없습니다.  
테스트가 추가되면 아래 명령으로 실행할 수 있습니다.

```bash
cargo test
```

```bash
python -m pytest
```

## 사용 예시 (mrt_to_parquet)

샘플 입력(MRT 업데이트, gzip):

- <https://data.ris.ripe.net/rrc00/latest-update.gz>

```bash
python - <<'PY'
import bgpframe

bgpframe.mrt_to_parquet(
    "https://data.ris.ripe.net/rrc00/latest-update.gz",
    "rrc00_latest.parquet",
    # limit는 테스트용; 생략 시 전체 처리
    limit=200_000,
    # 큰 파일은 배치 크기를 낮추면 메모리 사용량이 줄어듭니다.
    batch_size=100_000,
)
print("done")
PY
```
