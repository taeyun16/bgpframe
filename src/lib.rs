use bgpkit_parser::{
    BgpkitParser,
    models::{AsPathSegment, ElemType},
};
use polars::io::parquet::write::BatchedWriter;
use polars::prelude::*;
use polars::prelude::{ParquetCompression, ZstdLevel};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::fs::File;
use std::net::{IpAddr, Ipv6Addr};

fn ipv6_to_u64s(addr: Ipv6Addr) -> (u64, u64) {
    let octets = addr.octets();
    let hi = u64::from_be_bytes(octets[0..8].try_into().unwrap());
    let lo = u64::from_be_bytes(octets[8..16].try_into().unwrap());
    (hi, lo)
}

fn split_ip(ip: IpAddr) -> (u8, Option<u32>, Option<u64>, Option<u64>) {
    match ip {
        IpAddr::V4(addr) => (4, Some(u32::from(addr)), None, None),
        IpAddr::V6(addr) => {
            let (hi, lo) = ipv6_to_u64s(addr);
            (6, None, Some(hi), Some(lo))
        }
    }
}

fn prefix_end_v4(start: u32, prefix_len: u8) -> u32 {
    if prefix_len == 0 {
        return u32::MAX;
    }

    let host_bits = 32u32 - prefix_len as u32;
    let size = (1u64 << host_bits) - 1;
    (start as u64 + size) as u32
}

enum ParsedIp {
    V4(u32),
    V6(u64, u64),
}

fn parse_ip(ip: &str) -> PyResult<ParsedIp> {
    let addr = ip
        .parse::<IpAddr>()
        .map_err(|err| PyRuntimeError::new_err(format!("invalid ip {ip}: {err}")))?;
    match addr {
        IpAddr::V4(v4) => Ok(ParsedIp::V4(u32::from(v4))),
        IpAddr::V6(v6) => {
            let (hi, lo) = ipv6_to_u64s(v6);
            Ok(ParsedIp::V6(hi, lo))
        }
    }
}

fn prefix_contains_v6(
    prefix_hi: u64,
    prefix_lo: u64,
    prefix_len: u32,
    ip_hi: u64,
    ip_lo: u64,
) -> bool {
    if prefix_len == 0 {
        return true;
    }
    if prefix_len <= 64 {
        let shift = 64 - prefix_len;
        return (prefix_hi >> shift) == (ip_hi >> shift);
    }
    if prefix_hi != ip_hi {
        return false;
    }
    let shift = 128 - prefix_len;
    (prefix_lo >> shift) == (ip_lo >> shift)
}

fn parsed_ip_to_parts(parsed: ParsedIp) -> (u8, Option<u32>, Option<u64>, Option<u64>) {
    match parsed {
        ParsedIp::V4(v4) => (4, Some(v4), None, None),
        ParsedIp::V6(hi, lo) => (6, None, Some(hi), Some(lo)),
    }
}

#[pyfunction]
fn ip_to_parts(ip: &str) -> PyResult<(u8, Option<u32>, Option<u64>, Option<u64>)> {
    let parsed = parse_ip(ip)?;
    Ok(parsed_ip_to_parts(parsed))
}

#[pyfunction]
fn v6_prefix_contains(
    prefix_hi: Option<u64>,
    prefix_lo: Option<u64>,
    prefix_len: Option<u32>,
    ip_hi: u64,
    ip_lo: u64,
) -> bool {
    match (prefix_hi, prefix_lo, prefix_len) {
        (Some(hi), Some(lo), Some(len)) => prefix_contains_v6(hi, lo, len, ip_hi, ip_lo),
        _ => false,
    }
}

struct BatchColumns {
    timestamps: Vec<i64>,
    elem_types: Vec<u32>,
    peer_ip_vers: Vec<u32>,
    peer_ip_v4s: Vec<Option<u32>>,
    peer_ip_v6_his: Vec<Option<u64>>,
    peer_ip_v6_los: Vec<Option<u64>>,
    peer_asns: Vec<u32>,
    prefix_vers: Vec<u32>,
    prefix_v4s: Vec<Option<u32>>,
    prefix_v6_his: Vec<Option<u64>>,
    prefix_v6_los: Vec<Option<u64>>,
    prefix_lens: Vec<u32>,
    prefix_end_v4s: Vec<Option<u32>>,
    next_hop_vers: Vec<Option<u32>>,
    next_hop_v4s: Vec<Option<u32>>,
    next_hop_v6_his: Vec<Option<u64>>,
    next_hop_v6_los: Vec<Option<u64>>,
    as_paths: Vec<Vec<u32>>,
    as_path_lens: Vec<u32>,
    has_as_sets: Vec<bool>,
    origin_asns: Vec<Option<u32>>,
    local_prefs: Vec<Option<u32>>,
    meds: Vec<Option<u32>>,
}

impl BatchColumns {
    fn with_capacity(batch_size: usize) -> Self {
        Self {
            timestamps: Vec::with_capacity(batch_size),
            elem_types: Vec::with_capacity(batch_size),
            peer_ip_vers: Vec::with_capacity(batch_size),
            peer_ip_v4s: Vec::with_capacity(batch_size),
            peer_ip_v6_his: Vec::with_capacity(batch_size),
            peer_ip_v6_los: Vec::with_capacity(batch_size),
            peer_asns: Vec::with_capacity(batch_size),
            prefix_vers: Vec::with_capacity(batch_size),
            prefix_v4s: Vec::with_capacity(batch_size),
            prefix_v6_his: Vec::with_capacity(batch_size),
            prefix_v6_los: Vec::with_capacity(batch_size),
            prefix_lens: Vec::with_capacity(batch_size),
            prefix_end_v4s: Vec::with_capacity(batch_size),
            next_hop_vers: Vec::with_capacity(batch_size),
            next_hop_v4s: Vec::with_capacity(batch_size),
            next_hop_v6_his: Vec::with_capacity(batch_size),
            next_hop_v6_los: Vec::with_capacity(batch_size),
            as_paths: Vec::with_capacity(batch_size),
            as_path_lens: Vec::with_capacity(batch_size),
            has_as_sets: Vec::with_capacity(batch_size),
            origin_asns: Vec::with_capacity(batch_size),
            local_prefs: Vec::with_capacity(batch_size),
            meds: Vec::with_capacity(batch_size),
        }
    }

    fn len(&self) -> usize {
        self.timestamps.len()
    }

    fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }
}

fn take_owned_with_spare<T>(full: &mut Vec<T>, spare: &mut Vec<T>) -> Vec<T> {
    std::mem::replace(full, std::mem::take(spare))
}

fn drain_batch(active: &mut BatchColumns, spare: &mut BatchColumns) -> BatchColumns {
    std::mem::swap(active, spare);

    let drained = BatchColumns {
        timestamps: take_owned_with_spare(&mut spare.timestamps, &mut active.timestamps),
        elem_types: take_owned_with_spare(&mut spare.elem_types, &mut active.elem_types),
        peer_ip_vers: take_owned_with_spare(&mut spare.peer_ip_vers, &mut active.peer_ip_vers),
        peer_ip_v4s: take_owned_with_spare(&mut spare.peer_ip_v4s, &mut active.peer_ip_v4s),
        peer_ip_v6_his: take_owned_with_spare(
            &mut spare.peer_ip_v6_his,
            &mut active.peer_ip_v6_his,
        ),
        peer_ip_v6_los: take_owned_with_spare(
            &mut spare.peer_ip_v6_los,
            &mut active.peer_ip_v6_los,
        ),
        peer_asns: take_owned_with_spare(&mut spare.peer_asns, &mut active.peer_asns),
        prefix_vers: take_owned_with_spare(&mut spare.prefix_vers, &mut active.prefix_vers),
        prefix_v4s: take_owned_with_spare(&mut spare.prefix_v4s, &mut active.prefix_v4s),
        prefix_v6_his: take_owned_with_spare(&mut spare.prefix_v6_his, &mut active.prefix_v6_his),
        prefix_v6_los: take_owned_with_spare(&mut spare.prefix_v6_los, &mut active.prefix_v6_los),
        prefix_lens: take_owned_with_spare(&mut spare.prefix_lens, &mut active.prefix_lens),
        prefix_end_v4s: take_owned_with_spare(
            &mut spare.prefix_end_v4s,
            &mut active.prefix_end_v4s,
        ),
        next_hop_vers: take_owned_with_spare(&mut spare.next_hop_vers, &mut active.next_hop_vers),
        next_hop_v4s: take_owned_with_spare(&mut spare.next_hop_v4s, &mut active.next_hop_v4s),
        next_hop_v6_his: take_owned_with_spare(
            &mut spare.next_hop_v6_his,
            &mut active.next_hop_v6_his,
        ),
        next_hop_v6_los: take_owned_with_spare(
            &mut spare.next_hop_v6_los,
            &mut active.next_hop_v6_los,
        ),
        as_paths: take_owned_with_spare(&mut spare.as_paths, &mut active.as_paths),
        as_path_lens: take_owned_with_spare(&mut spare.as_path_lens, &mut active.as_path_lens),
        has_as_sets: take_owned_with_spare(&mut spare.has_as_sets, &mut active.has_as_sets),
        origin_asns: take_owned_with_spare(&mut spare.origin_asns, &mut active.origin_asns),
        local_prefs: take_owned_with_spare(&mut spare.local_prefs, &mut active.local_prefs),
        meds: take_owned_with_spare(&mut spare.meds, &mut active.meds),
    };

    std::mem::swap(active, spare);
    drained
}

fn build_batch_df(batch: BatchColumns) -> PolarsResult<DataFrame> {
    let BatchColumns {
        timestamps,
        elem_types,
        peer_ip_vers,
        peer_ip_v4s,
        peer_ip_v6_his,
        peer_ip_v6_los,
        peer_asns,
        prefix_vers,
        prefix_v4s,
        prefix_v6_his,
        prefix_v6_los,
        prefix_lens,
        prefix_end_v4s,
        next_hop_vers,
        next_hop_v4s,
        next_hop_v6_his,
        next_hop_v6_los,
        as_paths,
        as_path_lens,
        has_as_sets,
        origin_asns,
        local_prefs,
        meds,
    } = batch;

    let mut as_path_series = Vec::with_capacity(as_paths.len());
    for path in as_paths {
        let series = if path.is_empty() {
            Series::new_empty("as_path_item".into(), &DataType::UInt32)
        } else {
            UInt32Chunked::from_vec("as_path_item".into(), path).into_series()
        };
        as_path_series.push(series);
    }
    let as_path_column = Column::new("as_path".into(), as_path_series);

    DataFrame::new(vec![
        Int64Chunked::from_vec("timestamp".into(), timestamps)
            .into_series()
            .into_column(),
        UInt32Chunked::from_vec("elem_type".into(), elem_types)
            .into_series()
            .into_column(),
        UInt32Chunked::from_vec("peer_ip_ver".into(), peer_ip_vers)
            .into_series()
            .into_column(),
        Column::new("peer_ip_v4".into(), peer_ip_v4s),
        Column::new("peer_ip_v6_hi".into(), peer_ip_v6_his),
        Column::new("peer_ip_v6_lo".into(), peer_ip_v6_los),
        UInt32Chunked::from_vec("peer_asn".into(), peer_asns)
            .into_series()
            .into_column(),
        UInt32Chunked::from_vec("prefix_ver".into(), prefix_vers)
            .into_series()
            .into_column(),
        Column::new("prefix_v4".into(), prefix_v4s),
        Column::new("prefix_v6_hi".into(), prefix_v6_his),
        Column::new("prefix_v6_lo".into(), prefix_v6_los),
        UInt32Chunked::from_vec("prefix_len".into(), prefix_lens)
            .into_series()
            .into_column(),
        Column::new("prefix_end_v4".into(), prefix_end_v4s),
        Column::new("next_hop_ver".into(), next_hop_vers),
        Column::new("next_hop_v4".into(), next_hop_v4s),
        Column::new("next_hop_v6_hi".into(), next_hop_v6_his),
        Column::new("next_hop_v6_lo".into(), next_hop_v6_los),
        as_path_column,
        UInt32Chunked::from_vec("as_path_len".into(), as_path_lens)
            .into_series()
            .into_column(),
        Column::new("has_as_set".into(), has_as_sets),
        Column::new("origin_asn".into(), origin_asns),
        Column::new("local_pref".into(), local_prefs),
        Column::new("med".into(), meds),
    ])
}

fn flush_batch(
    writer: &mut Option<BatchedWriter<File>>,
    output: &str,
    active_batch: &mut BatchColumns,
    spare_batch: &mut BatchColumns,
) -> PyResult<()> {
    if active_batch.is_empty() {
        return Ok(());
    }

    let write_batch = drain_batch(active_batch, spare_batch);
    let df = build_batch_df(write_batch)
        .map_err(|err| PyRuntimeError::new_err(format!("dataframe build failed: {err}")))?;

    if writer.is_none() {
        let file = File::create(output)
            .map_err(|err| PyRuntimeError::new_err(format!("{output}: {err}")))?;
        let zstd_level =
            ZstdLevel::try_new(22).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        let mut batch_writer = ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(Some(zstd_level)))
            .batched(df.schema())
            .map_err(|err| PyRuntimeError::new_err(format!("parquet init failed: {err}")))?;
        batch_writer
            .write_batch(&df)
            .map_err(|err| PyRuntimeError::new_err(format!("parquet write failed: {err}")))?;
        *writer = Some(batch_writer);
    } else if let Some(batch_writer) = writer.as_mut() {
        batch_writer
            .write_batch(&df)
            .map_err(|err| PyRuntimeError::new_err(format!("parquet write failed: {err}")))?;
    }

    Ok(())
}

#[pyfunction]
fn hello_from_bin() -> String {
    "Hello from bgpframe!".to_string()
}

#[pyfunction]
#[pyo3(signature = (input, output, limit=None, batch_size=None))]
fn mrt_to_parquet(
    input: &str,
    output: &str,
    limit: Option<usize>,
    batch_size: Option<usize>,
) -> PyResult<usize> {
    let parser = BgpkitParser::new(input)
        .map_err(|err| PyRuntimeError::new_err(format!("parser init failed: {err}")))?;
    let batch_size = batch_size.unwrap_or(100_000);

    let mut active_batch = BatchColumns::with_capacity(batch_size);
    let mut spare_batch = BatchColumns::with_capacity(batch_size);

    let mut count = 0usize;
    let mut writer: Option<BatchedWriter<File>> = None;
    for elem in parser {
        if limit.is_some_and(|max| count >= max) {
            break;
        }

        active_batch.timestamps.push(elem.timestamp.trunc() as i64);
        active_batch.elem_types.push(match elem.elem_type {
            ElemType::ANNOUNCE => 1u32,
            ElemType::WITHDRAW => 0u32,
        });

        let (peer_ver, peer_v4, peer_v6_hi, peer_v6_lo) = split_ip(elem.peer_ip);
        active_batch.peer_ip_vers.push(peer_ver as u32);
        active_batch.peer_ip_v4s.push(peer_v4);
        active_batch.peer_ip_v6_his.push(peer_v6_hi);
        active_batch.peer_ip_v6_los.push(peer_v6_lo);
        active_batch.peer_asns.push(elem.peer_asn.to_u32());

        let prefix_addr = elem.prefix.prefix.addr();
        let prefix_len = elem.prefix.prefix.prefix_len();
        let (prefix_ver, prefix_v4, prefix_v6_hi, prefix_v6_lo) = split_ip(prefix_addr);
        let prefix_end = match (prefix_ver, prefix_v4) {
            (4, Some(v4)) => Some(prefix_end_v4(v4, prefix_len)),
            _ => None,
        };
        active_batch.prefix_vers.push(prefix_ver as u32);
        active_batch.prefix_v4s.push(prefix_v4);
        active_batch.prefix_v6_his.push(prefix_v6_hi);
        active_batch.prefix_v6_los.push(prefix_v6_lo);
        active_batch.prefix_lens.push(prefix_len as u32);
        active_batch.prefix_end_v4s.push(prefix_end);

        match elem.next_hop {
            Some(next_hop) => {
                let (nh_ver, nh_v4, nh_v6_hi, nh_v6_lo) = split_ip(next_hop);
                active_batch.next_hop_vers.push(Some(nh_ver as u32));
                active_batch.next_hop_v4s.push(nh_v4);
                active_batch.next_hop_v6_his.push(nh_v6_hi);
                active_batch.next_hop_v6_los.push(nh_v6_lo);
            }
            None => {
                active_batch.next_hop_vers.push(None);
                active_batch.next_hop_v4s.push(None);
                active_batch.next_hop_v6_his.push(None);
                active_batch.next_hop_v6_los.push(None);
            }
        }

        let mut has_as_set = false;
        let (as_path, as_path_len) = match elem.as_path.as_ref() {
            Some(path) => {
                let mut flat = Vec::with_capacity(path.route_len());
                for segment in path.iter_segments() {
                    match segment {
                        AsPathSegment::AsSequence(v) | AsPathSegment::ConfedSequence(v) => {
                            flat.extend(v.iter().map(|asn| asn.to_u32()));
                        }
                        AsPathSegment::AsSet(v) | AsPathSegment::ConfedSet(v) => {
                            has_as_set = true;
                            flat.extend(v.iter().map(|asn| asn.to_u32()));
                        }
                    }
                }
                let len = path.route_len() as u32;
                (flat, len)
            }
            None => (Vec::new(), 0),
        };
        active_batch.as_paths.push(as_path);
        active_batch.as_path_lens.push(as_path_len);

        let origin_asn = match elem.origin_asns.as_ref() {
            Some(asns) if asns.len() == 1 => Some(asns[0].to_u32()),
            Some(asns) if !asns.is_empty() => {
                has_as_set = true;
                None
            }
            _ => None,
        };
        active_batch.has_as_sets.push(has_as_set);
        active_batch.origin_asns.push(origin_asn);

        active_batch.local_prefs.push(elem.local_pref);
        active_batch.meds.push(elem.med);

        count += 1;
        if active_batch.len() >= batch_size {
            flush_batch(&mut writer, output, &mut active_batch, &mut spare_batch)?;
        }
    }

    flush_batch(&mut writer, output, &mut active_batch, &mut spare_batch)?;

    if let Some(batch_writer) = writer.as_mut() {
        batch_writer
            .finish()
            .map_err(|err| PyRuntimeError::new_err(format!("parquet finalize failed: {err}")))?;
    }

    Ok(count)
}

#[pyfunction]
#[pyo3(signature = (input, ip, output=None, limit=None))]
fn parquet_contains_ip(
    input: &str,
    ip: &str,
    output: Option<&str>,
    limit: Option<usize>,
) -> PyResult<usize> {
    let target = parse_ip(ip)?;

    let file =
        File::open(input).map_err(|err| PyRuntimeError::new_err(format!("{input}: {err}")))?;
    let df = ParquetReader::new(file)
        .finish()
        .map_err(|err| PyRuntimeError::new_err(format!("parquet read failed: {err}")))?;

    let prefix_vers = df
        .column("prefix_ver")
        .map_err(|err| PyRuntimeError::new_err(format!("missing column prefix_ver: {err}")))?
        .u32()
        .map_err(|err| PyRuntimeError::new_err(format!("invalid column prefix_ver: {err}")))?;
    let prefix_v4s = df
        .column("prefix_v4")
        .map_err(|err| PyRuntimeError::new_err(format!("missing column prefix_v4: {err}")))?
        .u32()
        .map_err(|err| PyRuntimeError::new_err(format!("invalid column prefix_v4: {err}")))?;
    let prefix_end_v4s = df
        .column("prefix_end_v4")
        .map_err(|err| PyRuntimeError::new_err(format!("missing column prefix_end_v4: {err}")))?
        .u32()
        .map_err(|err| PyRuntimeError::new_err(format!("invalid column prefix_end_v4: {err}")))?;
    let prefix_v6_his = df
        .column("prefix_v6_hi")
        .map_err(|err| PyRuntimeError::new_err(format!("missing column prefix_v6_hi: {err}")))?
        .u64()
        .map_err(|err| PyRuntimeError::new_err(format!("invalid column prefix_v6_hi: {err}")))?;
    let prefix_v6_los = df
        .column("prefix_v6_lo")
        .map_err(|err| PyRuntimeError::new_err(format!("missing column prefix_v6_lo: {err}")))?
        .u64()
        .map_err(|err| PyRuntimeError::new_err(format!("invalid column prefix_v6_lo: {err}")))?;
    let prefix_lens = df
        .column("prefix_len")
        .map_err(|err| PyRuntimeError::new_err(format!("missing column prefix_len: {err}")))?
        .u32()
        .map_err(|err| PyRuntimeError::new_err(format!("invalid column prefix_len: {err}")))?;

    let mut mask = Vec::with_capacity(df.height());
    for idx in 0..df.height() {
        let matches = match target {
            ParsedIp::V4(ip_v4) => {
                if prefix_vers.get(idx) != Some(4) {
                    false
                } else {
                    match (prefix_v4s.get(idx), prefix_end_v4s.get(idx)) {
                        (Some(start), Some(end)) => start <= ip_v4 && ip_v4 <= end,
                        _ => false,
                    }
                }
            }
            ParsedIp::V6(ip_hi, ip_lo) => {
                if prefix_vers.get(idx) != Some(6) {
                    false
                } else {
                    match (
                        prefix_v6_his.get(idx),
                        prefix_v6_los.get(idx),
                        prefix_lens.get(idx),
                    ) {
                        (Some(prefix_hi), Some(prefix_lo), Some(prefix_len)) => {
                            prefix_contains_v6(prefix_hi, prefix_lo, prefix_len, ip_hi, ip_lo)
                        }
                        _ => false,
                    }
                }
            }
        };
        mask.push(matches);
    }

    let mask_ca = BooleanChunked::from_slice("contains".into(), &mask);
    let mut filtered = df
        .filter(&mask_ca)
        .map_err(|err| PyRuntimeError::new_err(format!("parquet filter failed: {err}")))?;
    if let Some(max) = limit {
        filtered = filtered.head(Some(max));
    }

    if let Some(output_path) = output {
        let out_file = File::create(output_path)
            .map_err(|err| PyRuntimeError::new_err(format!("{output_path}: {err}")))?;
        let zstd_level =
            ZstdLevel::try_new(22).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        let writer = ParquetWriter::new(out_file)
            .with_compression(ParquetCompression::Zstd(Some(zstd_level)));
        writer
            .finish(&mut filtered)
            .map_err(|err| PyRuntimeError::new_err(format!("parquet write failed: {err}")))?;
    }

    Ok(filtered.height())
}

/// A Python module implemented in Rust. The name of this module must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_from_bin, m)?)?;
    m.add_function(wrap_pyfunction!(mrt_to_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(parquet_contains_ip, m)?)?;
    m.add_function(wrap_pyfunction!(ip_to_parts, m)?)?;
    m.add_function(wrap_pyfunction!(v6_prefix_contains, m)?)?;
    Ok(())
}
