use bgpkit_parser::{
    models::{AsPathSegment, ElemType},
    BgpkitParser,
};
use polars::prelude::*;
use polars::prelude::{ParquetCompression, ZstdLevel};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use polars::io::parquet::write::BatchedWriter;
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

fn build_batch_df(
    timestamps: &[i64],
    elem_types: &[u32],
    peer_ip_vers: &[u32],
    peer_ip_v4s: &[Option<u32>],
    peer_ip_v6_his: &[Option<u64>],
    peer_ip_v6_los: &[Option<u64>],
    peer_asns: &[u32],
    prefix_vers: &[u32],
    prefix_v4s: &[Option<u32>],
    prefix_v6_his: &[Option<u64>],
    prefix_v6_los: &[Option<u64>],
    prefix_lens: &[u32],
    prefix_end_v4s: &[Option<u32>],
    next_hop_vers: &[Option<u32>],
    next_hop_v4s: &[Option<u32>],
    next_hop_v6_his: &[Option<u64>],
    next_hop_v6_los: &[Option<u64>],
    as_paths: &[Vec<u32>],
    as_path_lens: &[u32],
    has_as_sets: &[bool],
    origin_asns: &[Option<u32>],
    local_prefs: &[Option<u32>],
    meds: &[Option<u32>],
) -> PolarsResult<DataFrame> {
    let as_path_series: Vec<Series> = as_paths
        .iter()
        .map(|path| {
            if path.is_empty() {
                Series::new_empty("as_path_item".into(), &DataType::UInt32)
            } else {
                Series::new("as_path_item".into(), path)
            }
        })
        .collect();

    DataFrame::new(vec![
        Column::new("timestamp".into(), timestamps),
        Column::new("elem_type".into(), elem_types),
        Column::new("peer_ip_ver".into(), peer_ip_vers),
        Column::new("peer_ip_v4".into(), peer_ip_v4s),
        Column::new("peer_ip_v6_hi".into(), peer_ip_v6_his),
        Column::new("peer_ip_v6_lo".into(), peer_ip_v6_los),
        Column::new("peer_asn".into(), peer_asns),
        Column::new("prefix_ver".into(), prefix_vers),
        Column::new("prefix_v4".into(), prefix_v4s),
        Column::new("prefix_v6_hi".into(), prefix_v6_his),
        Column::new("prefix_v6_lo".into(), prefix_v6_los),
        Column::new("prefix_len".into(), prefix_lens),
        Column::new("prefix_end_v4".into(), prefix_end_v4s),
        Column::new("next_hop_ver".into(), next_hop_vers),
        Column::new("next_hop_v4".into(), next_hop_v4s),
        Column::new("next_hop_v6_hi".into(), next_hop_v6_his),
        Column::new("next_hop_v6_lo".into(), next_hop_v6_los),
        Column::new("as_path".into(), as_path_series),
        Column::new("as_path_len".into(), as_path_lens),
        Column::new("has_as_set".into(), has_as_sets),
        Column::new("origin_asn".into(), origin_asns),
        Column::new("local_pref".into(), local_prefs),
        Column::new("med".into(), meds),
    ])
}

fn flush_batch(
    writer: &mut Option<BatchedWriter<File>>,
    output: &str,
    timestamps: &mut Vec<i64>,
    elem_types: &mut Vec<u32>,
    peer_ip_vers: &mut Vec<u32>,
    peer_ip_v4s: &mut Vec<Option<u32>>,
    peer_ip_v6_his: &mut Vec<Option<u64>>,
    peer_ip_v6_los: &mut Vec<Option<u64>>,
    peer_asns: &mut Vec<u32>,
    prefix_vers: &mut Vec<u32>,
    prefix_v4s: &mut Vec<Option<u32>>,
    prefix_v6_his: &mut Vec<Option<u64>>,
    prefix_v6_los: &mut Vec<Option<u64>>,
    prefix_lens: &mut Vec<u32>,
    prefix_end_v4s: &mut Vec<Option<u32>>,
    next_hop_vers: &mut Vec<Option<u32>>,
    next_hop_v4s: &mut Vec<Option<u32>>,
    next_hop_v6_his: &mut Vec<Option<u64>>,
    next_hop_v6_los: &mut Vec<Option<u64>>,
    as_paths: &mut Vec<Vec<u32>>,
    as_path_lens: &mut Vec<u32>,
    has_as_sets: &mut Vec<bool>,
    origin_asns: &mut Vec<Option<u32>>,
    local_prefs: &mut Vec<Option<u32>>,
    meds: &mut Vec<Option<u32>>,
) -> PyResult<()> {
    if timestamps.is_empty() {
        return Ok(());
    }

    let df = build_batch_df(
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
    )
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

    timestamps.clear();
    elem_types.clear();
    peer_ip_vers.clear();
    peer_ip_v4s.clear();
    peer_ip_v6_his.clear();
    peer_ip_v6_los.clear();
    peer_asns.clear();
    prefix_vers.clear();
    prefix_v4s.clear();
    prefix_v6_his.clear();
    prefix_v6_los.clear();
    prefix_lens.clear();
    prefix_end_v4s.clear();
    next_hop_vers.clear();
    next_hop_v4s.clear();
    next_hop_v6_his.clear();
    next_hop_v6_los.clear();
    as_paths.clear();
    as_path_lens.clear();
    has_as_sets.clear();
    origin_asns.clear();
    local_prefs.clear();
    meds.clear();

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

    let mut timestamps = Vec::new();
    let mut elem_types = Vec::new();
    let mut peer_ip_vers = Vec::new();
    let mut peer_ip_v4s = Vec::new();
    let mut peer_ip_v6_his = Vec::new();
    let mut peer_ip_v6_los = Vec::new();
    let mut peer_asns = Vec::new();
    let mut prefix_vers = Vec::new();
    let mut prefix_v4s = Vec::new();
    let mut prefix_v6_his = Vec::new();
    let mut prefix_v6_los = Vec::new();
    let mut prefix_lens = Vec::new();
    let mut prefix_end_v4s = Vec::new();
    let mut next_hop_vers = Vec::new();
    let mut next_hop_v4s = Vec::new();
    let mut next_hop_v6_his = Vec::new();
    let mut next_hop_v6_los = Vec::new();
    let mut as_paths = Vec::new();
    let mut as_path_lens = Vec::new();
    let mut has_as_sets = Vec::new();
    let mut origin_asns = Vec::new();
    let mut local_prefs = Vec::new();
    let mut meds = Vec::new();

    let mut count = 0usize;
    let mut writer: Option<BatchedWriter<File>> = None;
    for elem in parser {
        if let Some(max) = limit {
            if count >= max {
                break;
            }
        }

        timestamps.push(elem.timestamp.trunc() as i64);
        elem_types.push(match elem.elem_type {
            ElemType::ANNOUNCE => 1u32,
            ElemType::WITHDRAW => 0u32,
        });

        let (peer_ver, peer_v4, peer_v6_hi, peer_v6_lo) = split_ip(elem.peer_ip);
        peer_ip_vers.push(peer_ver as u32);
        peer_ip_v4s.push(peer_v4);
        peer_ip_v6_his.push(peer_v6_hi);
        peer_ip_v6_los.push(peer_v6_lo);
        peer_asns.push(elem.peer_asn.to_u32());

        let prefix_addr = elem.prefix.prefix.addr();
        let prefix_len = elem.prefix.prefix.prefix_len();
        let (prefix_ver, prefix_v4, prefix_v6_hi, prefix_v6_lo) = split_ip(prefix_addr);
        let prefix_end = match (prefix_ver, prefix_v4) {
            (4, Some(v4)) => Some(prefix_end_v4(v4, prefix_len)),
            _ => None,
        };
        prefix_vers.push(prefix_ver as u32);
        prefix_v4s.push(prefix_v4);
        prefix_v6_his.push(prefix_v6_hi);
        prefix_v6_los.push(prefix_v6_lo);
        prefix_lens.push(prefix_len as u32);
        prefix_end_v4s.push(prefix_end);

        match elem.next_hop {
            Some(next_hop) => {
                let (nh_ver, nh_v4, nh_v6_hi, nh_v6_lo) = split_ip(next_hop);
                next_hop_vers.push(Some(nh_ver as u32));
                next_hop_v4s.push(nh_v4);
                next_hop_v6_his.push(nh_v6_hi);
                next_hop_v6_los.push(nh_v6_lo);
            }
            None => {
                next_hop_vers.push(None);
                next_hop_v4s.push(None);
                next_hop_v6_his.push(None);
                next_hop_v6_los.push(None);
            }
        }

        let mut has_as_set = false;
        let (as_path, as_path_len) = match elem.as_path.as_ref() {
            Some(path) => {
                let mut flat = Vec::new();
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
        as_paths.push(as_path);
        as_path_lens.push(as_path_len);

        let origin_asn = match elem.origin_asns.as_ref() {
            Some(asns) if asns.len() == 1 => Some(asns[0].to_u32()),
            Some(asns) if !asns.is_empty() => {
                has_as_set = true;
                None
            }
            _ => None,
        };
        has_as_sets.push(has_as_set);
        origin_asns.push(origin_asn);

        local_prefs.push(elem.local_pref);
        meds.push(elem.med);

        count += 1;
        if timestamps.len() >= batch_size {
            flush_batch(
                &mut writer,
                output,
                &mut timestamps,
                &mut elem_types,
                &mut peer_ip_vers,
                &mut peer_ip_v4s,
                &mut peer_ip_v6_his,
                &mut peer_ip_v6_los,
                &mut peer_asns,
                &mut prefix_vers,
                &mut prefix_v4s,
                &mut prefix_v6_his,
                &mut prefix_v6_los,
                &mut prefix_lens,
                &mut prefix_end_v4s,
                &mut next_hop_vers,
                &mut next_hop_v4s,
                &mut next_hop_v6_his,
                &mut next_hop_v6_los,
                &mut as_paths,
                &mut as_path_lens,
                &mut has_as_sets,
                &mut origin_asns,
                &mut local_prefs,
                &mut meds,
            )?;
        }
    }

    flush_batch(
        &mut writer,
        output,
        &mut timestamps,
        &mut elem_types,
        &mut peer_ip_vers,
        &mut peer_ip_v4s,
        &mut peer_ip_v6_his,
        &mut peer_ip_v6_los,
        &mut peer_asns,
        &mut prefix_vers,
        &mut prefix_v4s,
        &mut prefix_v6_his,
        &mut prefix_v6_los,
        &mut prefix_lens,
        &mut prefix_end_v4s,
        &mut next_hop_vers,
        &mut next_hop_v4s,
        &mut next_hop_v6_his,
        &mut next_hop_v6_los,
        &mut as_paths,
        &mut as_path_lens,
        &mut has_as_sets,
        &mut origin_asns,
        &mut local_prefs,
        &mut meds,
    )?;

    if let Some(batch_writer) = writer.as_mut() {
        batch_writer
            .finish()
            .map_err(|err| PyRuntimeError::new_err(format!("parquet finalize failed: {err}")))?;
    }

    Ok(count)
}

/// A Python module implemented in Rust. The name of this module must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_from_bin, m)?)?;
    m.add_function(wrap_pyfunction!(mrt_to_parquet, m)?)?;
    Ok(())
}
