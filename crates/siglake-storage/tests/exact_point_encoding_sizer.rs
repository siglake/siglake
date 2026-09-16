//! Offline dictionary + delta sizing for the exact-point falsifier.
//!
//! The ordinary test uses a synthetic export and pins the decoder round trip.
//! The ignored entry point consumes a retained export without contacting a
//! cluster:
//!
//! ```text
//! EXACT_POINT_NDJSON_PATH=exact-points.ndjson \
//! EXACT_POINT_CARDINALITY_PATH=cardinality.json \
//! EXACT_POINT_ENCODING_PATH=encoding.json \
//! EXACT_POINT_REPOSITORY_COMMIT=$(git rev-parse HEAD) \
//! cargo test -p siglake-storage --test exact_point_encoding_sizer \
//!   size_exact_point_export -- --ignored --nocapture
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

const MAGIC: &[u8; 4] = b"LLEP";
const VERSION: u64 = 1;
const JSON_UPPER_BOUND: usize = 757_764;
type PointCounts = BTreeMap<i64, Vec<u64>>;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ExportRow {
    timestamp_ns: i64,
    level: String,
    n: u64,
}

#[derive(Debug, Deserialize)]
struct Cardinality {
    row_count: u64,
    distinct_ts_ns: u64,
    distinct_ts_ns_level: u64,
}

#[derive(Debug, Serialize)]
struct JsonEquivalent<'a> {
    column: &'static str,
    values: &'a [String],
    width_ns: i64,
    counts: &'a PointCounts,
}

#[derive(Debug, Serialize)]
struct Revisions {
    repository_commit: String,
}

#[derive(Debug, Serialize)]
struct EncodingReport {
    schema_version: u64,
    evidence_kind: String,
    revisions: Revisions,
    points: usize,
    exported_pair_rows: usize,
    row_count: u64,
    dictionary_values: usize,
    encoded_bytes: usize,
    bytes_per_point: f64,
    serde_json_bytes: usize,
    decode_round_trip: bool,
    verdict: &'static str,
}

#[derive(Debug, PartialEq, Eq)]
struct Decoded {
    dictionary: Vec<String>,
    counts: PointCounts,
    exported_pair_rows: usize,
}

fn put_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn get_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let mut value = 0_u64;
    for shift in (0..=63).step_by(7) {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| "truncated varint".to_owned())?;
        *cursor += 1;
        if shift == 63 && byte > 1 {
            return Err("varint overflow".to_owned());
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("varint overflow".to_owned())
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

fn parse_export(bytes: &[u8]) -> Result<Vec<ExportRow>, String> {
    let text =
        std::str::from_utf8(bytes).map_err(|error| format!("export is not UTF-8: {error}"))?;
    let mut rows: Vec<ExportRow> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line)
            .map_err(|error| format!("line {} is not JSON: {error}", index + 1))?;
        if value.get("_meta").is_some() {
            return Err(format!(
                "line {} is an _meta truncation/error record, not a point",
                index + 1
            ));
        }
        let row: ExportRow = serde_json::from_value(value)
            .map_err(|error| format!("line {} is not a point: {error}", index + 1))?;
        if row.level.is_empty() {
            return Err(format!("line {} has an empty level", index + 1));
        }
        if row.n == 0 {
            return Err(format!("line {} has a zero count", index + 1));
        }
        if let Some(previous) = rows.last() {
            if (row.timestamp_ns, row.level.as_str())
                <= (previous.timestamp_ns, previous.level.as_str())
            {
                return Err(format!(
                    "line {} is not strictly sorted by timestamp_ns, level",
                    index + 1
                ));
            }
        }
        rows.push(row);
    }
    if rows.is_empty() {
        return Err("export has no point rows".to_owned());
    }
    Ok(rows)
}

fn coalesce(rows: &[ExportRow]) -> Result<(Vec<String>, PointCounts), String> {
    let dictionary: Vec<String> = rows
        .iter()
        .map(|row| row.level.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let indexes: BTreeMap<&str, usize> = dictionary
        .iter()
        .enumerate()
        .map(|(index, value)| (value.as_str(), index))
        .collect();
    let mut counts = BTreeMap::new();
    for row in rows {
        let vector = counts
            .entry(row.timestamp_ns)
            .or_insert_with(|| vec![0_u64; dictionary.len()]);
        let index = indexes[row.level.as_str()];
        vector[index] = vector[index]
            .checked_add(row.n)
            .ok_or_else(|| "count vector overflow".to_owned())?;
    }
    Ok((dictionary, counts))
}

fn encode(
    dictionary: &[String],
    counts: &PointCounts,
    exported_pair_rows: usize,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    put_varint(VERSION, &mut out);
    put_varint(dictionary.len() as u64, &mut out);
    for value in dictionary {
        put_varint(value.len() as u64, &mut out);
        out.extend_from_slice(value.as_bytes());
    }
    put_varint(counts.len() as u64, &mut out);
    put_varint(exported_pair_rows as u64, &mut out);
    let mut previous = 0_i64;
    for (&timestamp_ns, vector) in counts {
        if vector.len() != dictionary.len() {
            return Err("count vector length does not match dictionary".to_owned());
        }
        let delta = timestamp_ns
            .checked_sub(previous)
            .ok_or_else(|| "timestamp delta overflow".to_owned())?;
        put_varint(zigzag(delta), &mut out);
        for &count in vector {
            put_varint(count, &mut out);
        }
        previous = timestamp_ns;
    }
    Ok(out)
}

fn decode(bytes: &[u8]) -> Result<Decoded, String> {
    if bytes.get(..MAGIC.len()) != Some(MAGIC) {
        return Err("bad exact-point encoding magic".to_owned());
    }
    let mut cursor = MAGIC.len();
    if get_varint(bytes, &mut cursor)? != VERSION {
        return Err("unsupported exact-point encoding version".to_owned());
    }
    let dictionary_len = usize::try_from(get_varint(bytes, &mut cursor)?)
        .map_err(|_| "dictionary length does not fit usize".to_owned())?;
    let mut dictionary = Vec::with_capacity(dictionary_len);
    for _ in 0..dictionary_len {
        let len = usize::try_from(get_varint(bytes, &mut cursor)?)
            .map_err(|_| "dictionary value length does not fit usize".to_owned())?;
        let end = cursor
            .checked_add(len)
            .ok_or_else(|| "dictionary value length overflow".to_owned())?;
        let value = std::str::from_utf8(
            bytes
                .get(cursor..end)
                .ok_or_else(|| "truncated dictionary value".to_owned())?,
        )
        .map_err(|error| format!("dictionary value is not UTF-8: {error}"))?;
        dictionary.push(value.to_owned());
        cursor = end;
    }
    let point_len = usize::try_from(get_varint(bytes, &mut cursor)?)
        .map_err(|_| "point count does not fit usize".to_owned())?;
    let exported_pair_rows = usize::try_from(get_varint(bytes, &mut cursor)?)
        .map_err(|_| "pair-row count does not fit usize".to_owned())?;
    let mut counts = BTreeMap::new();
    let mut previous = 0_i64;
    for _ in 0..point_len {
        let delta = unzigzag(get_varint(bytes, &mut cursor)?);
        let timestamp_ns = previous
            .checked_add(delta)
            .ok_or_else(|| "decoded timestamp overflow".to_owned())?;
        let mut vector = Vec::with_capacity(dictionary_len);
        for _ in 0..dictionary_len {
            vector.push(get_varint(bytes, &mut cursor)?);
        }
        if counts.insert(timestamp_ns, vector).is_some() {
            return Err("decoded duplicate timestamp".to_owned());
        }
        previous = timestamp_ns;
    }
    if cursor != bytes.len() {
        return Err("trailing bytes in exact-point encoding".to_owned());
    }
    Ok(Decoded {
        dictionary,
        counts,
        exported_pair_rows,
    })
}

fn measure(
    export: &[u8],
    cardinality: &Cardinality,
    repository_commit: String,
    evidence_kind: String,
) -> Result<EncodingReport, String> {
    let rows = parse_export(export)?;
    let (dictionary, counts) = coalesce(&rows)?;
    let row_count = rows.iter().try_fold(0_u64, |total, row| {
        total
            .checked_add(row.n)
            .ok_or_else(|| "export row-count sum overflow".to_owned())
    })?;
    if counts.len() as u64 != cardinality.distinct_ts_ns {
        return Err(format!(
            "distinct timestamp points {} != cardinality.json {}",
            counts.len(),
            cardinality.distinct_ts_ns
        ));
    }
    if rows.len() as u64 != cardinality.distinct_ts_ns_level {
        return Err(format!(
            "exported pair rows {} != cardinality.json {}",
            rows.len(),
            cardinality.distinct_ts_ns_level
        ));
    }
    if row_count != cardinality.row_count {
        return Err(format!(
            "sum(n) {row_count} != cardinality.json {}",
            cardinality.row_count
        ));
    }

    let encoded = encode(&dictionary, &counts, rows.len())?;
    let decoded = decode(&encoded)?;
    let decode_round_trip = decoded
        == (Decoded {
            dictionary: dictionary.clone(),
            counts: counts.clone(),
            exported_pair_rows: rows.len(),
        });
    if !decode_round_trip {
        return Err("encoded bytes did not round-trip".to_owned());
    }
    let serde_json_bytes = serde_json::to_vec(&JsonEquivalent {
        column: "level",
        values: &dictionary,
        width_ns: 0,
        counts: &counts,
    })
    .map_err(|error| format!("serialize JSON equivalent: {error}"))?
    .len();
    let points = counts.len();
    Ok(EncodingReport {
        schema_version: 1,
        evidence_kind,
        revisions: Revisions { repository_commit },
        points,
        exported_pair_rows: rows.len(),
        row_count,
        dictionary_values: dictionary.len(),
        encoded_bytes: encoded.len(),
        bytes_per_point: encoded.len() as f64 / points as f64,
        serde_json_bytes,
        decode_round_trip,
        verdict: if encoded.len() <= JSON_UPPER_BOUND {
            "passed"
        } else {
            "falsified"
        },
    })
}

fn read_cardinality(path: &Path) -> Result<Cardinality, String> {
    let bytes = fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("decode {}: {error}", path.display()))
}

#[test]
fn synthetic_encoding_coalesces_pairs_and_round_trips() {
    let export = br#"{"timestamp_ns":-2,"level":"info","n":3}
{"timestamp_ns":-2,"level":"warn","n":1}
{"timestamp_ns":7,"level":"info","n":2}
{"timestamp_ns":11,"level":"warn","n":4}
"#;
    let report = measure(
        export,
        &Cardinality {
            row_count: 10,
            distinct_ts_ns: 3,
            distinct_ts_ns_level: 4,
        },
        "synthetic-repository-commit".to_owned(),
        "synthetic".to_owned(),
    )
    .expect("synthetic export should size");
    assert_eq!(report.points, 3);
    assert_eq!(report.exported_pair_rows, 4);
    assert_eq!(report.row_count, 10);
    assert_eq!(report.dictionary_values, 2);
    assert!(report.decode_round_trip);
    assert_eq!(report.verdict, "passed");
}

#[test]
fn rejects_truncation_metadata_and_cardinality_mismatch() {
    let cardinality = Cardinality {
        row_count: 1,
        distinct_ts_ns: 1,
        distinct_ts_ns_level: 1,
    };
    let truncated = br#"{"timestamp_ns":1,"level":"info","n":1}
{"_meta":{"truncated":true}}
"#;
    assert!(measure(
        truncated,
        &cardinality,
        "synthetic-repository-commit".to_owned(),
        "synthetic".to_owned(),
    )
    .unwrap_err()
    .contains("_meta"));

    let mismatched = br#"{"timestamp_ns":1,"level":"info","n":2}
"#;
    assert!(measure(
        mismatched,
        &cardinality,
        "synthetic-repository-commit".to_owned(),
        "synthetic".to_owned(),
    )
    .unwrap_err()
    .contains("sum(n)"));
}

#[test]
#[ignore = "offline measurement; requires EXACT_POINT_* paths"]
fn size_exact_point_export() {
    let required = |name: &str| {
        std::env::var(name).unwrap_or_else(|_| panic!("{name} must name the retained artifact"))
    };
    let export_path = required("EXACT_POINT_NDJSON_PATH");
    let cardinality_path = required("EXACT_POINT_CARDINALITY_PATH");
    let output_path = required("EXACT_POINT_ENCODING_PATH");
    let repository_commit = required("EXACT_POINT_REPOSITORY_COMMIT");
    let evidence_kind =
        std::env::var("EXACT_POINT_EVIDENCE_KIND").unwrap_or_else(|_| "live".to_owned());
    let export =
        fs::read(&export_path).unwrap_or_else(|error| panic!("read {export_path}: {error}"));
    let cardinality =
        read_cardinality(Path::new(&cardinality_path)).unwrap_or_else(|error| panic!("{error}"));
    let report = measure(&export, &cardinality, repository_commit, evidence_kind)
        .unwrap_or_else(|error| panic!("size exact-point export: {error}"));
    let rendered = serde_json::to_vec_pretty(&report).expect("serialize encoding report");
    fs::write(&output_path, [rendered.as_slice(), b"\n"].concat())
        .unwrap_or_else(|error| panic!("write {output_path}: {error}"));
    println!("{}", String::from_utf8(rendered).expect("report is UTF-8"));
}
