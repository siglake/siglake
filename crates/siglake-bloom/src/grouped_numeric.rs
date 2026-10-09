//! Optional exact grouped numeric statistics stored in a Parquet footer.
//!
//! This is an accelerator: unknown, malformed, or incomplete payloads are
//! ignored and the SQL path scans the data instead.

use std::collections::BTreeMap;

use base64::Engine as _;

pub const GROUPED_NUMERIC_KV_KEY: &str = "siglake.grouped_numeric.v1";
const MAGIC: &[u8; 4] = b"LGNS";
const VERSION: u8 = 1;
const MAX_BLOB_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupedNumericKind {
    Int64,
    Float64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GroupedNumericSum {
    Int(i128),
    Float(f64),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GroupedNumericValue {
    pub rows: u64,
    pub non_null: u64,
    pub sum: GroupedNumericSum,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupedNumericSummary {
    pub group_column: String,
    pub value_column: String,
    pub kind: GroupedNumericKind,
    pub groups: BTreeMap<Option<String>, GroupedNumericValue>,
}

impl GroupedNumericSummary {
    pub fn new(group_column: String, value_column: String, kind: GroupedNumericKind) -> Self {
        Self {
            group_column,
            value_column,
            kind,
            groups: BTreeMap::new(),
        }
    }

    pub fn encode(&self) -> Option<String> {
        if self.groups.is_empty() {
            return None;
        }
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        bytes.push(match self.kind {
            GroupedNumericKind::Int64 => 0,
            GroupedNumericKind::Float64 => 1,
        });
        put_bytes(&mut bytes, self.group_column.as_bytes());
        put_bytes(&mut bytes, self.value_column.as_bytes());
        put_u64(&mut bytes, self.groups.len() as u64);
        for (group, value) in &self.groups {
            match group {
                Some(group) => {
                    bytes.push(1);
                    put_bytes(&mut bytes, group.as_bytes());
                }
                None => bytes.push(0),
            }
            put_u64(&mut bytes, value.rows);
            put_u64(&mut bytes, value.non_null);
            match (self.kind, value.sum) {
                (GroupedNumericKind::Int64, GroupedNumericSum::Int(sum)) => {
                    bytes.extend_from_slice(&sum.to_le_bytes());
                }
                (GroupedNumericKind::Float64, GroupedNumericSum::Float(sum)) => {
                    bytes.extend_from_slice(&sum.to_bits().to_le_bytes());
                }
                _ => return None,
            }
        }
        Some(base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub fn decode(encoded: &str) -> Option<Self> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .ok()?;
        if bytes.len() > MAX_BLOB_BYTES || bytes.len() < MAGIC.len() + 2 {
            return None;
        }
        let mut cursor = Cursor::new(&bytes);
        if cursor.take(MAGIC.len())? != MAGIC || cursor.byte()? != VERSION {
            return None;
        }
        let kind = match cursor.byte()? {
            0 => GroupedNumericKind::Int64,
            1 => GroupedNumericKind::Float64,
            _ => return None,
        };
        let group_column = String::from_utf8(cursor.bytes()?.to_vec()).ok()?;
        let value_column = String::from_utf8(cursor.bytes()?.to_vec()).ok()?;
        let count = usize::try_from(cursor.u64()?).ok()?;
        if count > 65_536 {
            return None;
        }
        let mut groups = BTreeMap::new();
        for _ in 0..count {
            let group = match cursor.byte()? {
                0 => None,
                1 => Some(String::from_utf8(cursor.bytes()?.to_vec()).ok()?),
                _ => return None,
            };
            let rows = cursor.u64()?;
            let non_null = cursor.u64()?;
            if non_null > rows {
                return None;
            }
            let sum = match kind {
                GroupedNumericKind::Int64 => {
                    let raw: [u8; 16] = cursor.take(16)?.try_into().ok()?;
                    GroupedNumericSum::Int(i128::from_le_bytes(raw))
                }
                GroupedNumericKind::Float64 => {
                    let raw: [u8; 8] = cursor.take(8)?.try_into().ok()?;
                    GroupedNumericSum::Float(f64::from_bits(u64::from_le_bytes(raw)))
                }
            };
            if groups
                .insert(
                    group,
                    GroupedNumericValue {
                        rows,
                        non_null,
                        sum,
                    },
                )
                .is_some()
            {
                return None;
            }
        }
        (cursor.pos == bytes.len()).then_some(Self {
            group_column,
            value_column,
            kind,
            groups,
        })
    }
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    put_u64(out, value.len() as u64);
    out.extend_from_slice(value);
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        let value = self.bytes.get(self.pos..end)?;
        self.pos = end;
        Some(value)
    }

    fn byte(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u64(&mut self) -> Option<u64> {
        let raw: [u8; 8] = self.take(8)?.try_into().ok()?;
        Some(u64::from_le_bytes(raw))
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.u64()?).ok()?;
        self.take(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_integer_float_nulls_and_unknown_versions() {
        for (kind, sum) in [
            (GroupedNumericKind::Int64, GroupedNumericSum::Int(-17)),
            (GroupedNumericKind::Float64, GroupedNumericSum::Float(-3.25)),
        ] {
            let mut summary = GroupedNumericSummary::new("status".into(), "size".into(), kind);
            summary.groups.insert(
                None,
                GroupedNumericValue {
                    rows: 3,
                    non_null: 2,
                    sum,
                },
            );
            let encoded = summary.encode().unwrap();
            assert_eq!(GroupedNumericSummary::decode(&encoded), Some(summary));

            let mut raw = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap();
            raw[MAGIC.len()] = 99;
            let unknown = base64::engine::general_purpose::STANDARD.encode(raw);
            assert!(GroupedNumericSummary::decode(&unknown).is_none());
        }
    }
}
