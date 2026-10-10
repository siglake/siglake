//! Compact per-file **group-count footer** encoding.
//!
//! The group-count footer is the "fast-field term agg" hedge: for each
//! designated column, one file's exact row count per distinct value, stamped
//! into the Parquet footer KV at write so a `GROUP BY <col> COUNT(*)` (and the
//! dimensional-count fast paths built on it) is answered by summing footers
//! instead of scanning rows.
//!
//! The obvious encoding — one JSON blob per file — does not survive contact
//! with high-cardinality columns: those blobs reach multiple MB, the whole blob
//! rides in the Parquet footer so every footer read pulls all of it, and
//! reading it materializes *every* column into a map when the query wanted one.
//!
//! So the payload ([`GROUP_COUNTS_KV_KEY`]) is a compact binary form:
//!
//! ```text
//! "LGCF" | version:u8 | codec:u8 | payload
//! ```
//!
//! where `codec` is [`CODEC_RAW`] or [`CODEC_ZSTD`] and the payload decodes to
//! the body:
//!
//! ```text
//! uvarint n_columns
//!   repeat: uvarint name_len, name bytes
//!           uvarint nulls
//!           uvarint n_values
//!             repeat: uvarint shared_prefix_len   (with the previous value)
//!                     uvarint suffix_len, suffix bytes
//!                     uvarint count
//! ```
//!
//! Two properties shrink it. Values arrive in sorted order (a `BTreeMap`
//! iterates sorted), so **front coding** — storing only each value's delta from
//! its predecessor — collapses the shared prefixes that dominate real
//! dimensional columns (hostnames, paths, URLs, IPs). Counts are LEB128
//! varints, so the common small count costs one byte instead of a decimal
//! rendering plus punctuation. Whatever survives that is zstd'd. Parquet footer
//! KV values are `String`s, so the byte blob is base64'd; it still lands ~9×
//! under the JSON it replaces.
//!
//! What makes it *faster* to read is the access shape, not the encoding: JSON
//! tokenizing was never the parse cost — map construction and string
//! allocation were. The body is a single forward pass, so [`decode_column`]
//! walks past every other column's bytes without allocating and hands back
//! values in stored order, and [`decode_column_names`] skips values entirely.
//! Measured on a representative wide footer, that is ~5× faster than parsing
//! an equivalent JSON blob and pulling one column out of the resulting maps; see
//! `docs/DESIGN_group_count_footer_encoding.md`.
//!
//! **Compatibility.** A blob that fails to decode — wrong magic, unknown
//! version, truncated, foreign — is simply not usable, and every caller treats
//! `None` as "scan this file instead". So a format change is a version bump
//! plus, if the old encoding still matters, a branch on the version byte;
//! never a silent reinterpretation.
//! Streaming selection changes only the in-memory read shape, not these bytes.

use std::collections::BTreeMap;

use base64::Engine as _;

/// Parquet footer KV key holding the per-file group-count summary in this
/// module's compact format. See `docs/DESIGN_file_formats.md` for the
/// versioning contract: this is in the ACCELERATOR class, so an unreadable
/// payload costs a scan, never a wrong answer.
pub const GROUP_COUNTS_KV_KEY: &str = "siglake.group_counts.v1";

/// Blob magic. Guards against decoding a foreign or truncated KV value.
const MAGIC: [u8; 4] = *b"LGCF";
/// On-disk format version, bumped for any change to the body encoding.
const FORMAT_VERSION: u8 = 1;

/// Payload is the body verbatim (compression didn't pay).
const CODEC_RAW: u8 = 0;
/// Payload is a zstd frame of the body.
const CODEC_ZSTD: u8 = 1;

/// zstd level for footer bodies. The body is already front-coded and varint'd,
/// so this is mostly squeezing residual value-alphabet redundancy; 3 is the
/// knee — higher levels cost write time for a few percent.
const ZSTD_LEVEL: i32 = 3;

/// Ceiling on a decoded body. Footers are single-digit MB at the extreme, so
/// this is generous headroom that still bounds what a corrupt or hostile blob
/// can make us allocate.
const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;

/// One column's per-file group counts: distinct value -> rows, plus the rows
/// where the column is NULL (NULL is a valid `GROUP BY` group).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnCounts {
    /// Distinct value -> number of rows with that value.
    pub values: BTreeMap<String, u64>,
    /// Number of rows where this column is NULL.
    pub nulls: u64,
}

/// A whole file's group-count summary: column name -> its counts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupCounts {
    /// Column name -> its group counts.
    pub columns: BTreeMap<String, ColumnCounts>,
}

impl GroupCounts {
    /// Encode to the footer KV string, or `None` when there is nothing to stamp.
    pub fn encode(&self) -> Option<String> {
        encode_columns(&self.columns)
    }

    /// Decode a footer KV string, or `None` if it isn't a valid blob.
    pub fn decode(s: &str) -> Option<Self> {
        decode_columns(s).map(|columns| GroupCounts { columns })
    }
}

/// Encode `columns` to the footer KV string. Borrows, so the writer stamps
/// straight from its accumulator without cloning the value maps. `None` when
/// `columns` is empty (nothing to stamp — the caller omits the KV entirely).
pub fn encode_columns(columns: &BTreeMap<String, ColumnCounts>) -> Option<String> {
    if columns.is_empty() {
        return None;
    }
    let body = encode_body(columns);
    let compressed = zstd::encode_all(body.as_slice(), ZSTD_LEVEL).ok();
    let (codec, payload) = match compressed {
        // Only pay the decompression step when it actually bought something.
        Some(c) if c.len() < body.len() => (CODEC_ZSTD, c),
        _ => (CODEC_RAW, body),
    };
    let mut blob = Vec::with_capacity(MAGIC.len() + 2 + payload.len());
    blob.extend_from_slice(&MAGIC);
    blob.push(FORMAT_VERSION);
    blob.push(codec);
    blob.extend_from_slice(&payload);
    Some(base64::engine::general_purpose::STANDARD.encode(blob))
}

/// Decode a footer KV string into the per-column counts. `None` for anything
/// that isn't a well-formed blob — callers treat that as "no summary" and
/// scan the file instead, so this never has to distinguish corruption from
/// absence.
///
/// This materializes every column into a `BTreeMap`, which is the expensive
/// part of reading a footer — the read path should prefer [`decode_column`]
/// (one column, no map) or [`decode_column_names`] (no values at all).
pub fn decode_columns(s: &str) -> Option<BTreeMap<String, ColumnCounts>> {
    decode_body(&blob_body(s)?)
}

/// Decode **only** `column`'s counts, skipping every other column's bytes
/// without allocating, and returning the values in their stored (sorted) order
/// rather than a map. `None` when the blob is invalid or doesn't cover
/// `column` — the caller scans that file either way.
///
/// This is the shape the read path actually wants, and it is why the format is
/// faster to read and not just smaller: a file summarizes every dimensional
/// column at once, a query wants one of them, and neither the map nor the other
/// columns' strings ever need to exist.
///
/// Unlike [`decode_columns`] this returns as soon as it has the column, so it
/// does not validate the bytes after it. That is deliberate: the caller checks
/// `Σ values + nulls` against the file's row count before trusting the result.
pub fn decode_column(s: &str, column: &str) -> Option<(Vec<(String, u64)>, u64)> {
    let body = blob_body(s)?;
    let mut cur = Cursor { buf: &body, pos: 0 };
    let n_columns = cur.uvarint()?;
    for _ in 0..n_columns {
        let name = cur.bytes()?;
        let nulls = cur.uvarint()?;
        let n_values = cur.uvarint()?;
        if name != column.as_bytes() {
            skip_values(&mut cur, n_values)?;
            continue;
        }
        let mut values = Vec::with_capacity(prealloc(n_values));
        let mut prev: Vec<u8> = Vec::new();
        for _ in 0..n_values {
            let (value, count) = next_value(&mut cur, &mut prev)?;
            values.push((value, count));
        }
        return Some((values, nulls));
    }
    None
}

/// One validated column from a compact group-count blob, ready for an
/// allocation-lean streaming read.
///
/// The decoded body is retained once, while values are reconstructed into one
/// reusable scratch buffer by [`Self::for_each`]. Preparing the column validates
/// the complete body and computes its row total before a caller can consume any
/// value. A malformed or truncated aggregate therefore cannot yield a partial
/// streamed answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingColumnCounts {
    body: Vec<u8>,
    values_start: usize,
    n_values: u64,
    nulls: u64,
    total: u64,
}

impl StreamingColumnCounts {
    /// Number of groups, including the NULL group when present.
    pub fn len(&self) -> usize {
        usize::try_from(self.n_values)
            .unwrap_or(usize::MAX)
            .saturating_add(usize::from(self.nulls > 0))
    }

    /// Whether this column has no value or NULL groups.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Rows accounted for by the values and NULL group.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Approximate retained bytes for cache accounting.
    pub fn retained_bytes(&self) -> usize {
        self.body.capacity() + std::mem::size_of::<Self>()
    }

    /// Visit values in stored (ascending) order, followed by NULL when present.
    ///
    /// The `&str` is valid only for the callback invocation: the next value is
    /// reconstructed in the same scratch buffer. A caller that retains a key
    /// must copy that key, which lets a top-K selector own only its competitive
    /// entries instead of every group.
    pub fn for_each(&self, mut visit: impl FnMut(Option<&str>, u64)) {
        let mut cur = Cursor {
            buf: &self.body,
            pos: self.values_start,
        };
        let mut value = Vec::new();
        for _ in 0..self.n_values {
            let shared = usize::try_from(cur.uvarint().expect("validated shared prefix"))
                .expect("validated shared prefix fits usize");
            let suffix = cur.bytes().expect("validated suffix");
            value.truncate(shared);
            value.extend_from_slice(suffix);
            let count = cur.uvarint().expect("validated count");
            let value = std::str::from_utf8(&value).expect("validated UTF-8");
            visit(Some(value), count);
        }
        if self.nulls > 0 {
            visit(None, self.nulls);
        }
    }
}

/// Validate a compact blob and prepare one column for a streaming read.
///
/// Unlike [`decode_column`], this walks the complete body before returning.
/// That extra pass is required for selection directly from the codec: callers
/// may retain only top-K candidates, so discovering corruption after emitting
/// candidates must still fall back rather than return the partial selection.
pub fn streaming_column(s: &str, column: &str) -> Option<StreamingColumnCounts> {
    let body = blob_body(s)?;
    let mut cur = Cursor { buf: &body, pos: 0 };
    let n_columns = cur.uvarint()?;
    let mut found = None;
    for _ in 0..n_columns {
        let name = cur.bytes()?;
        std::str::from_utf8(name).ok()?;
        let nulls = cur.uvarint()?;
        let n_values = cur.uvarint()?;
        let values_start = cur.pos;
        let target = found.is_none() && name == column.as_bytes();
        if !target {
            validate_value_frames(&mut cur, n_values)?;
            continue;
        }
        let mut prev = Vec::new();
        let mut total = nulls;
        for _ in 0..n_values {
            let shared = usize::try_from(cur.uvarint()?).ok()?;
            if shared > prev.len() {
                return None;
            }
            let suffix = cur.bytes()?;
            prev.truncate(shared);
            prev.extend_from_slice(suffix);
            std::str::from_utf8(&prev).ok()?;
            let count = cur.uvarint()?;
            total = total.saturating_add(count);
        }
        found = Some((values_start, n_values, nulls, total));
    }
    if cur.pos != body.len() {
        return None;
    }
    let (values_start, n_values, nulls, total) = found?;
    Some(StreamingColumnCounts {
        body,
        values_start,
        n_values,
        nulls,
        total,
    })
}

/// Validate front-coded entry framing without reconstructing key bytes.
///
/// The predecessor length is enough to prove that each shared prefix is in
/// bounds. Non-target key bytes deliberately receive no UTF-8 validation,
/// matching [`decode_column`]'s skip behavior.
fn validate_value_frames(cur: &mut Cursor, n_values: u64) -> Option<()> {
    let mut prev_len = 0usize;
    for _ in 0..n_values {
        let shared = usize::try_from(cur.uvarint()?).ok()?;
        if shared > prev_len {
            return None;
        }
        let suffix_len = cur.bytes()?.len();
        prev_len = shared.checked_add(suffix_len)?;
        cur.uvarint()?; // count
    }
    Some(())
}

/// Every column's name and ROW TOTAL (`Σ counts + nulls`), in one forward pass
/// and without materializing a single value string.
///
/// The shape a maintenance census wants. "Is this column short of
/// `record_count`?" is a question about one number per column, and asking it
/// through [`decode_column`] once per column walks the blob once per column and
/// allocates every value of the one it stops at — at the measured extreme
/// (26.5 MB, ~20M keys, 22 columns) that is the whole blob decoded 22 times
/// over to compare 22 integers.
///
/// Counts saturate rather than wrap: a corrupt or hostile blob claiming
/// `u64::MAX` rows must not fold around into a plausible total that compares
/// equal to `record_count`.
pub fn decode_column_totals(s: &str) -> Option<Vec<(String, u64)>> {
    let body = blob_body(s)?;
    let mut cur = Cursor { buf: &body, pos: 0 };
    let n_columns = cur.uvarint()?;
    let mut totals = Vec::with_capacity(prealloc(n_columns));
    for _ in 0..n_columns {
        let name = std::str::from_utf8(cur.bytes()?).ok()?.to_string();
        let mut total = cur.uvarint()?; // nulls are rows too
        let n_values = cur.uvarint()?;
        for _ in 0..n_values {
            cur.uvarint()?; // shared prefix length
            cur.bytes()?; // suffix
            total = total.saturating_add(cur.uvarint()?);
        }
        totals.push((name, total));
    }
    Some(totals)
}

/// The column names a footer covers, without decoding any values — the warm
/// cycle's census of which columns are footer-aggregated for a table.
pub fn decode_column_names(s: &str) -> Option<Vec<String>> {
    let body = blob_body(s)?;
    let mut cur = Cursor { buf: &body, pos: 0 };
    let n_columns = cur.uvarint()?;
    let mut names = Vec::with_capacity(prealloc(n_columns));
    for _ in 0..n_columns {
        let name = std::str::from_utf8(cur.bytes()?).ok()?.to_string();
        let _nulls = cur.uvarint()?;
        let n_values = cur.uvarint()?;
        skip_values(&mut cur, n_values)?;
        names.push(name);
    }
    Some(names)
}

/// Strip the header and decompress, yielding the body bytes.
fn blob_body(s: &str) -> Option<Vec<u8>> {
    let mut blob = base64::engine::general_purpose::STANDARD
        .decode(s.trim())
        .ok()?;
    if blob.len() < MAGIC.len() + 2
        || blob[..MAGIC.len()] != MAGIC
        || blob[MAGIC.len()] != FORMAT_VERSION
    {
        return None;
    }
    let codec = blob[MAGIC.len() + 1];
    match codec {
        CODEC_RAW => {
            blob.drain(..MAGIC.len() + 2);
            Some(blob)
        }
        CODEC_ZSTD => zstd_decode_bounded(&blob[MAGIC.len() + 2..]),
        _ => None,
    }
}

/// A decoded element count is untrusted, so never reserve more than a footer
/// plausibly holds — an over-long run fails when the cursor runs dry instead.
fn prealloc(n: u64) -> usize {
    n.min(4096) as usize
}

/// Advance past `n_values` front-coded entries without materializing them.
fn skip_values(cur: &mut Cursor, n_values: u64) -> Option<()> {
    for _ in 0..n_values {
        cur.uvarint()?; // shared prefix length
        cur.bytes()?; // suffix
        cur.uvarint()?; // count
    }
    Some(())
}

/// Read one front-coded `(value, count)`, updating the predecessor buffer.
fn next_value(cur: &mut Cursor, prev: &mut Vec<u8>) -> Option<(String, u64)> {
    let shared = usize::try_from(cur.uvarint()?).ok()?;
    if shared > prev.len() {
        return None; // prefix claims more than the predecessor has
    }
    let suffix = cur.bytes()?;
    let mut value = Vec::with_capacity(shared + suffix.len());
    value.extend_from_slice(&prev[..shared]);
    value.extend_from_slice(suffix);
    let count = cur.uvarint()?;
    // `from_utf8` moves the buffer (no copy), and `prev` is a reused scratch
    // buffer, so a value costs one allocation total.
    let value = String::from_utf8(value).ok()?;
    prev.clear();
    prev.extend_from_slice(value.as_bytes());
    Some((value, count))
}

fn encode_body(columns: &BTreeMap<String, ColumnCounts>) -> Vec<u8> {
    let mut out = Vec::new();
    put_uvarint(&mut out, columns.len() as u64);
    for (name, col) in columns {
        put_bytes(&mut out, name.as_bytes());
        put_uvarint(&mut out, col.nulls);
        put_uvarint(&mut out, col.values.len() as u64);
        // Front coding: `values` iterates sorted, so consecutive values share
        // long prefixes on exactly the columns whose footers got big.
        let mut prev: &[u8] = b"";
        for (value, count) in &col.values {
            let value = value.as_bytes();
            let shared = common_prefix_len(prev, value);
            put_uvarint(&mut out, shared as u64);
            put_bytes(&mut out, &value[shared..]);
            put_uvarint(&mut out, *count);
            prev = value;
        }
    }
    out
}

fn decode_body(body: &[u8]) -> Option<BTreeMap<String, ColumnCounts>> {
    let mut cur = Cursor { buf: body, pos: 0 };
    let n_columns = cur.uvarint()?;
    let mut columns = BTreeMap::new();
    // No capacity is reserved from a decoded length anywhere in this function:
    // a corrupt count simply runs the cursor dry and fails, it can't preallocate.
    for _ in 0..n_columns {
        let name = std::str::from_utf8(cur.bytes()?).ok()?.to_string();
        let nulls = cur.uvarint()?;
        let n_values = cur.uvarint()?;
        let mut values = BTreeMap::new();
        let mut prev: Vec<u8> = Vec::new();
        for _ in 0..n_values {
            let (value, count) = next_value(&mut cur, &mut prev)?;
            values.insert(value, count);
        }
        columns.insert(name, ColumnCounts { values, nulls });
    }
    // Trailing bytes mean the blob isn't what it claims — reject rather than
    // serve a partial summary (the read-path row-count guard would likely catch
    // it, but a footer is cheap to distrust).
    if cur.pos != body.len() {
        return None;
    }
    Some(columns)
}

fn zstd_decode_bounded(payload: &[u8]) -> Option<Vec<u8>> {
    use std::io::Read as _;
    let mut decoder = zstd::stream::read::Decoder::new(payload).ok()?;
    let mut out = Vec::new();
    let read = decoder
        .by_ref()
        .take(MAX_DECODED_BYTES as u64 + 1)
        .read_to_end(&mut out)
        .ok()?;
    if read > MAX_DECODED_BYTES {
        return None;
    }
    Some(out)
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    // Byte-wise is safe for reconstruction even if it splits a codepoint: the
    // decoder concatenates the predecessor's prefix bytes with the stored
    // suffix bytes, reproducing the original byte sequence exactly (and then
    // validates UTF-8).
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_uvarint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn uvarint(&mut self) -> Option<u64> {
        let mut value: u64 = 0;
        let mut shift = 0u32;
        loop {
            let byte = *self.buf.get(self.pos)?;
            self.pos += 1;
            if shift >= 64 {
                return None; // overlong encoding
            }
            value |= u64::from(byte & 0x7f).checked_shl(shift)?;
            if byte & 0x80 == 0 {
                // Reject the non-canonical continuation that would overflow.
                if shift == 63 && byte & 0x7f > 1 {
                    return None;
                }
                return Some(value);
            }
            shift += 7;
        }
    }

    /// A length-prefixed byte run, borrowed from the body.
    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.uvarint()?).ok()?;
        let end = self.pos.checked_add(len)?;
        let out = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(values: &[(&str, u64)], nulls: u64) -> ColumnCounts {
        ColumnCounts {
            values: values.iter().map(|(v, n)| ((*v).to_string(), *n)).collect(),
            nulls,
        }
    }

    fn sample() -> GroupCounts {
        let mut columns = BTreeMap::new();
        columns.insert(
            "sourcetype".to_string(),
            col(&[("app:json", 480), ("syslog", 320)], 0),
        );
        columns.insert(
            "level".to_string(),
            col(&[("ERROR", 3), ("INFO", 1_000_000)], 7),
        );
        GroupCounts { columns }
    }

    #[test]
    fn round_trips() {
        let gc = sample();
        let blob = gc.encode().expect("non-empty encodes");
        assert_eq!(GroupCounts::decode(&blob).expect("decodes"), gc);
    }

    #[test]
    fn empty_encodes_to_nothing() {
        assert!(GroupCounts::default().encode().is_none());
    }

    #[test]
    fn round_trips_edge_values() {
        // Empty-string value, unicode, a value that is a prefix of the next
        // (front coding's boundary case), NULL-only column, and u64::MAX counts.
        let mut columns = BTreeMap::new();
        columns.insert(
            "weird".to_string(),
            col(
                &[
                    ("", 1),
                    ("host", 2),
                    ("host-01", 3),
                    ("host-01.example.com", u64::MAX),
                    ("höst-π-🎯", 5),
                ],
                u64::MAX,
            ),
        );
        columns.insert("all_null".to_string(), col(&[], 42));
        let gc = GroupCounts { columns };
        let blob = gc.encode().expect("encodes");
        assert_eq!(GroupCounts::decode(&blob).expect("decodes"), gc);
    }

    #[test]
    fn targeted_decode_matches_full_decode() {
        let gc = sample();
        let blob = gc.encode().unwrap();
        for (name, col) in &gc.columns {
            let (values, nulls) = decode_column(&blob, name).expect("covered column decodes");
            assert_eq!(nulls, col.nulls);
            // Stored order is the map's order, so this is directly comparable.
            let want: Vec<(String, u64)> =
                col.values.iter().map(|(v, n)| (v.clone(), *n)).collect();
            assert_eq!(values, want, "column {name}");
        }
        assert_eq!(decode_column(&blob, "not_a_column"), None);
        assert_eq!(
            decode_column_names(&blob),
            Some(gc.columns.keys().cloned().collect::<Vec<_>>())
        );
    }

    #[test]
    fn streaming_decode_matches_owned_decode() {
        let gc = sample();
        let blob = gc.encode().unwrap();
        for (name, col) in &gc.columns {
            let streamed = streaming_column(&blob, name).expect("covered column streams");
            let mut values = Vec::new();
            streamed.for_each(|value, count| values.push((value.map(str::to_string), count)));
            let mut want: Vec<(Option<String>, u64)> = col
                .values
                .iter()
                .map(|(value, count)| (Some(value.clone()), *count))
                .collect();
            if col.nulls > 0 {
                want.push((None, col.nulls));
            }
            assert_eq!(values, want, "column {name}");
            assert_eq!(streamed.len(), want.len());
            assert_eq!(
                streamed.total(),
                col.values
                    .values()
                    .fold(col.nulls, |total, count| total.saturating_add(*count))
            );
        }
        assert!(streaming_column(&blob, "not_a_column").is_none());
    }

    #[test]
    fn streaming_decode_rejects_damage_after_the_target() {
        let columns = sample().columns;
        let mut body = encode_body(&columns);
        body.push(0); // valid target bytes followed by an unclaimed byte
        let mut raw = Vec::from(MAGIC);
        raw.extend_from_slice(&[FORMAT_VERSION, CODEC_RAW]);
        raw.extend_from_slice(&body);
        let malformed = base64::engine::general_purpose::STANDARD.encode(raw);

        // The legacy targeted decoder intentionally returns after its column.
        assert!(decode_column(&malformed, "level").is_some());
        // Streaming selection must validate the whole aggregate first so a
        // caller never mistakes a partial visit for an exact answer.
        assert!(streaming_column(&malformed, "level").is_none());
    }

    #[test]
    fn streaming_decode_rejects_invalid_later_column() {
        let mut body = Vec::new();
        put_uvarint(&mut body, 2);
        put_bytes(&mut body, b"level");
        put_uvarint(&mut body, 0);
        put_uvarint(&mut body, 1);
        put_uvarint(&mut body, 0);
        put_bytes(&mut body, b"info");
        put_uvarint(&mut body, 1);
        put_bytes(&mut body, b"zone");
        put_uvarint(&mut body, 0);
        put_uvarint(&mut body, 1);
        put_uvarint(&mut body, 1); // impossible prefix: tracked predecessor length is zero
        put_bytes(&mut body, b"east");
        put_uvarint(&mut body, 1);

        let mut raw = Vec::from(MAGIC);
        raw.extend_from_slice(&[FORMAT_VERSION, CODEC_RAW]);
        raw.extend_from_slice(&body);
        let malformed = base64::engine::general_purpose::STANDARD.encode(raw);

        assert!(decode_column(&malformed, "level").is_some());
        // Non-target columns are checked by framing, including prefix bounds.
        assert!(streaming_column(&malformed, "level").is_none());
    }

    #[test]
    fn streaming_decode_accepts_invalid_utf8_in_non_target_column() {
        let mut body = Vec::new();
        put_uvarint(&mut body, 2);
        put_bytes(&mut body, b"level");
        put_uvarint(&mut body, 0);
        put_uvarint(&mut body, 1);
        put_uvarint(&mut body, 0);
        put_bytes(&mut body, b"info");
        put_uvarint(&mut body, 1);
        put_bytes(&mut body, b"zone");
        put_uvarint(&mut body, 0);
        put_uvarint(&mut body, 1);
        put_uvarint(&mut body, 0);
        put_bytes(&mut body, &[0xff]); // invalid UTF-8, but valid skipped framing
        put_uvarint(&mut body, 1);

        let mut raw = Vec::from(MAGIC);
        raw.extend_from_slice(&[FORMAT_VERSION, CODEC_RAW]);
        raw.extend_from_slice(&body);
        let malformed = base64::engine::general_purpose::STANDARD.encode(raw);

        // UTF-8 damage confined to a non-target column is accepted exactly as
        // decode_column accepts it; only the requested column is interpreted.
        assert!(decode_column(&malformed, "level").is_some());
        assert!(streaming_column(&malformed, "level").is_some());
    }

    #[test]
    fn column_totals_match_a_full_decode() {
        let gc = sample();
        let blob = gc.encode().unwrap();
        let want: Vec<(String, u64)> = gc
            .columns
            .iter()
            .map(|(name, col)| (name.clone(), col.values.values().sum::<u64>() + col.nulls))
            .collect();
        assert_eq!(decode_column_totals(&blob), Some(want));
        for bad in ["", "not base64 !!!", r#"{"columns":{}}"#] {
            assert_eq!(decode_column_totals(bad), None);
        }
    }

    #[test]
    fn column_totals_saturate_instead_of_wrapping() {
        // A total that overflows must not land on a small plausible number: the
        // census compares it against `record_count` to decide a column is
        // short, and a wrapped total is how a corrupt blob would read healthy.
        let mut columns = BTreeMap::new();
        columns.insert("wide".to_string(), col(&[("a", u64::MAX), ("b", 7)], 3));
        let blob = GroupCounts { columns }.encode().unwrap();
        assert_eq!(
            decode_column_totals(&blob),
            Some(vec![("wide".to_string(), u64::MAX)])
        );
    }

    #[test]
    fn targeted_decode_reads_past_a_skipped_column() {
        // The target column sits AFTER a fat one, so getting it right depends on
        // skipping the fat column's front-coded values byte-exactly.
        let mut columns = BTreeMap::new();
        columns.insert(
            "a_fat_column".to_string(),
            ColumnCounts {
                values: (0..500)
                    .map(|i| (format!("prefix-that-front-codes-{i:04}"), i as u64 + 1))
                    .collect(),
                nulls: 9,
            },
        );
        columns.insert("z_target".to_string(), col(&[("wanted", 5)], 2));
        let blob = GroupCounts { columns }.encode().unwrap();
        assert_eq!(
            decode_column(&blob, "z_target"),
            Some((vec![("wanted".to_string(), 5)], 2))
        );
    }

    #[test]
    fn targeted_decode_rejects_garbage() {
        for bad in ["", "not base64 !!!", r#"{"columns":{}}"#] {
            assert_eq!(decode_column(bad, "level"), None);
            assert_eq!(streaming_column(bad, "level"), None);
            assert_eq!(decode_column_names(bad), None);
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!(GroupCounts::decode("").is_none());
        assert!(GroupCounts::decode("not base64 at all !!!").is_none());
        // Valid base64, wrong magic.
        let wrong = base64::engine::general_purpose::STANDARD.encode(b"XXXX\x01\x00");
        assert!(GroupCounts::decode(&wrong).is_none());
        // Right magic, unknown codec.
        let bad_codec = base64::engine::general_purpose::STANDARD.encode(b"LGCF\x01\x7f");
        assert!(GroupCounts::decode(&bad_codec).is_none());
        // Right magic, FUTURE version — must fail closed, not be reinterpreted.
        let future = base64::engine::general_purpose::STANDARD.encode(b"LGCF\x02\x00");
        assert!(GroupCounts::decode(&future).is_none());
        // A JSON blob must NOT decode as the compact format: a mixed-up reader
        // has to fail closed rather than invent counts.
        let json = r#"{"columns":{"level":{"values":{"INFO":1},"nulls":0}}}"#;
        assert!(GroupCounts::decode(json).is_none());
    }

    #[test]
    fn rejects_truncation() {
        let blob = sample().encode().unwrap();
        let raw = base64::engine::general_purpose::STANDARD
            .decode(&blob)
            .unwrap();
        for cut in [5, raw.len() / 2, raw.len() - 1] {
            let truncated = base64::engine::general_purpose::STANDARD.encode(&raw[..cut]);
            assert!(
                GroupCounts::decode(&truncated).is_none(),
                "truncation at {cut} must not decode"
            );
            assert!(
                streaming_column(&truncated, "level").is_none(),
                "truncation at {cut} must not stream"
            );
        }
    }

    #[test]
    fn rejects_trailing_bytes_in_body() {
        let mut body = encode_body(&sample().columns);
        assert!(decode_body(&body).is_some(), "clean body decodes");
        body.push(0);
        assert!(
            decode_body(&body).is_none(),
            "a body with unconsumed trailing bytes isn't the summary it claims"
        );
    }

    /// The point of the format: a realistic high-cardinality column encodes far
    /// smaller than the v1 JSON for the same data.
    #[test]
    fn compact_beats_json_on_realistic_hostnames() {
        let values: Vec<(String, u64)> = (0..1024)
            .map(|i| {
                (
                    format!("web-{i:04}.us-east-1.prod.internal.example.com"),
                    (i as u64 % 97) + 1,
                )
            })
            .collect();
        let mut columns = BTreeMap::new();
        columns.insert(
            "host".to_string(),
            ColumnCounts {
                values: values.iter().cloned().collect(),
                nulls: 3,
            },
        );
        let gc = GroupCounts { columns };

        // The v1 rendering of the same data (no escapes in this data, so the
        // quoting is exactly what serde_json would emit).
        let json_len = {
            let pairs: Vec<String> = values.iter().map(|(v, n)| format!("\"{v}\":{n}")).collect();
            format!(
                r#"{{"columns":{{"host":{{"values":{{{}}},"nulls":3}}}}}}"#,
                pairs.join(",")
            )
            .len()
        };
        let blob = gc.encode().unwrap();

        assert_eq!(GroupCounts::decode(&blob).unwrap(), gc, "round-trips");
        assert!(
            blob.len() * 8 < json_len,
            "compact blob {} should be <1/8 of the v1 JSON {json_len}",
            blob.len()
        );
    }

    use proptest::prelude::*;

    proptest! {
        /// Any summary survives the round trip byte-for-byte.
        #[test]
        fn prop_round_trips(
            raw in proptest::collection::vec(
                (
                    "[a-z_]{1,12}",
                    proptest::collection::vec(("[ -~]{0,24}", 0u64..u64::MAX), 0..40),
                    0u64..1_000_000,
                ),
                0..6,
            )
        ) {
            let mut columns = BTreeMap::new();
            for (name, values, nulls) in raw {
                columns.insert(
                    name,
                    ColumnCounts {
                        values: values.into_iter().collect(),
                        nulls,
                    },
                );
            }
            let gc = GroupCounts { columns };
            match gc.encode() {
                Some(blob) => prop_assert_eq!(GroupCounts::decode(&blob), Some(gc)),
                None => prop_assert!(gc.columns.is_empty()),
            }
        }

        /// Arbitrary bytes never decode into a summary we'd act on (they may
        /// legitimately fail, but must never panic).
        #[test]
        fn prop_arbitrary_input_never_panics(s in ".{0,256}") {
            let _ = GroupCounts::decode(&s);
        }
    }
}
