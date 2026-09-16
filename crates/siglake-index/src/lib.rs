//! Per-file **inverted index**: term → sorted row-ordinal postings.
//!
//! The token blooms (`siglake-bloom`) prune at file / row-group granularity —
//! "this block *might* contain the term." This index is the exact, row-level
//! complement: for a block it answers "*which rows* contain the term," so a
//! `raw LIKE '%term%'` / term search can skip the non-matching rows in a block
//! instead of materializing them (the WS-3 RowSelection bridge). It is built
//! once per data file at write/compaction time and serialized into a side blob
//! (Puffin-style) keyed to that file.
//!
//! Tokenization is shared with the blooms ([`siglake_bloom::tokenize`] /
//! [`normalize_query_term`](siglake_bloom::normalize_query_term)) so a term that
//! a bloom admits is looked up here under the same normalization — no
//! index/bloom skew. Postings are row ordinals **within the file** (0-based, in
//! the file's physical row order), so `doc_id == row ordinal`, the identity the
//! WS-3 bridge needs to map a hit back to a Parquet row.
//!
//! Scope (first slice): the core structure, build, query (single term + AND of
//! terms), and a compact self-describing serialization. Wiring into the write
//! path and the query-time RowSelection are later slices.

use std::borrow::Cow;
use std::collections::BTreeMap;

use siglake_bloom::{normalize_query_term, Tokenizer};

pub mod segmented;

/// Magic prefixing a serialized index blob.
const INDEX_MAGIC: &[u8; 4] = b"KIDX";
/// Serialization version.
const INDEX_VERSION: u8 = 1;

/// Parquet `key_value_metadata` key under which a data file carries its
/// hex-encoded inverted-index blob (set at write time, read at query time —
/// mirrors `siglake_bloom::RAW_TRIGRAM_BLOOM_KV_KEY`).
pub const INVERTED_INDEX_KV_KEY: &str = "siglake.inverted_index.v1";

/// Footer-KV key for `column`'s inverted-index blob. `raw` keeps the original
/// key for on-disk back-compat; other columns use a per-column suffix.
pub fn inverted_index_kv_key(column: &str) -> Cow<'static, str> {
    if column == "raw" {
        Cow::Borrowed(INVERTED_INDEX_KV_KEY)
    } else {
        Cow::Owned(format!("{INVERTED_INDEX_KV_KEY}.{column}"))
    }
}

/// An immutable inverted index over one file's rows. Terms are normalized exactly
/// as the blooms normalize them; postings are sorted, deduplicated row ordinals.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InvertedIndex {
    /// Normalized term → ascending row ordinals containing it.
    postings: BTreeMap<String, Vec<u32>>,
    /// Number of rows indexed (the ordinal space is `0..n_rows`).
    n_rows: u32,
}

/// Accumulates rows into an [`InvertedIndex`]. Feed each row's `raw` text in
/// physical row order; the ordinal is the call count.
pub struct IndexBuilder {
    postings: BTreeMap<String, Vec<u32>>,
    n_rows: u32,
    tokenizer: Tokenizer,
}

impl Default for IndexBuilder {
    fn default() -> Self {
        Self {
            postings: BTreeMap::new(),
            n_rows: 0,
            tokenizer: Tokenizer::Default,
        }
    }
}

impl IndexBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tokenizer(tokenizer: Tokenizer) -> Self {
        Self {
            tokenizer,
            ..Self::default()
        }
    }

    /// Index the next row's `raw` text. The row ordinal is assigned in call
    /// order (0-based), so callers must push rows in the file's physical order.
    pub fn push_row(&mut self, raw: &str) {
        let ordinal = self.n_rows;
        // Distinct tokens only — a term appears once per row in the postings.
        let mut seen: Vec<String> = self.tokenizer.tokenize(raw);
        seen.sort();
        seen.dedup();
        for token in seen {
            self.postings.entry(token).or_default().push(ordinal);
        }
        self.n_rows += 1;
    }

    /// Finalize. Postings are already ascending (ordinals pushed in order) and
    /// per-row-distinct, so no extra sort/dedup is needed.
    pub fn build(self) -> InvertedIndex {
        InvertedIndex {
            postings: self.postings,
            n_rows: self.n_rows,
        }
    }
}

impl InvertedIndex {
    /// Build directly from an iterator of rows' `raw` text, in physical order.
    pub fn from_rows<'a, I: IntoIterator<Item = &'a str>>(rows: I) -> Self {
        Self::from_rows_with_tokenizer(rows, Tokenizer::Default)
    }

    pub fn from_rows_with_tokenizer<'a, I: IntoIterator<Item = &'a str>>(
        rows: I,
        tokenizer: Tokenizer,
    ) -> Self {
        let mut b = IndexBuilder::with_tokenizer(tokenizer);
        for raw in rows {
            b.push_row(raw);
        }
        b.build()
    }

    /// Number of rows indexed (the ordinal space is `0..n_rows()`).
    pub fn n_rows(&self) -> u32 {
        self.n_rows
    }

    /// Number of distinct indexed terms.
    pub fn n_terms(&self) -> usize {
        self.postings.len()
    }

    /// Approximate resident size of the parsed index, for bounding a cache of
    /// parsed indexes by bytes: the postings and term bytes plus a fixed
    /// allowance per dictionary entry. It ignores `BTreeMap` node slack and
    /// `Vec` over-allocation, so it is a floor, not an exact footprint — close
    /// enough to keep a cache under a byte budget, not a heap accounting tool.
    pub fn heap_size_bytes(&self) -> usize {
        const ENTRY_OVERHEAD: usize =
            std::mem::size_of::<String>() + std::mem::size_of::<Vec<u32>>();
        std::mem::size_of::<Self>()
            + self
                .postings
                .iter()
                .map(|(term, rows)| {
                    ENTRY_OVERHEAD + term.len() + rows.len() * std::mem::size_of::<u32>()
                })
                .sum::<usize>()
    }

    /// The dictionary in term order, each term with its ascending postings. The
    /// serializers walk this; so does the experimental segmented encoder
    /// ([`segmented::SegmentedWriter::push_group_index`]), which is what lets a
    /// segmented sidecar be built from an index that already exists without
    /// re-tokenizing its rows.
    pub fn terms(&self) -> impl ExactSizeIterator<Item = (&str, &[u32])> {
        self.postings
            .iter()
            .map(|(term, rows)| (term.as_str(), rows.as_slice()))
    }

    /// Ascending row ordinals containing `term` (normalized as the blooms do),
    /// or `None` when the term is absent / not indexable. A present-but-empty
    /// result is impossible (a term is only stored if some row has it).
    pub fn postings(&self, term: &str) -> Option<&[u32]> {
        let norm = normalize_query_term(term)?;
        self.postings.get(&norm).map(Vec::as_slice)
    }

    /// Row ordinals matching **all** `terms` (AND) — the intersection of their
    /// postings, ascending. Empty when any term is absent or unindexable, so an
    /// empty result is a definitive "no rows match" (the caller can skip the
    /// whole block). An empty `terms` list matches nothing.
    pub fn matching_rows_all(&self, terms: &[&str]) -> Vec<u32> {
        if terms.is_empty() {
            return Vec::new();
        }
        // Resolve every term first; a single miss ⇒ no matches.
        let mut lists: Vec<&[u32]> = Vec::with_capacity(terms.len());
        for t in terms {
            match self.postings(t) {
                Some(p) => lists.push(p),
                None => return Vec::new(),
            }
        }
        // Intersect smallest-first to minimize work.
        lists.sort_by_key(|l| l.len());
        let mut acc: Vec<u32> = lists[0].to_vec();
        for list in &lists[1..] {
            acc = intersect_sorted(&acc, list);
            if acc.is_empty() {
                break;
            }
        }
        acc
    }

    /// Row ordinals matching `raw LIKE '%substr%'`, or `None` when this index
    /// can't answer that substring exactly (the caller then falls back to a full
    /// scan / `FilterExec`).
    ///
    /// Answerable iff `substr` normalizes via [`normalize_query_term`] — i.e. it
    /// is delimiter-free and ≥ `MIN_TOKEN_LEN`. For such a substring, "`substr`
    /// occurs in `raw`" ⟺ "`substr` occurs within some token of `raw`" (it can't
    /// span a token delimiter), and every token long enough to contain it is
    /// indexed — so the union of postings over **every dictionary term that
    /// contains the normalized substring** is exactly the matching rows. The
    /// result is at least a superset of the true `LIKE` matches (case-folded), so
    /// a caller may treat it as candidates and re-check the exact predicate.
    pub fn rows_containing(&self, substr: &str) -> Option<Vec<u32>> {
        let norm = normalize_query_term(substr)?;
        let mut acc: Vec<u32> = Vec::new();
        for (term, rows) in &self.postings {
            if term.contains(&norm) {
                acc = union_sorted(&acc, rows);
            }
        }
        Some(acc)
    }

    /// Convert this index's matches for `terms` (AND) into **row-selection
    /// runs** over the whole file: a sequence of `(selected, length)` covering
    /// `0..n_rows` with no zero-length or adjacent same-kind runs. This is a
    /// Parquet-`RowSelection` in a Parquet-agnostic form — the storage layer
    /// maps each run to a `RowSelector::{select,skip}` to skip the non-matching
    /// rows of a block (the WS-3 bridge). An all-skip result (every run
    /// `(false, _)`) means the block can be skipped entirely.
    pub fn matching_row_selection(&self, terms: &[&str]) -> Vec<(bool, u32)> {
        row_selection_runs(&self.matching_rows_all(terms), self.n_rows)
    }

    /// Serialize to a compact, self-describing blob: a 4-byte magic + version +
    /// row count + term count, then each term (ascending) with its postings
    /// **delta-encoded** as LEB128 varints (postings are ascending, so deltas
    /// are small and pack tightly — the whole point of an index over a raw list).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(INDEX_MAGIC);
        out.push(INDEX_VERSION);
        write_varint(&mut out, self.n_rows as u64);
        write_varint(&mut out, self.postings.len() as u64);
        for (term, rows) in &self.postings {
            write_varint(&mut out, term.len() as u64);
            out.extend_from_slice(term.as_bytes());
            write_varint(&mut out, rows.len() as u64);
            let mut prev = 0u32;
            for &r in rows {
                write_varint(&mut out, (r - prev) as u64);
                prev = r;
            }
        }
        out
    }

    /// [`to_bytes`](Self::to_bytes) as a lowercase-hex string, for a Parquet
    /// footer-KV value (which must be UTF-8). See [`INVERTED_INDEX_KV_KEY`].
    pub fn to_hex(&self) -> String {
        let bytes = self.to_bytes();
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
            s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
        }
        s
    }

    /// Inverse of [`to_hex`](Self::to_hex). `None` on non-hex / odd-length input
    /// or an invalid blob.
    pub fn from_hex(s: &str) -> Option<Self> {
        if !s.len().is_multiple_of(2) {
            return None;
        }
        let bytes: Option<Vec<u8>> = s
            .as_bytes()
            .chunks(2)
            .map(|pair| {
                let hi = (pair[0] as char).to_digit(16)?;
                let lo = (pair[1] as char).to_digit(16)?;
                Some(((hi << 4) | lo) as u8)
            })
            .collect();
        Self::from_bytes(&bytes?)
    }

    /// Parse a blob produced by [`to_bytes`](Self::to_bytes). Returns `None` on a
    /// bad magic/version or a truncated/garbled body.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut c = Cursor::new(bytes);
        if c.take(4)? != INDEX_MAGIC {
            return None;
        }
        if c.u8()? != INDEX_VERSION {
            return None;
        }
        let n_rows = c.varint()? as u32;
        let n_terms = c.varint()? as usize;
        let mut postings = BTreeMap::new();
        for _ in 0..n_terms {
            let tlen = c.varint()? as usize;
            let term = std::str::from_utf8(c.take(tlen)?).ok()?.to_string();
            let plen = c.varint()? as usize;
            let mut rows = Vec::with_capacity(plen);
            let mut prev = 0u32;
            for _ in 0..plen {
                prev = prev.checked_add(c.varint()? as u32)?;
                rows.push(prev);
            }
            postings.insert(term, rows);
        }
        Some(Self { postings, n_rows })
    }
}

/// Build `(selected, length)` runs over `0..n_rows` from the ascending,
/// deduplicated `selected` ordinals. Coalesces adjacent runs of the same kind
/// and emits none past `n_rows`. Ordinals `>= n_rows` (shouldn't occur) are
/// ignored defensively.
pub fn row_selection_runs(selected: &[u32], n_rows: u32) -> Vec<(bool, u32)> {
    let mut runs: Vec<(bool, u32)> = Vec::new();
    let mut push = |kind: bool, len: u32| {
        if len == 0 {
            return;
        }
        match runs.last_mut() {
            Some((k, l)) if *k == kind => *l += len,
            _ => runs.push((kind, len)),
        }
    };
    let mut cursor = 0u32; // next un-emitted ordinal
    for &ord in selected {
        if ord >= n_rows || ord < cursor {
            continue;
        }
        push(false, ord - cursor); // skipped gap before this hit
        push(true, 1); // the hit
        cursor = ord + 1;
    }
    push(false, n_rows.saturating_sub(cursor)); // trailing skip
    runs
}

/// Union of two ascending, deduplicated `u32` slices.
fn union_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// Intersection of two ascending, deduplicated `u32` slices.
fn intersect_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

/// Minimal forward byte cursor for [`InvertedIndex::from_bytes`].
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|s| s[0])
    }
    fn varint(&mut self) -> Option<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.u8()?;
            if shift >= 64 {
                return None; // overlong / corrupt
            }
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Some(result);
            }
            shift += 7;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idx() -> InvertedIndex {
        // row 0: "error connecting to database"
        // row 1: "user login ok"
        // row 2: "database error: timeout"
        InvertedIndex::from_rows([
            "error connecting to database",
            "user login ok",
            "database error: timeout",
        ])
    }

    #[test]
    fn postings_are_ascending_distinct_row_ordinals() {
        let i = idx();
        assert_eq!(i.n_rows(), 3);
        assert_eq!(i.postings("error"), Some([0u32, 2].as_slice()));
        assert_eq!(i.postings("database"), Some([0u32, 2].as_slice()));
        assert_eq!(i.postings("login"), Some([1u32].as_slice()));
        // Case-insensitive (normalized like the blooms).
        assert_eq!(i.postings("ERROR"), i.postings("error"));
        // Absent term.
        assert_eq!(i.postings("kafka"), None);
    }

    #[test]
    fn repeated_term_in_a_row_appears_once() {
        // "error error error" ⇒ row 0 listed once for `error`.
        let i = InvertedIndex::from_rows(["error error error", "calm"]);
        assert_eq!(i.postings("error"), Some([0u32].as_slice()));
    }

    #[test]
    fn matching_rows_all_intersects_terms() {
        let i = idx();
        // Both "database" {0,2} AND "error" {0,2} ⇒ {0,2}.
        assert_eq!(i.matching_rows_all(&["database", "error"]), vec![0, 2]);
        // "error" {0,2} AND "timeout" {2} ⇒ {2}.
        assert_eq!(i.matching_rows_all(&["error", "timeout"]), vec![2]);
        // Any absent term ⇒ definitive no-match.
        assert_eq!(i.matching_rows_all(&["error", "kafka"]), Vec::<u32>::new());
        // Empty query ⇒ nothing.
        assert_eq!(i.matching_rows_all(&[]), Vec::<u32>::new());
    }

    #[test]
    fn rows_containing_substring_unions_matching_terms() {
        // row 0: error connecting database
        // row 1: user login okay
        // row 2: database error timeout
        let i = idx();
        // "data" is a substring of token "database" {0,2}.
        assert_eq!(i.rows_containing("data"), Some(vec![0, 2]));
        // "err" ⊂ "error" {0,2}.
        assert_eq!(i.rows_containing("err"), Some(vec![0, 2]));
        // "login" exact token {1}.
        assert_eq!(i.rows_containing("login"), Some(vec![1]));
        // No token contains "xyz".
        assert_eq!(i.rows_containing("xyz"), Some(vec![]));
        // Not answerable: too short / contains a delimiter ⇒ None (caller scans).
        assert_eq!(i.rows_containing("ab"), None);
        assert_eq!(i.rows_containing("error timeout"), None);
        // Case-insensitive (index is lowercased) ⇒ at least a superset.
        assert_eq!(i.rows_containing("ERROR"), Some(vec![0, 2]));
    }

    #[test]
    fn union_sorted_dedups_and_orders() {
        assert_eq!(union_sorted(&[1, 3, 5], &[2, 3, 6]), vec![1, 2, 3, 5, 6]);
        assert_eq!(union_sorted(&[], &[2, 4]), vec![2, 4]);
        assert_eq!(union_sorted(&[1, 2], &[]), vec![1, 2]);
    }

    #[test]
    fn row_selection_runs_cover_the_block() {
        // matches at rows 0 and 2 of 3 ⇒ select 1, skip 1, select 1.
        let i = idx();
        assert_eq!(
            i.matching_row_selection(&["error"]),
            vec![(true, 1), (false, 1), (true, 1)]
        );
        // Every run's lengths sum to n_rows, and kinds alternate.
        let runs = i.matching_row_selection(&["error"]);
        assert_eq!(runs.iter().map(|(_, l)| l).sum::<u32>(), i.n_rows());
        assert!(
            runs.windows(2).all(|w| w[0].0 != w[1].0),
            "no adjacent same-kind runs"
        );

        // No matches ⇒ a single all-skip run (skip the whole block).
        assert_eq!(i.matching_row_selection(&["kafka"]), vec![(false, 3)]);
        // Leading + trailing selects coalesce correctly.
        assert_eq!(row_selection_runs(&[0, 1, 2], 3), vec![(true, 3)]);
        assert_eq!(
            row_selection_runs(&[1], 4),
            vec![(false, 1), (true, 1), (false, 2)]
        );
        // Empty block.
        assert_eq!(row_selection_runs(&[], 0), Vec::<(bool, u32)>::new());
    }

    #[test]
    fn heap_size_tracks_postings_and_terms() {
        let small = idx();
        let big = InvertedIndex::from_rows(
            (0..1000)
                .map(|row| format!("database timeout row-{row}"))
                .collect::<Vec<_>>()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        );
        assert!(small.heap_size_bytes() > std::mem::size_of::<InvertedIndex>());
        assert!(
            big.heap_size_bytes() > 10 * small.heap_size_bytes(),
            "a thousand-row index must account far above a three-row one: {} vs {}",
            big.heap_size_bytes(),
            small.heap_size_bytes()
        );
        // The postings dominate: 1000 rows carry "database" and "timeout".
        assert!(big.heap_size_bytes() > 2 * 1000 * std::mem::size_of::<u32>());
        assert_eq!(
            InvertedIndex::default().heap_size_bytes(),
            std::mem::size_of::<InvertedIndex>()
        );
    }

    #[test]
    fn round_trips_through_bytes() {
        let i = idx();
        let bytes = i.to_bytes();
        let back = InvertedIndex::from_bytes(&bytes).expect("valid blob");
        assert_eq!(i, back);
        assert_eq!(back.postings("timeout"), Some([2u32].as_slice()));
    }

    #[test]
    fn hex_round_trips_and_rejects_bad_input() {
        let i = idx();
        assert_eq!(InvertedIndex::from_hex(&i.to_hex()), Some(i));
        assert!(InvertedIndex::from_hex("abc").is_none(), "odd length");
        assert!(InvertedIndex::from_hex("zz").is_none(), "non-hex");
        assert_eq!(
            InvertedIndex::from_hex(&InvertedIndex::default().to_hex()),
            Some(InvertedIndex::default())
        );
    }

    #[test]
    fn from_bytes_rejects_garbage() {
        assert!(InvertedIndex::from_bytes(b"").is_none());
        assert!(
            InvertedIndex::from_bytes(b"XXXX\x01").is_none(),
            "bad magic"
        );
        let mut bytes = idx().to_bytes();
        bytes[4] = 99; // bad version
        assert!(InvertedIndex::from_bytes(&bytes).is_none());
        // Truncation mid-body.
        let good = idx().to_bytes();
        assert!(InvertedIndex::from_bytes(&good[..good.len() - 1]).is_none());
    }

    #[test]
    fn empty_index_round_trips() {
        let i = InvertedIndex::from_rows(std::iter::empty::<&str>());
        assert_eq!(i.n_rows(), 0);
        assert_eq!(i.n_terms(), 0);
        assert_eq!(InvertedIndex::from_bytes(&i.to_bytes()), Some(i));
    }

    #[test]
    fn varint_round_trips_boundaries() {
        for v in [0u64, 1, 127, 128, 300, 16383, 16384, u32::MAX as u64] {
            let mut b = Vec::new();
            write_varint(&mut b, v);
            let mut c = Cursor::new(&b);
            assert_eq!(c.varint(), Some(v), "varint {v}");
        }
    }
}
