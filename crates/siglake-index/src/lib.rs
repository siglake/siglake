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
    ///
    /// Every length in the blob is untrusted input — a footer KV or a Puffin
    /// blob can be corrupt on disk — so no count is allocated from before it is
    /// checked against the bytes that are left: a five-byte blob claiming four
    /// billion postings has to cost five bytes of work, not 16 GiB of `Vec`.
    /// The structural invariants [`to_bytes`](Self::to_bytes) holds are checked
    /// too — terms strictly ascending, postings strictly ascending and inside
    /// `0..n_rows` — because the reader turns postings straight into a Parquet
    /// `RowSelection` and cannot tell a corrupt ordinal from a real one. What
    /// this cannot catch is a flipped bit inside a delta that leaves the
    /// ordinals ordered and in range; there is no checksum in v1 (recorded
    /// under "Integrity" in `docs/DESIGN_segmented_inverted_index.md`), which
    /// is why the reader also checks the row domain against the file.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut c = Cursor::new(bytes);
        if c.take(4)? != INDEX_MAGIC {
            return None;
        }
        if c.u8()? != INDEX_VERSION {
            return None;
        }
        let n_rows: u32 = c.varint()?.try_into().ok()?;
        let n_terms: usize = c.varint()?.try_into().ok()?;
        // Cheapest a dictionary entry can be: a one-byte zero term length, no
        // term bytes, a one-byte posting count, and one byte for the single
        // delta that count must cover.
        if n_terms > c.remaining() / 3 {
            return None;
        }
        let mut postings: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        for _ in 0..n_terms {
            let tlen: usize = c.varint()?.try_into().ok()?;
            let term = std::str::from_utf8(c.take(tlen)?).ok()?.to_string();
            // The encoder walks a `BTreeMap`, so terms arrive strictly
            // ascending. Equal or descending means a duplicate entry — which
            // `insert` below would silently collapse, keeping the last
            // postings list and dropping the first — or a garbled dictionary.
            // (An empty term is legitimate: the `raw` tokenizer emits one for
            // a null or empty column value.)
            if postings
                .last_key_value()
                .is_some_and(|(last, _)| term.as_str() <= last.as_str())
            {
                return None;
            }
            let plen: usize = c.varint()?.try_into().ok()?;
            // A term is stored only when some row has it, and each of its
            // deltas costs at least one byte.
            if plen == 0 || plen > c.remaining() {
                return None;
            }
            let mut rows = Vec::with_capacity(plen);
            let mut prev = 0u32;
            for nth in 0..plen {
                let delta: u32 = c.varint()?.try_into().ok()?;
                // Only the first ordinal may be zero: postings are strictly
                // ascending, so every later delta is at least one.
                if nth > 0 && delta == 0 {
                    return None;
                }
                prev = prev.checked_add(delta)?;
                // Postings are row ordinals within this file. One at or past
                // `n_rows` would select a row the index does not claim to
                // cover.
                if prev >= n_rows {
                    return None;
                }
                rows.push(prev);
            }
            postings.insert(term, rows);
        }
        // A well-formed blob is consumed exactly: trailing bytes mean the
        // dictionary count disagrees with the payload.
        if c.remaining() != 0 {
            return None;
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
    /// Bytes not yet consumed — the ceiling every serialized count in the blob
    /// is checked against before anything is allocated from it.
    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }
    /// LEB128, rejecting any encoding that does not round-trip through `u64`:
    /// more than ten bytes, or a tenth byte carrying more than the single
    /// payload bit that fits. The shift alone used to drop those bits, so
    /// `0x80 … 0x80 0x7f` (ten bytes) decoded as a plausible small count
    /// instead of being refused.
    fn varint(&mut self) -> Option<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.u8()?;
            if shift >= 64 {
                return None; // overlong / corrupt
            }
            let payload = (byte & 0x7f) as u64;
            if payload << shift >> shift != payload {
                return None; // the shift would silently discard value bits
            }
            result |= payload << shift;
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

    /// A blob header, then whatever body the case wants: the shortest way to
    /// hand the decoder a hostile length.
    fn blob(n_rows: u64, n_terms: u64, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(INDEX_MAGIC);
        out.push(INDEX_VERSION);
        write_varint(&mut out, n_rows);
        write_varint(&mut out, n_terms);
        out.extend_from_slice(body);
        out
    }

    /// One dictionary entry, spelled out so a case can break exactly one field.
    fn term_entry(term: &str, deltas: &[u64]) -> Vec<u8> {
        let mut out = Vec::new();
        write_varint(&mut out, term.len() as u64);
        out.extend_from_slice(term.as_bytes());
        write_varint(&mut out, deltas.len() as u64);
        for delta in deltas {
            write_varint(&mut out, *delta);
        }
        out
    }

    /// The header-only refusals: a count that no remaining payload could
    /// justify has to be rejected from the count itself, before any `Vec` is
    /// reserved. The decoder allocated `Vec::with_capacity(plen)` straight from
    /// the serialized posting count, so a nine-byte blob could ask for 16 GiB
    /// (task #4558). Nothing here asserts on allocation directly — the proof is
    /// that the blob is refused while it is still a handful of bytes.
    #[test]
    fn from_bytes_refuses_counts_the_payload_cannot_cover() {
        // 2^32 - 1 terms behind an empty body.
        let bytes = blob(10, u32::MAX as u64, &[]);
        assert!(bytes.len() < 16, "the hostile blob stays tiny: {bytes:?}");
        assert!(InvertedIndex::from_bytes(&bytes).is_none());
        // ... and behind a body that could hold one entry, not four billion.
        assert!(
            InvertedIndex::from_bytes(&blob(10, u32::MAX as u64, &term_entry("ab", &[1])))
                .is_none()
        );
        // 2^32 - 1 postings for a real term behind two bytes of deltas.
        let mut body = Vec::new();
        write_varint(&mut body, 2);
        body.extend_from_slice(b"ab");
        write_varint(&mut body, u32::MAX as u64);
        body.extend_from_slice(&[1, 1]);
        let bytes = blob(10, 1, &body);
        assert!(bytes.len() < 24, "the hostile blob stays tiny: {bytes:?}");
        assert!(InvertedIndex::from_bytes(&bytes).is_none());
        // A term length past the end of the blob.
        assert!(InvertedIndex::from_bytes(&blob(10, 1, &[200, b'a', b'b'])).is_none());
        // The exact-fit boundary is accepted: an empty term with one posting is
        // three bytes, so one term behind three bytes is plausible.
        assert_eq!(
            InvertedIndex::from_bytes(&blob(1, 1, &term_entry("", &[0])))
                .map(|index| index.n_terms()),
            Some(1)
        );
    }

    /// Counts that do not fit the types they are read into. `n_rows` and the
    /// postings were narrowed with `as u32`, so `n_rows = 2^32` decoded as 0
    /// and a delta of `2^32 + 5` as 5 — a silently different index, not a
    /// refusal.
    #[test]
    fn from_bytes_refuses_values_that_do_not_fit_their_field() {
        assert!(
            InvertedIndex::from_bytes(&blob(1u64 << 32, 0, &[])).is_none(),
            "n_rows past u32"
        );
        assert!(
            InvertedIndex::from_bytes(&blob(u64::MAX, 0, &[])).is_none(),
            "n_rows at u64::MAX"
        );
        assert!(
            InvertedIndex::from_bytes(&blob(100, 1, &term_entry("ab", &[(1u64 << 32) + 5])))
                .is_none(),
            "posting delta past u32"
        );
        // A delta that fits u32 but walks the running ordinal past u32.
        assert!(
            InvertedIndex::from_bytes(&blob(
                u32::MAX as u64,
                1,
                &term_entry("ab", &[u32::MAX as u64, u32::MAX as u64])
            ))
            .is_none(),
            "running ordinal overflows u32"
        );
    }

    /// Postings are the reader's row ordinals. Out of domain, out of order, or
    /// repeated, they have to be refused rather than handed to
    /// `row_selection_runs`, which silently drops what it cannot place.
    #[test]
    fn from_bytes_refuses_postings_outside_the_row_domain() {
        // n_rows = 3, so ordinal 3 does not exist.
        assert!(InvertedIndex::from_bytes(&blob(3, 1, &term_entry("ab", &[3]))).is_none());
        assert!(InvertedIndex::from_bytes(&blob(3, 1, &term_entry("ab", &[1, 2]))).is_none());
        // Zero rows cannot carry a posting at all.
        assert!(InvertedIndex::from_bytes(&blob(0, 1, &term_entry("ab", &[0]))).is_none());
        // A zero delta after the first repeats the previous ordinal.
        assert!(InvertedIndex::from_bytes(&blob(9, 1, &term_entry("ab", &[2, 0]))).is_none());
        // An empty postings list: a term is only stored when a row has it.
        assert!(InvertedIndex::from_bytes(&blob(9, 1, &term_entry("ab", &[]))).is_none());
        // In-domain and ascending is accepted, including ordinal 0 and the last.
        assert_eq!(
            InvertedIndex::from_bytes(&blob(3, 1, &term_entry("abc", &[0, 2])))
                .and_then(|index| index.postings("abc").map(<[u32]>::to_vec)),
            Some(vec![0, 2])
        );
    }

    /// The dictionary is serialized in `BTreeMap` order, so a repeated or
    /// descending term is corruption. `insert` used to collapse a duplicate,
    /// keeping the second postings list and dropping the first — a decode that
    /// succeeds with fewer terms than the blob claims.
    #[test]
    fn from_bytes_refuses_duplicate_or_unordered_terms() {
        let mut duplicate = term_entry("ab", &[0]);
        duplicate.extend_from_slice(&term_entry("ab", &[1]));
        assert!(InvertedIndex::from_bytes(&blob(9, 2, &duplicate)).is_none());

        let mut descending = term_entry("cd", &[0]);
        descending.extend_from_slice(&term_entry("ab", &[1]));
        assert!(InvertedIndex::from_bytes(&blob(9, 2, &descending)).is_none());

        let mut ascending = term_entry("ab", &[0]);
        ascending.extend_from_slice(&term_entry("cd", &[1]));
        assert_eq!(
            InvertedIndex::from_bytes(&blob(9, 2, &ascending)).map(|index| index.n_terms()),
            Some(2)
        );
    }

    /// Bytes past the last dictionary entry mean the term count disagrees with
    /// the payload, which is the truncation case seen from the other end.
    #[test]
    fn from_bytes_refuses_trailing_payload() {
        let mut bytes = idx().to_bytes();
        bytes.push(0);
        assert!(InvertedIndex::from_bytes(&bytes).is_none());
        let mut bytes = idx().to_bytes();
        bytes.extend_from_slice(&term_entry("zzzz", &[1]));
        assert!(InvertedIndex::from_bytes(&bytes).is_none());
    }

    /// Every one-byte truncation of a valid blob, and every single-byte
    /// mutation of its length fields, must come back as `None` or as a decoded
    /// index — never as a panic and never as an allocation the blob cannot pay
    /// for. Runs the mutations to completion rather than sampling, since the
    /// blob is small.
    #[test]
    fn from_bytes_survives_truncation_and_length_mutation() {
        let good = idx().to_bytes();
        for cut in 0..good.len() {
            assert!(
                InvertedIndex::from_bytes(&good[..cut]).is_none(),
                "a truncated blob is never valid (cut at {cut})"
            );
        }
        for pos in 0..good.len() {
            for replacement in [0x00u8, 0x01, 0x7f, 0x80, 0xff] {
                let mut mutated = good.clone();
                mutated[pos] = replacement;
                if let Some(index) = InvertedIndex::from_bytes(&mutated) {
                    // Whatever survives must still be internally consistent:
                    // every posting inside the row domain it declares.
                    for (term, rows) in index.terms() {
                        assert!(
                            rows.iter().all(|row| *row < index.n_rows()),
                            "term {term:?} escaped the row domain after \
                             byte {pos} := {replacement:#04x}"
                        );
                        assert!(rows.windows(2).all(|w| w[0] < w[1]), "postings unordered");
                    }
                }
            }
        }
    }

    #[test]
    fn varint_round_trips_boundaries() {
        for v in [
            0u64,
            1,
            127,
            128,
            300,
            16383,
            16384,
            u32::MAX as u64,
            u64::MAX - 1,
            u64::MAX,
        ] {
            let mut b = Vec::new();
            write_varint(&mut b, v);
            let mut c = Cursor::new(&b);
            assert_eq!(c.varint(), Some(v), "varint {v}");
            assert_eq!(c.remaining(), 0, "varint {v} consumed exactly its bytes");
        }
    }

    /// The tenth byte of a `u64` LEB128 holds one payload bit. Anything more
    /// was shifted off the top and the varint decoded as a small number, so
    /// `2^64 + 1` read back as `1` and reached a `Vec::with_capacity` as a
    /// plausible count.
    #[test]
    fn varint_refuses_payload_bits_that_do_not_fit_u64() {
        // Ten bytes, tenth byte = 2: bit 64, which has nowhere to go.
        let overflow = [0x80u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02];
        assert_eq!(Cursor::new(&overflow).varint(), None);
        // Tenth byte = 1 is the largest that fits, and is u64::MAX's encoding.
        let max = [0xffu8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        assert_eq!(Cursor::new(&max).varint(), Some(u64::MAX));
        // Tenth byte = 0x7f: six payload bits past the top of a u64.
        let dropped = [0x80u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x7f];
        assert_eq!(Cursor::new(&dropped).varint(), None);
        // Eleven bytes is never valid, whatever the last one carries.
        let overlong = [0x80u8; 11];
        assert_eq!(Cursor::new(&overlong).varint(), None);
        // Unterminated (continuation bit on every byte, then end of input).
        assert_eq!(Cursor::new(&[0x80u8, 0x80]).varint(), None);
    }
}
