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
//!
//! ## Two forms
//!
//! The index has two in-memory forms. [`InvertedIndex`] is the original
//! map-and-vectors form (`BTreeMap<String, Vec<u32>>`); it is what the v1
//! stored blob decodes to, and is kept as-is so historical files read
//! unchanged. [`FlatIndex`] is the flat form — one term byte array, one
//! postings buffer, one [`TermSpan`] per term — which the same answer set
//! charges far less resident memory (no per-term `String`/`Vec` headers, no
//! map/vector slack). It is the intended canonical resident form:
//!
//! - **from scratch** — [`FlatIndex::from_rows`] / [`FlatIndexBuilder`] build
//!   it directly, with no intermediate map;
//! - **from a v1 file** — [`InvertedIndex::flat`] converts a decoded
//!   [`InvertedIndex`];
//! - **on the wire** — [`FlatIndex::to_bytes_v2`] /
//!   [`FlatIndex::from_bytes_v2`] round-trip the v2 blob (the write path
//!   flipping to emit v2 is the maintainer's integration, not this change).
//!
//! Both forms answer identically: `postings`, `matching_rows_all`, and
//! `rows_containing` are implemented on each.

use std::borrow::Cow;
use std::collections::BTreeMap;

use siglake_bloom::{normalize_query_term, Tokenizer, MIN_TOKEN_LEN};

pub mod segmented;
pub mod spill;

/// Magic prefixing a serialized index blob.
const INDEX_MAGIC: &[u8; 4] = b"KIDX";
/// Serialization version of the stored blob (footer KV / Puffin sidecar). This
/// is what the write path emits today; readers must keep parsing it for
/// historical files.
const INDEX_VERSION: u8 = 1;
/// Serialization version of the flat (resident) layout — the intended
/// canonical form. In this POC it is the in-memory form a v1 decode re-parses
/// into, and it round-trips through [`FlatIndex::to_bytes_v2`] /
/// [`FlatIndex::from_bytes_v2`]; the write path flipping to emit it is the
/// maintainer's integration, not part of this change.
const INDEX_VERSION_2: u8 = 2;
/// The target size of the jump table's local window, in bytes of term data.
/// One anchor is recorded per `TARGET_WINDOW_BYTES` of `term_bytes`, so a
/// point lookup narrows to a ≤ 32 KB window (L1) at any index size — the
/// anchor count scales with the index, keeping the window in L1 from a few
/// MB up to the 4 GB `u32`-offset cap.
const TARGET_WINDOW_BYTES: usize = 32 * 1024;

/// Parquet `key_value_metadata` key under which a data file carries its
/// hex-encoded inverted-index blob (set at write time, read at query time —
/// mirrors `siglake_bloom::RAW_TRIGRAM_BLOOM_KV_KEY`).
pub const INVERTED_INDEX_KV_KEY: &str = "siglake.inverted_index.v1";

/// Parquet `key_value_metadata` namespace for the CRC-32 of a footer-KV
/// inverted-index blob. This is deliberately not below [`INVERTED_INDEX_KV_KEY`]:
/// `siglake.inverted_index.v1.crc32` is already the blob key for a column named
/// `crc32`.
pub const INVERTED_INDEX_CRC32_KV_KEY: &str = "siglake.inverted_index.crc32.v1";

/// Footer-KV key for `column`'s inverted-index blob. `raw` keeps the original
/// key for on-disk back-compat; other columns use a per-column suffix.
pub fn inverted_index_kv_key(column: &str) -> Cow<'static, str> {
    if column == "raw" {
        Cow::Borrowed(INVERTED_INDEX_KV_KEY)
    } else {
        Cow::Owned(format!("{INVERTED_INDEX_KV_KEY}.{column}"))
    }
}

/// Footer-KV key for `column`'s inverted-index CRC-32. `raw` mirrors the
/// legacy unsuffixed blob key; other column names are preserved as suffixes.
pub fn inverted_index_crc32_kv_key(column: &str) -> Cow<'static, str> {
    if column == "raw" {
        Cow::Borrowed(INVERTED_INDEX_CRC32_KV_KEY)
    } else {
        Cow::Owned(format!("{INVERTED_INDEX_CRC32_KV_KEY}.{column}"))
    }
}

/// CRC-32 of the serialized bytes carried by a footer-KV inverted index.
pub fn inverted_index_crc32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

/// CRC-32 of a hex-encoded footer-KV blob, without allocating its decoded
/// bytes. `None` means the footer value is not even-length hexadecimal.
pub fn inverted_index_hex_crc32(hex: &str) -> Option<u32> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut hasher = crc32fast::Hasher::new();
    for pair in hex.as_bytes().chunks(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        hasher.update(&[((hi << 4) | lo) as u8]);
    }
    Some(hasher.finalize())
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

/// One dictionary entry: where a term lives in the two shared buffers.
/// Private — a `FlatIndex` is only ever produced by a constructor that
/// validates the layout, so the readers may rely on the invariants.
///
/// The offsets are `u32` and are enforced to fit by every constructor (each
/// refuses an input whose buffers would not fit, so the cap is checked, not
/// assumed). `term_len` is `u32`, not `u16`, because the `Raw` tokenizer
/// makes the entire line a single token, so a term can be longer than 65535
/// bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct TermSpan {
    /// Byte offset of this term in the term buffer.
    bytes_start: u32,
    /// Byte length of this term in the term buffer.
    term_len: u32,
    /// Offset of this term's first posting in the postings buffer.
    postings_start: u32,
}

/// The resident (canonical) layout: one term byte array, all postings in a
/// single `u32` buffer, and one 12-byte span per term — all in **sorted term
/// order**. A derived jump table (`anchors`) provides O(log) point lookups
/// with an L1-resident local window, so the sorted array stays contiguous
/// for the substring sweep and the wire format.
///
/// Compared with the map-and-vectors form this drops the per-entry
/// `String` + `Vec<u32>` headers (48 bytes per term on 64-bit), the
/// `BTreeMap` node slack and the per-`Vec` capacity slack. What it buys is a
/// smaller cache charge for the same answer set: `heap_size_bytes` on the map
/// form is the number the parsed-index budgets were sized from, and it
/// overstates this layout by the per-entry headers and the slack.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FlatIndex {
    terms: Vec<TermSpan>,
    postings: Vec<u32>,
    term_bytes: Vec<u8>,
    /// Jump table: `(first-8-bytes-of-term, term_index)`, one anchor every
    /// `TARGET_WINDOW_BYTES` of term bytes. Binary-searched to narrow a point
    /// lookup to a ≤ 32 KB local window (L1) before the final binary search.
    anchors: Vec<(u64, usize)>,
    n_rows: u32,
}

impl FlatIndex {
    /// [`normalize_query_term`](siglake_bloom::normalize_query_term) without
    /// the allocation: `Some(bytes)` when `term` is *already* in indexed
    /// form — length ≥ `MIN_TOKEN_LEN` and every byte in `[a-z0-9]` (the exact
    /// set `normalize_query_term` produces via `to_ascii_lowercase`). Any
    /// uppercase, whitespace, or short term returns `None` and the caller
    /// takes the allocating path, which must produce the identical answer.
    fn normalize_fast(term: &str) -> Option<&[u8]> {
        let b = term.as_bytes();
        (b.len() >= MIN_TOKEN_LEN).then_some(b).filter(|b| {
            b.iter()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        })
    }

    /// Build from the map-and-vectors form. Borrows the map, so during the
    /// conversion both the map and the flat buffers are resident at once; use
    /// [`InvertedIndex::consume`] to avoid that peak. The term order is
    /// preserved (it is the `BTreeMap` order), and the buffers are sized
    /// exactly, so the resident charge carries no growth headroom.
    pub fn from_map(map: &BTreeMap<String, Vec<u32>>, n_rows: u32) -> Self {
        let n_terms = map.len();
        let term_bytes_total: usize = map.keys().map(String::len).sum();
        let postings_total: usize = map.values().map(Vec::len).sum();
        let mut term_bytes = Vec::with_capacity(term_bytes_total);
        let mut postings = Vec::with_capacity(postings_total);
        let mut terms = Vec::with_capacity(n_terms);
        for (term, rows) in map {
            // Checked: a buffer past 4 GiB would truncate a `u32` offset and
            // silently corrupt the layout, so refuse it instead.
            let bytes_start = u32::try_from(term_bytes.len())
                .expect("a resident index's term buffer fits a u32 offset");
            let postings_start = u32::try_from(postings.len())
                .expect("a resident index's postings buffer fits a u32 offset");
            let term_len =
                u32::try_from(term.len()).expect("a resident index's term fits a u32 length");
            term_bytes.extend_from_slice(term.as_bytes());
            postings.extend_from_slice(rows);
            terms.push(TermSpan {
                bytes_start,
                term_len,
                postings_start,
            });
        }
        Self::from_parts(terms, postings, term_bytes, n_rows)
    }

    /// Assemble from already-built **sorted** buffers (term-sorted, contiguous
    /// spans, ascending in-domain postings). The buffers go straight into the
    /// resident layout — the sorted array is the wire format and the
    /// contiguous sweep target — and the jump table is derived from them.
    /// Every constructor goes through here, so a `FlatIndex` is always valid.
    ///
    /// [`validate`](Self::validate) is a full `O(n)` pass over every buffer, so
    /// it runs only in debug builds as a constructor tripwire — free on the
    /// release load path. An out-of-band construction (e.g. FFI) that bypasses
    /// the checked builders should call it explicitly.
    fn from_parts(
        terms: Vec<TermSpan>,
        postings: Vec<u32>,
        term_bytes: Vec<u8>,
        n_rows: u32,
    ) -> Self {
        // Derive the jump table: one anchor per `TARGET_WINDOW_BYTES` of term
        // bytes, so a point lookup narrows to a ≤ 32 KB window (L1) at any
        // index size. The key is the term's first 8 bytes zero-padded, read
        // big-endian (most-significant = first byte), so the keys are in the
        // same lexicographic order as the terms.
        let mut anchors = Vec::new();
        if !terms.is_empty() && term_bytes.len() >= TARGET_WINDOW_BYTES {
            let mut next_anchor_at = TARGET_WINDOW_BYTES;
            for (i, span) in terms.iter().enumerate() {
                let bytes_start = span.bytes_start as usize;
                if bytes_start >= next_anchor_at {
                    let term = &term_bytes[bytes_start..bytes_start + span.term_len as usize];
                    let mut prefix = [0u8; 8];
                    let n = term.len().min(8);
                    prefix[..n].copy_from_slice(&term[..n]);
                    anchors.push((u64::from_be_bytes(prefix), i));
                    next_anchor_at += TARGET_WINDOW_BYTES;
                }
            }
        }
        let index = Self {
            terms,
            postings,
            term_bytes,
            anchors,
            n_rows,
        };
        if cfg!(debug_assertions) {
            index.validate();
        }
        index
    }

    /// Check the layout invariants: contiguous, in-bounds, strictly ascending
    /// UTF-8, non-empty term spans; non-empty, strictly ascending, in-domain
    /// postings; and each jump-table anchor's prefix matches its term.
    /// Constructors call this (debug builds); it is public so an out-of-band
    /// construction (e.g. FFI) can verify before use. Panics on a violation.
    pub fn validate(&self) -> &Self {
        let total_bytes = self.term_bytes.len();
        let total_postings = self.postings.len();
        let mut bytes_prev_end = 0usize;
        let mut postings_prev_end = 0usize;
        let mut last_term: Option<&[u8]> = None;
        for (i, span) in self.terms.iter().enumerate() {
            let bytes_end = bytes_prev_end + span.term_len as usize;
            assert!(
                bytes_prev_end == span.bytes_start as usize && bytes_end <= total_bytes,
                "term spans must be contiguous and in bounds"
            );
            let postings_end = self.postings_end(i);
            assert!(
                postings_prev_end == span.postings_start as usize
                    && postings_end >= postings_prev_end
                    && postings_end <= total_postings,
                "posting spans must be contiguous and in bounds"
            );
            let term = &self.term_bytes[bytes_prev_end..bytes_end];
            assert!(!term.is_empty(), "a term must not be empty");
            assert!(
                std::str::from_utf8(term).is_ok(),
                "term bytes must be valid UTF-8"
            );
            if let Some(prev) = last_term {
                assert!(prev < term, "terms must be strictly ascending");
            }
            last_term = Some(term);
            let rows = &self.postings[postings_prev_end..postings_end];
            assert!(!rows.is_empty(), "a term must have at least one posting");
            for w in rows.windows(2) {
                assert!(w[0] < w[1], "postings must be strictly ascending");
            }
            if let Some(&last) = rows.last() {
                assert!(last < self.n_rows, "postings must be inside the row domain");
            }
            bytes_prev_end = bytes_end;
            postings_prev_end = postings_end;
        }
        // Each anchor's stored prefix must match its term's first 8 bytes.
        for &(prefix, i) in &self.anchors {
            let span = &self.terms[i];
            let term = &self.term_bytes
                [span.bytes_start as usize..span.bytes_start as usize + span.term_len as usize];
            let mut expected = [0u8; 8];
            let n = term.len().min(8);
            expected[..n].copy_from_slice(&term[..n]);
            assert_eq!(
                prefix,
                u64::from_be_bytes(expected),
                "anchor prefix must match its term"
            );
        }
        self
    }

    /// Build directly from an iterator of rows' `raw` text, in physical order.
    /// The from-scratch path to the flat layout — no intermediate
    /// [`InvertedIndex`] — mirroring [`InvertedIndex::from_rows`].
    pub fn from_rows<'a, I: IntoIterator<Item = &'a str>>(rows: I) -> Self {
        Self::from_rows_with_tokenizer(rows, Tokenizer::Default)
    }

    /// [`from_rows`](Self::from_rows) with an explicit tokenizer.
    pub fn from_rows_with_tokenizer<'a, I: IntoIterator<Item = &'a str>>(
        rows: I,
        tokenizer: Tokenizer,
    ) -> Self {
        let mut b = FlatIndexBuilder::with_tokenizer(tokenizer);
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
        self.terms.len()
    }

    /// Resident heap of this layout: the three vectors' *capacities*, the
    /// jump table, plus the struct itself. Because every builder sizes the
    /// buffers exactly, the capacity is the real allocation — no growth
    /// headroom to bill, unlike the map form's floor. (Allocator overhead is
    /// not counted; the figure is the byte total the buffers request, which
    /// is what the cache budget wants.)
    pub fn heap_size_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.terms.capacity() * std::mem::size_of::<TermSpan>()
            + self.postings.capacity() * std::mem::size_of::<u32>()
            + self.term_bytes.capacity()
            + self.anchors.capacity() * std::mem::size_of::<(u64, usize)>()
    }

    /// The dictionary in term order, each term with its ascending
    /// postings — the same view `InvertedIndex::terms` exposes. The buffers
    /// are in sorted term order, so this is a direct iteration.
    pub fn terms(&self) -> impl ExactSizeIterator<Item = (&str, &[u32])> + '_ {
        self.terms.iter().enumerate().map(|(i, span)| {
            let term = std::str::from_utf8(
                &self.term_bytes
                    [span.bytes_start as usize..span.bytes_start as usize + span.term_len as usize],
            )
            .expect("a FlatIndex built here holds valid UTF-8 terms");
            (
                term,
                &self.postings[span.postings_start as usize..self.postings_end(i)],
            )
        })
    }

    /// The term bytes for dictionary entry `i`.
    fn term_bytes_at(&self, i: usize) -> &[u8] {
        let span = &self.terms[i];
        &self.term_bytes
            [span.bytes_start as usize..span.bytes_start as usize + span.term_len as usize]
    }

    /// The `postings` slice's end index for term `i`: the next term's
    /// `postings_start`, or the buffer end for the last term. The one place
    /// the "where does term `i`'s postings end?" rule lives.
    fn postings_end(&self, i: usize) -> usize {
        if i + 1 < self.terms.len() {
            self.terms[i + 1].postings_start as usize
        } else {
            self.postings.len()
        }
    }

    /// Find the term index for `query` (already normalized bytes) using the
    /// jump table: binary-search the anchors (L2-resident) to narrow to a
    /// ≤ `TARGET_WINDOW_BYTES` local window, then binary-search that window
    /// (L1-resident). Falls back to a full binary search when the jump table
    /// is empty (tiny index). `None` when the term is absent.
    fn term_index(&self, query: &[u8]) -> Option<usize> {
        // The query's 8-byte zero-padded prefix, read big-endian (most-
        // significant = first byte) so it orders lexicographically.
        let mut qprefix = [0u8; 8];
        let n = query.len().min(8);
        qprefix[..n].copy_from_slice(&query[..n]);
        let qkey = u64::from_be_bytes(qprefix);
        // Binary-search the anchors for the first one with prefix > qkey; `lo`
        // is that index (or the end when every anchor is <= qkey). The window
        // that contains `qkey` starts at the last anchor <= qkey and ends at
        // the first anchor > qkey — one 32 KB window, no more.
        let mut lo = 0usize;
        let mut hi = self.anchors.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.anchors[mid].0 <= qkey {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // The window's start is the last anchor with prefix <= qkey. But a
        // *tied* prefix (first 8 bytes equal) is decided by the 9th+ bytes,
        // which the key cannot see: when `anchors[lo-1]`'s prefix equals
        // `qkey`, the query's full term may sort *before* that anchor, so the
        // window must reach one anchor further back to include it.
        let mut start = if lo == 0 { 0 } else { self.anchors[lo - 1].1 };
        if lo >= 1 && self.anchors[lo - 1].0 == qkey {
            start = if lo >= 2 { self.anchors[lo - 2].1 } else { 0 };
        }
        let end = if lo < self.anchors.len() {
            self.anchors[lo].1
        } else {
            self.terms.len()
        };
        // Binary-search the local window.
        let mut wlo = start;
        let mut whi = end;
        while wlo < whi {
            let mid = (wlo + whi) / 2;
            match self.term_bytes_at(mid).cmp(query) {
                std::cmp::Ordering::Less => wlo = mid + 1,
                std::cmp::Ordering::Greater => whi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Ascending row ordinals containing `term` (normalized as the blooms do),
    /// or `None` when the term is absent / not indexable. Mirrors
    /// [`InvertedIndex::postings`].
    pub fn postings(&self, term: &str) -> Option<&[u32]> {
        // Fast path: an already-indexed term (lowercase ASCII alphanumeric)
        // is used in place, skipping the `normalize_query_term` allocation.
        // The fallback allocates but produces a byte-identical answer.
        match Self::normalize_fast(term) {
            Some(b) => self.postings_norm(b),
            None => {
                let owned = normalize_query_term(term)?;
                self.postings_norm(owned.as_bytes())
            }
        }
    }

    /// [`postings`](Self::postings) for an already-normalized term: the
    /// fast/slow split above only changes how the bytes are obtained, never
    /// how they are looked up.
    fn postings_norm(&self, norm: &[u8]) -> Option<&[u32]> {
        let i = self.term_index(norm)?;
        let span = &self.terms[i];
        Some(&self.postings[span.postings_start as usize..self.postings_end(i)])
    }

    /// Row ordinals matching **all** `terms` (AND) — the intersection of their
    /// postings, ascending. Mirrors [`InvertedIndex::matching_rows_all`].
    pub fn matching_rows_all(&self, terms: &[&str]) -> Vec<u32> {
        if terms.is_empty() {
            return Vec::new();
        }
        let mut lists: Vec<&[u32]> = Vec::with_capacity(terms.len());
        for t in terms {
            match self.postings(t) {
                Some(p) => lists.push(p),
                None => return Vec::new(),
            }
        }
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

    /// Row ordinals matching `raw LIKE '%substr%'`, mirroring
    /// [`InvertedIndex::rows_containing`]: the union of postings over every
    /// dictionary term containing the normalized substring.
    pub fn rows_containing(&self, substr: &str) -> Option<Vec<u32>> {
        let norm = normalize_query_term(substr)?;
        let mut acc: Vec<u32> = Vec::new();
        for (term, rows) in self.terms() {
            if term.contains(&norm) {
                acc = union_sorted(&acc, rows);
            }
        }
        Some(acc)
    }

    /// Serialize to the v2 wire form: the magic prefix with version
    /// [`INDEX_VERSION_2`], then `n_rows`, the term count, the **total posting
    /// count**, each term's length, all the term bytes in term order, and per
    /// term its posting count and delta-encoded postings. The term and posting
    /// offsets are cumulative, so they are not stored — which keeps the blob
    /// about the v1 size. Storing the total posting count is what lets
    /// [`from_bytes_v2`] preallocate its postings buffer without a second pass
    /// over the (large) posting section. [`from_bytes_v2`] parses it. This is
    /// the canonical blob the layout is meant to be stored as; in this POC
    /// nothing writes it yet (the write path emits v1).
    pub fn to_bytes_v2(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(INDEX_MAGIC);
        out.push(INDEX_VERSION_2);
        write_varint(&mut out, self.n_rows as u64);
        write_varint(&mut out, self.terms.len() as u64);
        write_varint(&mut out, self.postings.len() as u64);
        for span in &self.terms {
            write_varint(&mut out, span.term_len as u64);
        }
        out.extend_from_slice(&self.term_bytes);
        for (i, span) in self.terms.iter().enumerate() {
            let rows = &self.postings[span.postings_start as usize..self.postings_end(i)];
            write_varint(&mut out, rows.len() as u64);
            let mut prev = 0u32;
            for &r in rows {
                write_varint(&mut out, (r - prev) as u64);
                prev = r;
            }
        }
        out
    }

    /// Parse a blob produced by [`to_bytes_v2`]. Returns `None` on a bad
    /// magic/version or a truncated/garbled body.
    ///
    /// One forward pass over the whole blob: the header carries the total
    /// posting count, so the postings buffer is preallocated exactly up front
    /// and the (large) posting section is read **once** — there is no dry-run
    /// pass and no seek back over it. Offsets are derived cumulatively in
    /// `usize` (never as a `u32` sum that could overflow), and every length is
    /// untrusted input checked against the bytes that remain.
    ///
    /// The semantic checks (strictly-ascending UTF-8 terms, strictly-ascending
    /// in-domain postings) are made here, on untrusted bytes, returning `None`
    /// rather than reaching [`Self::validate`], which panics by design — the
    /// same untrusted-input posture [`InvertedIndex::from_bytes`] takes.
    pub fn from_bytes_v2(bytes: &[u8]) -> Option<Self> {
        let mut c = Cursor::new(bytes);
        if c.take(4)? != INDEX_MAGIC {
            return None;
        }
        let version = c.u8()?;
        if version != INDEX_VERSION_2 {
            return None;
        }
        let n_rows: u32 = c.varint()?.try_into().ok()?;
        let n_terms: usize = c.varint()?.try_into().ok()?;
        let total_postings: usize = c.varint()?.try_into().ok()?;
        // Cheap guard against a huge `n_terms` driving the term-length read
        // (and the term-spans `Vec`) before any term is actually present: each
        // term costs at least one byte for its length varint and one for its
        // (non-empty) posting count.
        if n_terms > c.remaining() / 2 {
            return None;
        }
        // Term section: the per-term lengths, then the concatenated term bytes.
        // All offsets derived cumulatively in `usize` (never a `u32` sum).
        let mut terms = Vec::with_capacity(n_terms);
        let mut bytes_cursor = 0usize;
        for _ in 0..n_terms {
            let term_len: usize = c.varint()?.try_into().ok()?;
            if term_len == 0 || bytes_cursor.checked_add(term_len)? > c.remaining() {
                return None;
            }
            let bytes_start = u32::try_from(bytes_cursor).ok()?;
            let term_len_u32 = u32::try_from(term_len).ok()?;
            terms.push(TermSpan {
                bytes_start,
                term_len: term_len_u32,
                postings_start: 0, // derived from `plen` in the decode below
            });
            bytes_cursor += term_len;
        }
        let term_bytes = c.take(bytes_cursor)?.to_vec();
        // The v2 form must have strictly ascending UTF-8 terms in buffer
        // order.
        let mut last_term: Option<&str> = None;
        for span in &terms {
            let term = std::str::from_utf8(
                &term_bytes
                    [span.bytes_start as usize..span.bytes_start as usize + span.term_len as usize],
            )
            .ok()?;
            if let Some(prev) = last_term {
                if prev >= term {
                    return None;
                }
            }
            last_term = Some(term);
        }
        // Posting section: one forward decode into a buffer preallocated to the
        // header's total. The total is untrusted — a corrupt or hostile value
        // can over-allocate here — but that is the same corruption-DoS surface
        // the footer CRC covers, and this system reads back only its own files,
        // so no guard is added; see `docs/LIMITATIONS.md`.
        let mut postings = Vec::with_capacity(total_postings);
        let mut postings_cursor = 0u32;
        for span in &mut terms {
            let plen: u32 = c.varint()?.try_into().ok()?;
            if plen == 0 || (plen as usize) > c.remaining() {
                return None;
            }
            span.postings_start = postings_cursor;
            postings_cursor = postings_cursor.checked_add(plen)?;
            decode_delta_postings(&mut c, n_rows, plen as usize, &mut postings)?;
        }
        // A well-formed blob is consumed exactly: trailing bytes mean the
        // counts in the header disagree with the payload.
        if c.remaining() != 0 {
            return None;
        }
        Some(Self::from_parts(terms, postings, term_bytes, n_rows))
    }
}

/// The distinct, sorted tokens of a row's `raw` text, normalized exactly as
/// the blooms normalize them. Shared by both builders so the map form and the
/// flat form index a row identically — a skew between them would be an index
/// that answers differently depending only on which layout built it.
fn row_tokens(tokenizer: &Tokenizer, raw: &str) -> Vec<String> {
    let mut seen = tokenizer.tokenize(raw);
    seen.sort();
    seen.dedup();
    seen
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
        for token in row_tokens(&self.tokenizer, raw) {
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

/// Accumulates rows into a [`FlatIndex`] — the from-scratch path to the flat
/// layout, mirroring [`InvertedIndex::from_rows`].
///
/// It is a thin wrapper over [`IndexBuilder`]: the rows are accumulated the
/// same way (per-term postings, ascending ordinals), and [`build`](Self::build)
/// flattens the result into the flat layout. It avoids the *named*
/// `InvertedIndex` intermediate, not the accumulation map — a truly
/// allocation-light flat build is a later optimization.
#[derive(Default)]
pub struct FlatIndexBuilder {
    inner: IndexBuilder,
}

impl FlatIndexBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tokenizer(tokenizer: Tokenizer) -> Self {
        Self {
            inner: IndexBuilder::with_tokenizer(tokenizer),
        }
    }

    /// Index the next row's `raw` text. The row ordinal is assigned in call
    /// order (0-based), so callers must push rows in the file's physical
    /// order. Delegates to [`IndexBuilder::push_row`].
    pub fn push_row(&mut self, raw: &str) {
        self.inner.push_row(raw)
    }

    /// Finalize into the flat layout by consuming the accumulated index, so
    /// the map is dropped as the flat buffers are built (no simultaneous
    /// map + flat peak).
    pub fn build(self) -> FlatIndex {
        self.inner.build().consume()
    }
}

/// Decode `count` delta-encoded postings from `c` into `out`, appending.
/// `None` if a delta does not round-trip through `u64`, a later delta is zero
/// (postings are strictly ascending), the running ordinal overflows `u32`, or
/// an ordinal walks past the row domain `0..n_rows`. The caller reads the count
/// and bounds it against `c.remaining()`; this reads exactly `count` deltas.
/// Writing into a caller buffer (rather than returning a fresh `Vec`) is what
/// lets the v2 decoder append straight into its one preallocated postings
/// buffer with no per-term allocation or second copy. Shared by the v1 and v2
/// decoders so the posting invariants live in one place.
fn decode_delta_postings(
    c: &mut Cursor,
    n_rows: u32,
    count: usize,
    out: &mut Vec<u32>,
) -> Option<()> {
    let mut prev = 0u32;
    for nth in 0..count {
        let delta: u32 = c.varint()?.try_into().ok()?;
        // Only the first ordinal may be zero: postings are strictly ascending,
        // so every later delta is at least one.
        if nth > 0 && delta == 0 {
            return None;
        }
        prev = prev.checked_add(delta)?;
        // Postings are row ordinals within this file. One at or past `n_rows`
        // would select a row the index does not claim to cover.
        if prev >= n_rows {
            return None;
        }
        out.push(prev);
    }
    Some(())
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

    /// The flat layout of this index: term-sorted spans over one postings
    /// buffer. This is the v1→v2 conversion — the path that loads a
    /// historical v1 file into the canonical form. It borrows the map, so both
    /// the map and the flat buffers are resident during the conversion; use
    /// [`consume`](Self::consume) to avoid that peak. A writer that already
    /// has the rows should use [`FlatIndex::from_rows`] / [`FlatIndexBuilder`]
    /// instead. `heap_size_bytes` on the result is the resident charge the
    /// parsed-index budgets should use once they hold the flat form.
    pub fn flat(&self) -> FlatIndex {
        FlatIndex::from_map(&self.postings, self.n_rows)
    }

    /// Consume this map-form index into the flat layout. Unlike
    /// [`flat`](Self::flat), the map is moved, so the caller cannot keep using
    /// it; during the conversion both forms are briefly resident (the flat
    /// buffers are built from the map), after which the map is dropped. This
    /// is the path a cache-load of a historical v1 file should take — the
    /// result is the resident form, and the map is not returned to the caller.
    pub fn consume(self) -> FlatIndex {
        FlatIndex::from_map(&self.postings, self.n_rows)
    }

    /// Parse a resident-layout index back into the map form, so the two forms
    /// stay interchangeable.
    pub fn from_flat(flat: &FlatIndex) -> Option<Self> {
        let mut postings = BTreeMap::new();
        for (term, rows) in flat.terms() {
            postings.insert(term.to_string(), rows.to_vec());
        }
        Some(Self {
            postings,
            n_rows: flat.n_rows(),
        })
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
    /// ordinals ordered and in range; there is no checksum in v1, which is why
    /// the reader also checks the row domain against the file. What that
    /// residual costs a query depends on where the blob is stored — a Puffin
    /// sidecar's Zstd frame covers it, a Parquet footer value does not —
    /// measured in `docs/DESIGN_inverted_index.md` under "Which storage path
    /// carries that residual".
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
            // The map form keeps a `Vec` per term, so this decodes into a
            // fresh one (the v2 decoder appends into its single buffer).
            let mut rows = Vec::with_capacity(plen);
            decode_delta_postings(&mut c, n_rows, plen, &mut rows)?;
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
    fn footer_checksum_keys_cannot_collide_with_blob_keys() {
        for column in ["raw", "crc32", "message", "crc32.message", ""] {
            assert_ne!(
                inverted_index_kv_key(column),
                inverted_index_crc32_kv_key(column),
                "column {column:?}"
            );
        }
        assert_eq!(
            inverted_index_kv_key("crc32"),
            "siglake.inverted_index.v1.crc32"
        );
        assert_eq!(
            inverted_index_crc32_kv_key("crc32"),
            "siglake.inverted_index.crc32.v1.crc32"
        );
    }

    #[test]
    fn footer_checksum_covers_the_blob_bytes_encoded_as_hex() {
        let bytes = idx().to_bytes();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            inverted_index_hex_crc32(&hex),
            Some(inverted_index_crc32(&bytes))
        );
        assert_eq!(inverted_index_hex_crc32("abc"), None);
        assert_eq!(inverted_index_hex_crc32("zz"), None);
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
    fn flat_layout_matches_the_map_form() {
        let i = idx();
        let flat = i.flat();
        // Same dictionary, same postings, term order.
        let flat_terms: Vec<(&str, Vec<u32>)> =
            flat.terms().map(|(t, rows)| (t, rows.to_vec())).collect();
        let map_terms: Vec<(&str, Vec<u32>)> =
            i.terms().map(|(t, rows)| (t, rows.to_vec())).collect();
        assert_eq!(flat_terms, map_terms);
        assert_eq!(flat.n_rows, i.n_rows());
        // The query API answers the same as the map form, term by term and
        // across the AND / substring shapes.
        for term in [
            "error",
            "database",
            "login",
            "timeout",
            "connecting",
            "absent",
            "ERROR",
        ] {
            assert_eq!(
                flat.postings(term).map(<[u32]>::to_vec),
                i.postings(term).map(<[u32]>::to_vec),
                "postings({term})"
            );
        }
        assert_eq!(
            flat.matching_rows_all(&["database", "error"]),
            i.matching_rows_all(&["database", "error"])
        );
        assert_eq!(
            flat.matching_rows_all(&["error", "kafka"]),
            i.matching_rows_all(&["error", "kafka"])
        );
        assert_eq!(flat.rows_containing("data"), i.rows_containing("data"));
        assert_eq!(flat.rows_containing("xyz"), i.rows_containing("xyz"));
        assert_eq!(flat.rows_containing("ab"), i.rows_containing("ab"));
        // The buffers are sized exactly: the charge is the capacity, and a
        // builder that grew them would bill the cache for headroom.
        assert_eq!(flat.term_bytes.capacity(), flat.term_bytes.len());
        assert_eq!(flat.postings.capacity(), flat.postings.len());
        assert_eq!(flat.terms.capacity(), flat.terms.len());
        // The map form's charge overstates the flat form's by the per-entry
        // headers and the BTreeMap/Vec slack.
        assert!(
            flat.heap_size_bytes() < i.heap_size_bytes(),
            "flat {} must charge under the map form {}",
            flat.heap_size_bytes(),
            i.heap_size_bytes()
        );
        // And the flat charge is exact: the vectors' capacities + the
        // jump table + the struct itself.
        let exact = std::mem::size_of::<FlatIndex>()
            + flat.terms.capacity() * std::mem::size_of::<TermSpan>()
            + flat.postings.capacity() * std::mem::size_of::<u32>()
            + flat.term_bytes.capacity()
            + flat.anchors.capacity() * std::mem::size_of::<(u64, usize)>();
        assert_eq!(flat.heap_size_bytes(), exact);
        // A span is 12 bytes: three u32s, no padding.
        assert_eq!(std::mem::size_of::<TermSpan>(), 12);
        // Back to the map form, and it round-trips.
        assert_eq!(InvertedIndex::from_flat(&flat), Some(i.clone()));
    }

    #[test]
    fn normalize_fast_agrees_with_the_slow_path() {
        // The fast path must return `Some` exactly when it is safe to use the
        // term in place, and the answer it yields must equal the allocating
        // `normalize_query_term` on every input — a divergence would be an
        // index that answers differently depending on a term's casing.
        let cases = [
            "queen",      // already indexed: fast
            "rareneedle", // already indexed: fast
            "row-000000", // hyphen: slow
            "QUEEN",      // uppercase: slow (must not borrow as-is)
            "Queen",      // mixed case: slow
            "q1",         // too short: both None
            "  queen",    // leading space: slow
            "queen ",     // trailing space: slow
            "queen x",    // interior space: slow
            "quéen",      // non-ASCII: slow
            "",           // empty: both None
            "a1b2c3",     // alnum lowercase: fast
        ];
        for term in cases {
            let fast = FlatIndex::normalize_fast(term);
            let slow = normalize_query_term(term);
            // The invariant is one-way: a fast path is only taken when the
            // bytes are already in indexed form, so `fast = Some(b)` implies
            // `slow = Some(s)` with `b == s`. `fast = None` is always safe —
            // the allocating path answers whatever it answers.
            if let (Some(f), Some(s)) = (fast, &slow) {
                assert_eq!(f, s.as_bytes(), "{term:?}");
            } else if fast.is_some() {
                panic!("{term:?}: fast path taken but slow path is None");
            }
        }
        // The one that matters: an uppercase term must NOT be fast-pathed,
        // or `postings("QUEEN")` would search for the literal and miss.
        assert!(FlatIndex::normalize_fast("QUEEN").is_none());
        assert_eq!(normalize_query_term("QUEEN").as_deref(), Some("queen"));
    }

    #[test]
    fn flat_point_lookup_is_correct_when_the_jump_table_is_populated() {
        // The jump table only exists once `term_bytes` crosses
        // `TARGET_WINDOW_BYTES` (32 KB). Every other flat test stays under
        // that, so the lookup falls back to a full binary search and a
        // broken window computation passes silently. This one builds well
        // past the threshold and checks the jump-table path against the map
        // form: every present term, plus a sweep of absent probes in between.
        let n = 8_000usize; // 8_000 * 10 B = 80 KB of term bytes
        let keys: Vec<String> = (0..n).map(|k| format!("term{k:06}")).collect();
        let flat = FlatIndex::from_rows(keys.iter().map(String::as_str));
        assert!(
            !flat.anchors.is_empty(),
            "the index must be large enough to derive a jump table"
        );
        let i = InvertedIndex::from_rows(keys.iter().map(String::as_str));
        // Every present term, including those near window boundaries (the
        // anchors sit at 32 KB intervals, so mid-window and boundary terms
        // are the ones a window off-by-one drops).
        for k in (0..n).step_by(37) {
            let term = format!("term{k:06}");
            assert_eq!(
                flat.postings(&term).map(<[u32]>::to_vec),
                i.postings(&term).map(<[u32]>::to_vec),
                "present term {term}"
            );
        }
        // Absent probes: between-term and out-of-domain terms must be `None`,
        // not a wrong neighbour (a too-narrow window returns `None`, a too-
        // wide one risks a false hit).
        for k in 1..n {
            let probe = format!("term{k:06}x");
            assert!(flat.postings(&probe).is_none(), "absent term {probe}");
        }
        // A substring sweep must still agree with the map form: the whole-
        // dictionary walk is the one path that never touches the jump table,
        // so it guards the resident layout the jump table indexes.
        assert_eq!(
            flat.rows_containing("term0001"),
            i.rows_containing("term0001")
        );
    }

    #[test]
    fn flat_from_rows_matches_the_v1_to_v2_path() {
        // The from-scratch path and the v1→v2 path must produce the identical
        // flat layout for the same rows: same spans, same postings, same bytes,
        // same charge.
        let rows: Vec<String> = (0..500)
            .map(|r| format!("service-{} status {} row-{r:04}", r % 7, 200 + r % 3))
            .collect();
        let via_map = InvertedIndex::from_rows(rows.iter().map(String::as_str)).flat();
        let direct = FlatIndex::from_rows(rows.iter().map(String::as_str));
        assert_eq!(direct, via_map, "from-scratch and v1→v2 must agree");
        assert_eq!(
            direct.to_bytes_v2(),
            via_map.to_bytes_v2(),
            "the two paths must serialize identically"
        );
        assert_eq!(direct.heap_size_bytes(), via_map.heap_size_bytes());
        // And it answers like the map form it stands in for. The terms are
        // delimiter-free and ≥ `MIN_TOKEN_LEN` so `postings` actually indexes
        // them — a hyphenated term would normalize to `None` on both sides and
        // the comparison would pass vacuously.
        let i = InvertedIndex::from_rows(rows.iter().map(String::as_str));
        for term in ["service", "status", "row", "absent"] {
            assert_eq!(
                direct.postings(term).map(<[u32]>::to_vec),
                i.postings(term).map(<[u32]>::to_vec)
            );
        }
        // At least one is present, so the loop above is not all-`None`.
        assert!(direct.postings("service").is_some());
    }

    #[test]
    fn flat_v2_round_trips_and_is_version_separated() {
        let i = idx();
        let flat = i.flat();
        let bytes = flat.to_bytes_v2();
        let back = FlatIndex::from_bytes_v2(&bytes).expect("v2 blob round-trips");
        assert_eq!(back, flat);
        assert_eq!(InvertedIndex::from_flat(&back), Some(i.clone()));
        // The two versions refuse each other's blobs: the stored form is v1,
        // the resident form v2, and a reader must not parse one as the other.
        assert!(
            InvertedIndex::from_bytes(&bytes).is_none(),
            "v2 blob is not v1"
        );
        assert!(
            FlatIndex::from_bytes_v2(&i.to_bytes()).is_none(),
            "v1 blob is not v2"
        );
    }

    #[test]
    fn flat_empty_index_round_trips() {
        // The 0-term edge: no spans, no term bytes, no postings. The v2
        // decoder's contiguity checks must accept the empty body.
        let flat = FlatIndex::from_rows(std::iter::empty::<&str>());
        assert_eq!(flat.n_rows(), 0);
        assert_eq!(flat.n_terms(), 0);
        let back = FlatIndex::from_bytes_v2(&flat.to_bytes_v2()).expect("round-trips");
        assert_eq!(back, flat);
        assert_eq!(
            InvertedIndex::from_flat(&back),
            Some(InvertedIndex::from_rows(std::iter::empty::<&str>()))
        );
    }

    /// A well-formed **v3** blob (heap-BST term order, matching the resident
    /// layout) from sorted `(term, deltas)` pairs, so a hostile case can break
    /// exactly one field. The body is: `n_rows`, `n_terms`, `total_postings`,
    /// the term lengths, the term bytes, then per term its posting count and
    /// deltas — all in sorted term order.
    fn v2_blob(n_rows: u64, terms: &[(&[u8], &[u64])]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(INDEX_MAGIC);
        out.push(INDEX_VERSION_2);
        write_varint(&mut out, n_rows);
        write_varint(&mut out, terms.len() as u64);
        let total: u64 = terms.iter().map(|(_, d)| d.len() as u64).sum();
        write_varint(&mut out, total);
        for (term, _) in terms {
            write_varint(&mut out, term.len() as u64);
        }
        for (term, _) in terms {
            out.extend_from_slice(term);
        }
        for (_, deltas) in terms {
            write_varint(&mut out, deltas.len() as u64);
            for &d in *deltas {
                write_varint(&mut out, d);
            }
        }
        out
    }

    #[test]
    fn flat_v2_refuses_hostile_blobs() {
        // Bad magic / version.
        assert!(FlatIndex::from_bytes_v2(b"").is_none());
        assert!(FlatIndex::from_bytes_v2(b"XXXX\x02").is_none(), "bad magic");
        let mut bad_version = v2_blob(3, &[(b"ab", &[0])]);
        bad_version[4] = 99;
        assert!(
            FlatIndex::from_bytes_v2(&bad_version).is_none(),
            "bad version"
        );

        // A term count no payload can cover.
        let mut out = Vec::new();
        out.extend_from_slice(INDEX_MAGIC);
        out.push(INDEX_VERSION_2);
        write_varint(&mut out, 3);
        write_varint(&mut out, u32::MAX as u64);
        assert!(out.len() < 16, "the hostile blob stays tiny");
        assert!(FlatIndex::from_bytes_v2(&out).is_none());

        // A term length that runs past the term-byte region.
        let mut out = Vec::new();
        out.extend_from_slice(INDEX_MAGIC);
        out.push(INDEX_VERSION_2);
        write_varint(&mut out, 3);
        write_varint(&mut out, 1);
        write_varint(&mut out, 1); // total_postings
        write_varint(&mut out, 200); // term_len past the two bytes that follow
        out.extend_from_slice(b"ab");
        write_varint(&mut out, 1);
        out.push(0);
        assert!(FlatIndex::from_bytes_v2(&out).is_none());

        // A zero-length term.
        assert!(FlatIndex::from_bytes_v2(&v2_blob(3, &[(b"", &[0])])).is_none());

        // An empty postings list: a term is stored only when a row has it.
        assert!(FlatIndex::from_bytes_v2(&v2_blob(3, &[(b"ab", &[])])).is_none());

        // A posting count that overruns the remaining deltas.
        let mut out = Vec::new();
        out.extend_from_slice(INDEX_MAGIC);
        out.push(INDEX_VERSION_2);
        write_varint(&mut out, 3);
        write_varint(&mut out, 1);
        write_varint(&mut out, 5); // total_postings
        write_varint(&mut out, 2); // "ab"
        out.extend_from_slice(b"ab");
        write_varint(&mut out, 5); // claims five postings
        out.push(0); // but only one delta
        assert!(FlatIndex::from_bytes_v2(&out).is_none());

        // A posting that walks past the row domain.
        assert!(FlatIndex::from_bytes_v2(&v2_blob(3, &[(b"ab", &[3])])).is_none());
        // Two postings, the second out of domain (n_rows = 3, so 1 then 3).
        assert!(FlatIndex::from_bytes_v2(&v2_blob(3, &[(b"ab", &[1, 2])])).is_none());

        // A zero delta after the first repeats the previous ordinal.
        assert!(FlatIndex::from_bytes_v2(&v2_blob(9, &[(b"ab", &[2, 0])])).is_none());

        // A delta that fits u32 but walks the running ordinal past u32.
        assert!(FlatIndex::from_bytes_v2(&v2_blob(
            u32::MAX as u64,
            &[(b"ab", &[u32::MAX as u64, u32::MAX as u64])]
        ))
        .is_none());

        // A delta past u32.
        assert!(FlatIndex::from_bytes_v2(&v2_blob(100, &[(b"ab", &[(1u64 << 32) + 5])])).is_none());

        // Duplicate terms (the v2 sorted form must be strictly ascending in
        // buffer order).
        assert!(FlatIndex::from_bytes_v2(&v2_blob(3, &[(b"ab", &[0]), (b"ab", &[1])])).is_none());
        // Descending terms.
        assert!(FlatIndex::from_bytes_v2(&v2_blob(3, &[(b"cd", &[0]), (b"ab", &[1])])).is_none());

        // Non-UTF-8 term bytes.
        assert!(FlatIndex::from_bytes_v2(&v2_blob(3, &[(b"\xff\xfe", &[0])])).is_none());

        // Trailing payload: the body must be consumed exactly.
        let mut good = v2_blob(3, &[(b"ab", &[0])]);
        good.push(0);
        assert!(FlatIndex::from_bytes_v2(&good).is_none());

        // The exact-fit boundary is accepted: an in-domain, ascending index.
        assert_eq!(
            FlatIndex::from_bytes_v2(&v2_blob(3, &[(b"abc", &[0, 2])]))
                .and_then(|i| i.postings("abc").map(<[u32]>::to_vec)),
            Some(vec![0, 2])
        );
    }

    /// Every one-byte truncation and every single-byte mutation of a valid v2
    /// blob must come back as `None` or as a decoded index — never a panic.
    #[test]
    fn flat_v2_survives_truncation_and_mutation() {
        let good = v2_blob(3, &[(b"ab", &[0, 2]), (b"cd", &[1])]);
        for cut in 0..good.len() {
            assert!(
                FlatIndex::from_bytes_v2(&good[..cut]).is_none(),
                "a truncated blob is never valid (cut at {cut})"
            );
        }
        for pos in 0..good.len() {
            for replacement in [0x00u8, 0x01, 0x7f, 0x80, 0xff] {
                let mut mutated = good.clone();
                mutated[pos] = replacement;
                if let Some(index) = FlatIndex::from_bytes_v2(&mutated) {
                    // Whatever survives is internally consistent: every posting
                    // inside the row domain it declares.
                    for (term, rows) in index.terms() {
                        assert!(
                            rows.iter().all(|row| *row < index.n_rows()),
                            "term {term:?} escaped the row domain after byte {pos} := {replacement:#04x}"
                        );
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
