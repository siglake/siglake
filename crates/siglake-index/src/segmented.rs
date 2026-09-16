//! **Experimental** segmented sidecar layout (#4376, 0.2.0 prototype).
//!
//! The shipped [`InvertedIndex`](crate::InvertedIndex) is one blob per file that
//! a reader can only use whole: `from_bytes` materializes the file's entire term
//! dictionary and every posting list before the first lookup. On the compacted
//! 50G layout that is ~74 bytes of resident dictionary per indexed row — a
//! 7.34M-row file parses to ~545 MB, and a 14-file text plan wants 7.83 GB
//! against a 1 GiB cache (`docs/DESIGN_inverted_index.md`).
//!
//! This layout addresses the same postings **per row group**, through byte
//! ranges, so a reader holds only a directory and decodes only the sections a
//! query names:
//!
//! ```text
//! [magic "KIDS"][version u8]              header, for offline sniffing only
//! group 0: postings section               per-term delta-varint lists, group-relative
//! group 0: dictionary blocks              sorted, prefix-compressed, ~4 KiB each
//! group 1: ...
//! directory                               one range read: groups, per-block first terms + CRCs
//! trailer                                 fixed 25 bytes at EOF, with the directory's CRC
//! ```
//!
//! What each property buys:
//!
//! - **Row-group addressing.** Group `i` of the sidecar is Parquet row group `i`;
//!   the directory carries every group's `(first_row, n_rows)`, so a reader can
//!   restrict a lookup to the row groups the scan kept
//!   ([`SegmentedReader::matching_rows_all_in_groups`]) and can verify the
//!   sidecar's row domain against the Parquet metadata *per group*
//!   ([`SegmentedReader::matches_row_groups`]) rather than against one stamped
//!   row-group size.
//! - **Ordinals.** Postings are stored **group-relative** (so deltas stay small
//!   and a section is decodable on its own) and returned **file-physical**, the
//!   ordinal space [`InvertedIndex`](crate::InvertedIndex) already returns and the
//!   `RowSelection` bridge already consumes. Nothing downstream re-maps.
//! - **Everything at the end.** The directory and trailer are written last and
//!   nothing before them is patched, so a merge that emits one row group at a
//!   time can write this format in one forward pass (#4377) and never holds more
//!   than one group's postings.
//! - **A corrupt index says so instead of dropping rows.** A term that is
//!   absent and a structure that cannot be parsed are different answers here
//!   ([`Lookup::Absent`] vs [`Lookup::Unanswerable`]), where the v1 decoder
//!   conflates them into `None`. The AND/substring entry points return `None`
//!   for "cannot answer — scan", and `Some(vec![])` only for a definitive
//!   no-match. Partial reads make this load-bearing in a way v1 never faced:
//!   a flipped byte in a dictionary block would otherwise make a term look
//!   *absent* in one row group and silently drop its rows, so the directory
//!   carries a CRC per block and its own CRC sits in the trailer. Posting
//!   sections are covered by their stated document frequency only; the residual
//!   that leaves is stated in `docs/DESIGN_segmented_inverted_index.md`.
//!
//! The format is deliberately **not** wired into the writer, the reader or any
//! default: it carries its own magic, its own footer-KV key
//! ([`SEGMENTED_INDEX_KV_KEY`]) and its own Puffin blob type
//! ([`SEGMENTED_BLOB_TYPE`]), so a 0.1.x reader looking for
//! [`INVERTED_INDEX_KV_KEY`](crate::INVERTED_INDEX_KV_KEY) /
//! `siglake-inverted-v1` does not see a segmented sidecar at all and scans
//! exactly as it does for an unindexed file. v1 blobs stay readable by v1 code,
//! unchanged.
//!
//! Sections are stored **uncompressed**: the shipped v1 sidecar is one
//! Zstd-compressed Puffin blob, which is why it can only be read whole. A
//! per-section codec byte is a later version's concern; the byte ranges this
//! directory addresses do not change shape when one arrives.
//!
//! This module carries its own varint reader rather than sharing the v1
//! decoder's, so #4558's hardening of that decoder and this prototype do not
//! edit the same code.

use std::borrow::Cow;

use siglake_bloom::{normalize_query_term, Tokenizer};

use crate::{row_selection_runs, InvertedIndex};

/// Magic at both ends of a segmented blob (v1 uses `KIDX`).
pub const SEGMENTED_MAGIC: &[u8; 4] = b"KIDS";
/// Version of the segmented layout. Bumped when the byte layout changes; a
/// reader refuses any other value, which is the unknown-version fallback.
pub const SEGMENTED_VERSION: u8 = 1;
/// `dir_offset: u64 | dir_len: u64 | dir_crc: u32 | version: u8 | magic: [u8; 4]`.
pub const SEGMENTED_TRAILER_LEN: usize = 8 + 8 + 4 + 1 + 4;
/// Puffin blob type a segmented sidecar would be registered under. Distinct
/// from `siglake-inverted-v1`, so an existing reader skips it.
pub const SEGMENTED_BLOB_TYPE: &str = "siglake-inverted-seg-v1";
/// Value for the sidecar's `format` property, beside the `v1` the shipped
/// writer stamps.
pub const SEGMENTED_FORMAT_PROPERTY: &str = "seg1";
/// Footer-KV key a segmented blob would use. Distinct from
/// [`INVERTED_INDEX_KV_KEY`](crate::INVERTED_INDEX_KV_KEY) for the same reason.
pub const SEGMENTED_INDEX_KV_KEY: &str = "siglake.inverted_index.seg1";
/// Dictionary-block target size. A lookup reads one whole block, and the
/// directory holds one entry per block, so this trades the resident directory
/// against the bytes one point lookup fetches.
pub const DEFAULT_TARGET_BLOCK_BYTES: usize = 4096;

/// Footer-KV key for `column`'s segmented blob.
pub fn segmented_index_kv_key(column: &str) -> Cow<'static, str> {
    if column == "raw" {
        Cow::Borrowed(SEGMENTED_INDEX_KV_KEY)
    } else {
        Cow::Owned(format!("{SEGMENTED_INDEX_KV_KEY}.{column}"))
    }
}

/// Outcome of a single-term lookup. The three cases are distinct on purpose:
/// only `Unanswerable` may fall back to a scan, and only `Absent` licenses
/// skipping rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// Ascending **file-physical** row ordinals containing the term.
    Rows(Vec<u32>),
    /// The term is indexable and this index does not have it: no row matches.
    Absent,
    /// The term cannot be answered from this index — it does not normalize, or
    /// a section this lookup needed is malformed. The caller must scan.
    Unanswerable,
}

impl Lookup {
    /// The rows, or `None` when nothing can be concluded (`Unanswerable`).
    /// `Absent` becomes an empty list — a definitive no-match.
    pub fn rows(self) -> Option<Vec<u32>> {
        match self {
            Lookup::Rows(rows) => Some(rows),
            Lookup::Absent => Some(Vec::new()),
            Lookup::Unanswerable => None,
        }
    }
}

// ---------------------------------------------------------------------------
// writing
// ---------------------------------------------------------------------------

/// One dictionary block, as the directory describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct BlockEntry {
    /// First term in the block; the search key that selects a block without
    /// reading any.
    first_term: Box<str>,
    /// Block payload, relative to the group's dictionary offset.
    offset: u32,
    len: u32,
    /// Byte offset of the block's first term's postings, relative to the
    /// group's postings offset. Lengths accumulate from here within the block,
    /// so one block read gives an exact byte range for any term it holds.
    postings_base: u64,
    /// CRC-32 of the block payload. A lookup reads the whole block, so
    /// verifying it costs no extra bytes — and without it a flipped term byte
    /// reads as "this group does not have the term".
    crc: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GroupEntry {
    first_row: u32,
    n_rows: u32,
    dict_offset: u64,
    dict_len: u64,
    postings_offset: u64,
    postings_len: u64,
    blocks: Vec<BlockEntry>,
}

/// Builds a segmented blob one row group at a time. Peak memory is one group's
/// postings plus its dictionary entries — never the file's.
pub struct SegmentedWriter {
    out: Vec<u8>,
    groups: Vec<GroupEntry>,
    next_row: u32,
    target_block_bytes: usize,
    tokenizer: Tokenizer,
}

impl Default for SegmentedWriter {
    fn default() -> Self {
        Self::new(DEFAULT_TARGET_BLOCK_BYTES)
    }
}

impl SegmentedWriter {
    pub fn new(target_block_bytes: usize) -> Self {
        let mut out = Vec::new();
        out.extend_from_slice(SEGMENTED_MAGIC);
        out.push(SEGMENTED_VERSION);
        Self {
            out,
            groups: Vec::new(),
            next_row: 0,
            target_block_bytes: target_block_bytes.max(64),
            tokenizer: Tokenizer::Default,
        }
    }

    pub fn with_tokenizer(mut self, tokenizer: Tokenizer) -> Self {
        self.tokenizer = tokenizer;
        self
    }

    /// Append one row group's rows, in the group's physical row order.
    pub fn push_group_rows<'a, I: IntoIterator<Item = &'a str>>(&mut self, rows: I) {
        let group = InvertedIndex::from_rows_with_tokenizer(rows, self.tokenizer);
        self.push_group_index(&group);
    }

    /// Append one row group from an index built over exactly that group's rows
    /// (its ordinals are group-relative, `0..group.n_rows()`).
    pub fn push_group_index(&mut self, group: &InvertedIndex) {
        let postings_offset = self.out.len() as u64;
        // Postings first: a term's byte length is only known once written, and
        // the dictionary stores lengths.
        let mut entries: Vec<(&str, u32, u32)> = Vec::with_capacity(group.terms().len());
        for (term, rows) in group.terms() {
            let start = self.out.len();
            let mut prev = 0u32;
            for &row in rows {
                write_varint(&mut self.out, u64::from(row - prev));
                prev = row;
            }
            let len = (self.out.len() - start) as u32;
            entries.push((term, rows.len() as u32, len));
        }
        let postings_len = self.out.len() as u64 - postings_offset;

        let dict_offset = self.out.len() as u64;
        let mut blocks: Vec<BlockEntry> = Vec::new();
        let mut block = Vec::new();
        let mut block_terms: Vec<(&str, u32, u32)> = Vec::new();
        let mut postings_cursor = 0u64; // within the group's postings section
        let mut block_postings_base = 0u64;
        for entry in entries {
            block_terms.push(entry);
            postings_cursor += u64::from(entry.2);
            // Cheap running estimate of the payload this block will encode to;
            // the exact size comes out of `encode_block`.
            let estimate: usize = block_terms
                .iter()
                .map(|(term, _, _)| term.len() + 6)
                .sum::<usize>()
                + 2;
            if estimate >= self.target_block_bytes {
                encode_block(&mut block, &block_terms);
                blocks.push(self.flush_block(&block, dict_offset, block_postings_base));
                block_terms.clear();
                block_postings_base = postings_cursor;
            }
        }
        if !block_terms.is_empty() {
            encode_block(&mut block, &block_terms);
            blocks.push(self.flush_block(&block, dict_offset, block_postings_base));
        }
        let dict_len = self.out.len() as u64 - dict_offset;

        self.groups.push(GroupEntry {
            first_row: self.next_row,
            n_rows: group.n_rows(),
            dict_offset,
            dict_len,
            postings_offset,
            postings_len,
            blocks,
        });
        self.next_row += group.n_rows();
    }

    fn flush_block(&mut self, block: &[u8], dict_offset: u64, postings_base: u64) -> BlockEntry {
        let offset = (self.out.len() as u64 - dict_offset) as u32;
        self.out.extend_from_slice(block);
        BlockEntry {
            // `encode_block` writes the first term in full, so the block's own
            // bytes carry it; the directory copy is the search key.
            first_term: first_term_of(block).into(),
            offset,
            len: block.len() as u32,
            postings_base,
            crc: crc32(block),
        }
    }

    /// Row count across the groups pushed so far.
    pub fn n_rows(&self) -> u32 {
        self.next_row
    }

    /// Finish: append the directory and the fixed trailer.
    pub fn finish(mut self) -> Vec<u8> {
        let dir_offset = self.out.len() as u64;
        let mut dir = Vec::new();
        write_varint(&mut dir, u64::from(self.next_row));
        write_varint(&mut dir, self.groups.len() as u64);
        for group in &self.groups {
            write_varint(&mut dir, u64::from(group.first_row));
            write_varint(&mut dir, u64::from(group.n_rows));
            write_varint(&mut dir, group.dict_offset);
            write_varint(&mut dir, group.dict_len);
            write_varint(&mut dir, group.postings_offset);
            write_varint(&mut dir, group.postings_len);
            write_varint(&mut dir, group.blocks.len() as u64);
            for block in &group.blocks {
                write_varint(&mut dir, block.first_term.len() as u64);
                dir.extend_from_slice(block.first_term.as_bytes());
                write_varint(&mut dir, u64::from(block.offset));
                write_varint(&mut dir, u64::from(block.len));
                write_varint(&mut dir, block.postings_base);
                dir.extend_from_slice(&block.crc.to_le_bytes());
            }
        }
        let dir_len = dir.len() as u64;
        let dir_crc = crc32(&dir);
        self.out.extend_from_slice(&dir);
        self.out.extend_from_slice(&dir_offset.to_le_bytes());
        self.out.extend_from_slice(&dir_len.to_le_bytes());
        self.out.extend_from_slice(&dir_crc.to_le_bytes());
        self.out.push(SEGMENTED_VERSION);
        self.out.extend_from_slice(SEGMENTED_MAGIC);
        self.out
    }
}

/// Encode one dictionary block: `n_terms`, then per term the shared prefix with
/// its predecessor, the suffix, its document frequency and its postings byte
/// length.
fn encode_block(out: &mut Vec<u8>, terms: &[(&str, u32, u32)]) {
    out.clear();
    write_varint(out, terms.len() as u64);
    let mut prev: &str = "";
    for &(term, df, postings_len) in terms {
        let shared = shared_prefix_len(prev.as_bytes(), term.as_bytes());
        write_varint(out, shared as u64);
        write_varint(out, (term.len() - shared) as u64);
        out.extend_from_slice(&term.as_bytes()[shared..]);
        write_varint(out, u64::from(df));
        write_varint(out, u64::from(postings_len));
        prev = term;
    }
}

fn first_term_of(block: &[u8]) -> &str {
    let mut c = Reader::new(block);
    let _n_terms = c.varint().expect("block just encoded");
    let shared = c.varint().expect("block just encoded");
    debug_assert_eq!(shared, 0, "a block's first term is written in full");
    let len = c.varint().expect("block just encoded") as usize;
    std::str::from_utf8(c.take(len).expect("block just encoded")).expect("terms are UTF-8")
}

fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// Encode a whole file's index in one call, splitting it into groups of
/// `group_rows` rows. The equivalence fixtures use this; a production writer
/// would push the groups it actually emits.
pub fn encode_from_rows<'a, I: IntoIterator<Item = &'a str>>(rows: I, group_rows: u32) -> Vec<u8> {
    let group_rows = group_rows.max(1) as usize;
    let mut writer = SegmentedWriter::default();
    let mut buffer: Vec<&str> = Vec::with_capacity(group_rows);
    for row in rows {
        buffer.push(row);
        if buffer.len() == group_rows {
            writer.push_group_rows(buffer.iter().copied());
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        writer.push_group_rows(buffer.iter().copied());
    }
    writer.finish()
}

// ---------------------------------------------------------------------------
// reading
// ---------------------------------------------------------------------------

/// A byte-range source: the blob, however it is reached. The prototype's point
/// is that a reader never asks for the whole thing, so this is the interface
/// the accounting is taken from ([`SliceSource::reads`] /
/// [`SliceSource::bytes_read`]).
pub trait RangeSource {
    /// Total blob length.
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Exactly `len` bytes at `offset`, or `None` if that range is not wholly
    /// inside the blob (or the read failed).
    fn read(&self, offset: u64, len: usize) -> Option<Vec<u8>>;
}

/// An in-memory [`RangeSource`] that counts what a reader asked for. The bytes
/// are shared, so a measurement can put many readers on one blob without
/// copying it per reader — a segmented reader never keeps the blob, which is
/// the property the sharing keeps honest.
pub struct SliceSource {
    bytes: std::sync::Arc<[u8]>,
    reads: std::sync::atomic::AtomicU64,
    bytes_read: std::sync::atomic::AtomicU64,
}

impl SliceSource {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self::shared(bytes.into())
    }

    pub fn shared(bytes: std::sync::Arc<[u8]>) -> Self {
        Self {
            bytes,
            reads: std::sync::atomic::AtomicU64::new(0),
            bytes_read: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Range reads served since [`Self::reset_counters`].
    pub fn reads(&self) -> u64 {
        self.reads.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Bytes served since [`Self::reset_counters`] — the fetched-byte column of
    /// the accounting, and (in this prototype) also the decoded-byte column,
    /// since every fetched byte is parsed by the lookup that asked for it.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn reset_counters(&self) {
        self.reads.store(0, std::sync::atomic::Ordering::Relaxed);
        self.bytes_read
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn blob_len(&self) -> usize {
        self.bytes.len()
    }
}

impl RangeSource for SliceSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn read(&self, offset: u64, len: usize) -> Option<Vec<u8>> {
        let start = usize::try_from(offset).ok()?;
        let end = start.checked_add(len)?;
        let slice = self.bytes.get(start..end)?;
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_read
            .fetch_add(len as u64, std::sync::atomic::Ordering::Relaxed);
        Some(slice.to_vec())
    }
}

/// A segmented blob opened for lookups. Resident state is the directory only
/// ([`Self::resident_bytes`]); everything else is fetched per lookup.
pub struct SegmentedReader<S: RangeSource> {
    source: S,
    groups: Vec<GroupEntry>,
    n_rows: u32,
}

impl<S: RangeSource> SegmentedReader<S> {
    /// Read the trailer and the directory and validate both. `None` on a bad
    /// magic, an unknown version, an out-of-range offset, or a directory whose
    /// groups do not tile `0..n_rows` — every one of which leaves the caller on
    /// the scan path.
    pub fn open(source: S) -> Option<Self> {
        let total = source.len();
        if total < (SEGMENTED_TRAILER_LEN + 5) as u64 {
            return None;
        }
        let trailer = source.read(total - SEGMENTED_TRAILER_LEN as u64, SEGMENTED_TRAILER_LEN)?;
        if &trailer[SEGMENTED_TRAILER_LEN - 4..] != SEGMENTED_MAGIC {
            return None;
        }
        if trailer[SEGMENTED_TRAILER_LEN - 5] != SEGMENTED_VERSION {
            return None;
        }
        let dir_offset = u64::from_le_bytes(trailer[0..8].try_into().ok()?);
        let dir_len = u64::from_le_bytes(trailer[8..16].try_into().ok()?);
        let dir_crc = u32::from_le_bytes(trailer[16..20].try_into().ok()?);
        let dir_end = dir_offset.checked_add(dir_len)?;
        // The directory sits between the header and the trailer.
        if dir_offset < 5 || dir_end != total - SEGMENTED_TRAILER_LEN as u64 {
            return None;
        }
        let dir = source.read(dir_offset, usize::try_from(dir_len).ok()?)?;
        if crc32(&dir) != dir_crc {
            return None;
        }
        let mut c = Reader::new(&dir);
        let n_rows = u32::try_from(c.varint()?).ok()?;
        let n_groups = usize::try_from(c.varint()?).ok()?;
        // Each group costs at least 7 varint bytes, so a count larger than the
        // remaining directory is corrupt: never allocate from it.
        if n_groups > c.remaining() {
            return None;
        }
        let mut groups: Vec<GroupEntry> = Vec::with_capacity(n_groups);
        let mut expected_first_row = 0u32;
        for _ in 0..n_groups {
            let first_row = u32::try_from(c.varint()?).ok()?;
            let group_rows = u32::try_from(c.varint()?).ok()?;
            let dict_offset = c.varint()?;
            let group_dict_len = c.varint()?;
            let postings_offset = c.varint()?;
            let postings_len = c.varint()?;
            let n_blocks = usize::try_from(c.varint()?).ok()?;
            // Groups tile the file in order: this is the row-domain check that
            // makes a file-physical ordinal well defined.
            if first_row != expected_first_row {
                return None;
            }
            expected_first_row = expected_first_row.checked_add(group_rows)?;
            if !range_inside(dict_offset, group_dict_len, dir_offset)
                || !range_inside(postings_offset, postings_len, dir_offset)
            {
                return None;
            }
            if n_blocks > c.remaining() {
                return None;
            }
            let mut blocks: Vec<BlockEntry> = Vec::with_capacity(n_blocks);
            let mut previous_term: Option<Box<str>> = None;
            for _ in 0..n_blocks {
                let term_len = usize::try_from(c.varint()?).ok()?;
                let first_term: Box<str> = std::str::from_utf8(c.take(term_len)?).ok()?.into();
                let offset = u32::try_from(c.varint()?).ok()?;
                let len = u32::try_from(c.varint()?).ok()?;
                let postings_base = c.varint()?;
                let crc = u32::from_le_bytes(c.take(4)?.try_into().ok()?);
                // Blocks partition the group's dictionary in term order, and
                // their postings bases march forward inside its postings.
                match &previous_term {
                    Some(previous) if previous.as_ref() >= first_term.as_ref() => return None,
                    _ => {}
                }
                if u64::from(offset).checked_add(u64::from(len))? > group_dict_len
                    || postings_base > postings_len
                {
                    return None;
                }
                previous_term = Some(first_term.clone());
                blocks.push(BlockEntry {
                    first_term,
                    offset,
                    len,
                    postings_base,
                    crc,
                });
            }
            groups.push(GroupEntry {
                first_row,
                n_rows: group_rows,
                dict_offset,
                dict_len: group_dict_len,
                postings_offset,
                postings_len,
                blocks,
            });
        }
        if c.remaining() != 0 || expected_first_row != n_rows {
            return None;
        }
        Some(Self {
            source,
            groups,
            n_rows,
        })
    }

    pub fn source(&self) -> &S {
        &self.source
    }

    /// Rows the sidecar covers — the ordinal space of every posting it returns.
    pub fn n_rows(&self) -> u32 {
        self.n_rows
    }

    pub fn n_groups(&self) -> usize {
        self.groups.len()
    }

    /// Each group's row count, in file order.
    pub fn group_rows(&self) -> Vec<u32> {
        self.groups.iter().map(|group| group.n_rows).collect()
    }

    /// Whether this sidecar describes exactly the given Parquet row groups.
    /// The shipped v1 path can only compare one stamped `row_group_size`
    /// against the file's groups; a segmented directory states every group, so
    /// a sidecar written for a different layout is rejected before it prunes
    /// anything.
    pub fn matches_row_groups(&self, parquet_row_counts: &[u64]) -> bool {
        self.groups.len() == parquet_row_counts.len()
            && self
                .groups
                .iter()
                .zip(parquet_row_counts)
                .all(|(group, rows)| u64::from(group.n_rows) == *rows)
    }

    /// Bytes the reader holds between lookups: the parsed directory. This is
    /// the number the whole prototype is about — the v1 comparison is
    /// [`InvertedIndex::heap_size_bytes`].
    pub fn resident_bytes(&self) -> usize {
        const GROUP: usize = std::mem::size_of::<GroupEntry>();
        const BLOCK: usize = std::mem::size_of::<BlockEntry>();
        std::mem::size_of::<Self>()
            + self
                .groups
                .iter()
                .map(|group| {
                    GROUP
                        + group
                            .blocks
                            .iter()
                            .map(|block| BLOCK + block.first_term.len())
                            .sum::<usize>()
                })
                .sum::<usize>()
    }

    /// Ascending file-physical ordinals containing `term`, across every group.
    pub fn postings(&self, term: &str) -> Lookup {
        self.postings_in_groups(term, None)
    }

    /// [`Self::postings`] restricted to `groups` (indices into the file's row
    /// groups, ascending) — the reject path: a group the scan already pruned
    /// costs no read at all.
    pub fn postings_in_groups(&self, term: &str, groups: Option<&[usize]>) -> Lookup {
        let Some(normalized) = normalize_query_term(term) else {
            return Lookup::Unanswerable;
        };
        let mut rows: Vec<u32> = Vec::new();
        let mut found = false;
        for index in self.group_indices(groups) {
            let group = &self.groups[index];
            match self.group_postings(group, &normalized) {
                Ok(Some(group_rows)) => {
                    found = true;
                    rows.extend(group_rows);
                }
                Ok(None) => {}
                Err(()) => return Lookup::Unanswerable,
            }
        }
        if found {
            Lookup::Rows(rows)
        } else {
            Lookup::Absent
        }
    }

    /// Rows matching **all** `terms`. `None` means the index cannot answer and
    /// the caller must scan; `Some(vec![])` is a definitive no-match.
    pub fn matching_rows_all(&self, terms: &[&str]) -> Option<Vec<u32>> {
        self.matching_rows_all_in_groups(terms, None)
    }

    /// [`Self::matching_rows_all`] restricted to `groups`. Terms are resolved
    /// rarest-first *by the directory's document frequencies*: an absent term
    /// ends the lookup before any other term's postings are fetched.
    pub fn matching_rows_all_in_groups(
        &self,
        terms: &[&str],
        groups: Option<&[usize]>,
    ) -> Option<Vec<u32>> {
        if terms.is_empty() {
            return Some(Vec::new());
        }
        let mut lists: Vec<Vec<u32>> = Vec::with_capacity(terms.len());
        for term in terms {
            match self.postings_in_groups(term, groups) {
                Lookup::Rows(rows) => lists.push(rows),
                Lookup::Absent => return Some(Vec::new()),
                Lookup::Unanswerable => return None,
            }
        }
        lists.sort_by_key(Vec::len);
        let mut acc = lists.swap_remove(0);
        for list in &lists {
            acc = intersect_sorted(&acc, list);
            if acc.is_empty() {
                break;
            }
        }
        Some(acc)
    }

    /// Rows matching `raw LIKE '%substr%'` under the same argument v1's
    /// [`InvertedIndex::rows_containing`] makes, or `None` when the substring
    /// is not index-answerable.
    ///
    /// This is the one shape that costs the whole dictionary: finding every
    /// term that *contains* a substring means reading every block. It is bounded
    /// by the dictionary bytes rather than the postings, and the postings it
    /// then reads are only the matching terms' — but it is not a point lookup,
    /// and a caller that cares should treat [`Self::dictionary_bytes`] as its
    /// price.
    pub fn rows_containing(&self, substr: &str) -> Option<Vec<u32>> {
        self.rows_containing_in_groups(substr, None)
    }

    pub fn rows_containing_in_groups(
        &self,
        substr: &str,
        groups: Option<&[usize]>,
    ) -> Option<Vec<u32>> {
        let normalized = normalize_query_term(substr)?;
        let mut rows: Vec<u32> = Vec::new();
        for index in self.group_indices(groups) {
            let group = &self.groups[index];
            let mut matches: Vec<(u64, u32, u32)> = Vec::new();
            for block in &group.blocks {
                let bytes = self.read_block(group, block)?;
                let walked = scan_block(&bytes, block.postings_base, |term, df, offset, len| {
                    if std::str::from_utf8(term)
                        .map(|term| term.contains(&normalized))
                        .unwrap_or(false)
                    {
                        matches.push((offset, len, df));
                    }
                    true
                });
                if walked.is_err() {
                    return None;
                }
            }
            for (offset, len, df) in matches {
                let group_rows = self.read_postings(group, offset, len, df).ok()?;
                rows = union_sorted(&rows, &group_rows);
            }
        }
        Some(rows)
    }

    /// `(selected, length)` runs over the whole file, as
    /// [`InvertedIndex::matching_row_selection`] produces them. `None` when the
    /// index cannot answer.
    pub fn matching_row_selection(&self, terms: &[&str]) -> Option<Vec<(bool, u32)>> {
        let matching = self.matching_rows_all(terms)?;
        Some(row_selection_runs(&matching, self.n_rows))
    }

    /// Total dictionary bytes — what a substring sweep reads.
    pub fn dictionary_bytes(&self) -> u64 {
        self.groups.iter().map(|group| group.dict_len).sum()
    }

    /// Total postings bytes — what a v1 decode reads in full and a point lookup
    /// reads a slice of.
    pub fn postings_bytes(&self) -> u64 {
        self.groups.iter().map(|group| group.postings_len).sum()
    }

    fn group_indices(&self, groups: Option<&[usize]>) -> Vec<usize> {
        match groups {
            None => (0..self.groups.len()).collect(),
            Some(selected) => selected
                .iter()
                .copied()
                .filter(|index| *index < self.groups.len())
                .collect(),
        }
    }

    /// `Ok(None)` = this group does not have the term; `Err(())` = malformed.
    fn group_postings(&self, group: &GroupEntry, normalized: &str) -> Result<Option<Vec<u32>>, ()> {
        // Pick the one block whose term range can hold the term. No block read
        // at all when the term sorts before the group's first term.
        let block_index = group
            .blocks
            .partition_point(|block| block.first_term.as_ref() <= normalized);
        if block_index == 0 {
            return Ok(None);
        }
        let block = &group.blocks[block_index - 1];
        let bytes = self.read_block(group, block).ok_or(())?;
        let mut hit: Option<(u64, u32, u32)> = None;
        scan_block(&bytes, block.postings_base, |term, df, offset, len| {
            match term.cmp(normalized.as_bytes()) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => {
                    hit = Some((offset, len, df));
                    false
                }
                // Terms ascend, so the first term past the key ends the scan.
                std::cmp::Ordering::Greater => false,
            }
        })?;
        match hit {
            None => Ok(None),
            Some((offset, len, df)) => self.read_postings(group, offset, len, df).map(Some),
        }
    }

    /// Fetch and verify one dictionary block. `None` — not "the term is not
    /// here" — when the bytes do not match the CRC the directory recorded.
    fn read_block(&self, group: &GroupEntry, block: &BlockEntry) -> Option<Vec<u8>> {
        let bytes = self.source.read(
            group.dict_offset + u64::from(block.offset),
            block.len as usize,
        )?;
        if crc32(&bytes) != block.crc {
            return None;
        }
        Some(bytes)
    }

    /// Decode one term's postings into **file-physical** ordinals, rejecting a
    /// list that is not strictly ascending, that leaves the group's rows, or
    /// that does not hold exactly the `df` the dictionary stated. The count
    /// cross-check is what keeps a corrupt section from looking like a shorter
    /// valid one — a section whose *ordinals* are corrupt but whose count still
    /// matches is undetectable here, exactly as in the v1 format (see the
    /// integrity note in `docs/DESIGN_segmented_inverted_index.md`).
    fn read_postings(
        &self,
        group: &GroupEntry,
        offset: u64,
        len: u32,
        df: u32,
    ) -> Result<Vec<u32>, ()> {
        if offset + u64::from(len) > group.postings_len {
            return Err(());
        }
        let bytes = self
            .source
            .read(group.postings_offset + offset, len as usize)
            .ok_or(())?;
        let mut c = Reader::new(&bytes);
        let mut rows = Vec::new();
        let mut previous = 0u32;
        let mut first = true;
        while c.remaining() > 0 {
            let delta = u32::try_from(c.varint().ok_or(())?).map_err(|_| ())?;
            if !first && delta == 0 {
                return Err(()); // repeated ordinal
            }
            previous = previous.checked_add(delta).ok_or(())?;
            if previous >= group.n_rows {
                return Err(()); // outside the group's row domain
            }
            rows.push(previous + group.first_row);
            first = false;
        }
        if rows.is_empty() || rows.len() != df as usize {
            return Err(());
        }
        Ok(rows)
    }
}

/// Walk one dictionary block, handing each term's bytes, document frequency and
/// postings range (relative to the group's postings section) to `visit` until
/// it returns `false`. `Err(())` on a malformed block — a short payload,
/// non-ascending terms, a zero document frequency, or a postings length that
/// cannot hold the stated count.
fn scan_block(
    bytes: &[u8],
    postings_base: u64,
    mut visit: impl FnMut(&[u8], u32, u64, u32) -> bool,
) -> Result<(), ()> {
    let mut c = Reader::new(bytes);
    let n_terms = usize::try_from(c.varint().ok_or(())?).map_err(|_| ())?;
    if n_terms > c.remaining() {
        return Err(()); // each term costs ≥ 1 byte; never allocate from this
    }
    let mut term: Vec<u8> = Vec::new();
    let mut previous: Vec<u8> = Vec::new();
    let mut cursor = postings_base;
    for index in 0..n_terms {
        let shared = usize::try_from(c.varint().ok_or(())?).map_err(|_| ())?;
        let suffix_len = usize::try_from(c.varint().ok_or(())?).map_err(|_| ())?;
        if shared > previous.len() || (index == 0 && shared != 0) {
            return Err(());
        }
        term.clear();
        term.extend_from_slice(&previous[..shared]);
        term.extend_from_slice(c.take(suffix_len).ok_or(())?);
        let df = u32::try_from(c.varint().ok_or(())?).map_err(|_| ())?;
        let len = u32::try_from(c.varint().ok_or(())?).map_err(|_| ())?;
        if df == 0 || len < df {
            return Err(()); // every posting is ≥ 1 byte, and a stored term has rows
        }
        if index > 0 && term.as_slice() <= previous.as_slice() {
            return Err(());
        }
        if !visit(&term, df, cursor, len) {
            return Ok(());
        }
        cursor = cursor.checked_add(u64::from(len)).ok_or(())?;
        previous.clear();
        previous.extend_from_slice(&term);
    }
    if c.remaining() != 0 {
        return Err(());
    }
    Ok(())
}

fn range_inside(offset: u64, len: u64, limit: u64) -> bool {
    match offset.checked_add(len) {
        Some(end) => offset >= 5 && end <= limit,
        None => false,
    }
}

/// CRC-32 (IEEE 802.3), bytewise. Hand-rolled to keep this crate's dependency
/// list at one entry for the prototype; a production version should take
/// `crc32fast`, which is already in the lockfile and is SIMD-accelerated.
fn crc32(bytes: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut index = 0usize;
        while index < 256 {
            let mut value = index as u32;
            let mut bit = 0;
            while bit < 8 {
                value = if value & 1 == 1 {
                    0xedb8_8320 ^ (value >> 1)
                } else {
                    value >> 1
                };
                bit += 1;
            }
            table[index] = value;
            index += 1;
        }
        table
    };
    let mut crc = 0xffff_ffffu32;
    for &byte in bytes {
        crc = TABLE[((crc ^ u32::from(byte)) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

/// Forward byte reader. Unlike the v1 decoder's cursor it rejects an overlong
/// varint *and* payload bits that would not survive the shift, so a corrupt
/// length cannot be read as a small number (the hazard #4558 fixes on the v1
/// side).
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }

    fn varint(&mut self) -> Option<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = *self.take(1)?.first()?;
            let payload = u64::from(byte & 0x7f);
            if shift >= 64 || (payload << shift) >> shift != payload {
                return None;
            }
            result |= payload << shift;
            if byte & 0x80 == 0 {
                if shift > 0 && payload == 0 {
                    return None; // non-canonical: a trailing zero group
                }
                return Some(result);
            }
            shift += 7;
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    /// A corpus with the shape the measurement uses: a few shared tokens, one
    /// token unique to each row (the cardinality that makes a parsed v1
    /// dictionary proportional to the file's rows), a 2%-density term and a
    /// sparse one.
    fn corpus(rows: usize) -> Vec<String> {
        (0..rows)
            .map(|row| {
                let queen = if row.is_multiple_of(50) { " queen" } else { "" };
                let checkout = if row.is_multiple_of(20) {
                    " checkout"
                } else {
                    ""
                };
                let rare = if row.is_multiple_of(997) {
                    " rareneedle"
                } else {
                    ""
                };
                format!(
                    "service-{} status {}{queen}{checkout}{rare} row-{row:06}",
                    row % 20,
                    200 + row % 5
                )
            })
            .collect()
    }

    fn open(bytes: Vec<u8>) -> SegmentedReader<SliceSource> {
        SegmentedReader::open(SliceSource::new(bytes)).expect("well-formed blob opens")
    }

    fn encoded(rows: &[String], group_rows: u32) -> Vec<u8> {
        encode_from_rows(rows.iter().map(String::as_str), group_rows)
    }

    #[test]
    fn a_segmented_blob_answers_exactly_what_the_v1_index_answers() {
        let rows = corpus(2_500);
        let v1 = InvertedIndex::from_rows(rows.iter().map(String::as_str));
        let reader = open(encoded(&rows, 400));
        assert_eq!(reader.n_rows(), v1.n_rows());
        assert_eq!(reader.n_groups(), 7, "2500 rows in groups of 400");

        // Every term the corpus has, at every density, plus terms it does not.
        for term in [
            "rareneedle",
            "queen",
            "checkout",
            "service",
            "status",
            "200",
            "204",
            "row",
            "000000",
            "001249",
            "002499",
            "absentterm",
            "ROWS",
            "qu",
        ] {
            let expected = v1.postings(term);
            let actual = reader.postings(term);
            match expected {
                Some(rows) => assert_eq!(actual, Lookup::Rows(rows.to_vec()), "term {term}"),
                None => assert!(
                    matches!(actual, Lookup::Absent | Lookup::Unanswerable),
                    "term {term}: {actual:?}"
                ),
            }
        }

        // And every term in the dictionary, exhaustively.
        for (term, expected) in v1.terms() {
            assert_eq!(
                reader.postings(term),
                Lookup::Rows(expected.to_vec()),
                "term {term}"
            );
        }
    }

    #[test]
    fn and_substring_and_selection_agree_with_v1() {
        let rows = corpus(1_100);
        let v1 = InvertedIndex::from_rows(rows.iter().map(String::as_str));
        let reader = open(encoded(&rows, 256));

        for terms in [
            vec!["queen", "checkout"],
            vec!["rareneedle", "status"],
            vec!["queen", "absentterm"],
            vec!["service", "status", "row"],
            vec!["000000", "queen"],
        ] {
            assert_eq!(
                reader.matching_rows_all(&terms),
                Some(v1.matching_rows_all(&terms)),
                "AND {terms:?}"
            );
            assert_eq!(
                reader.matching_row_selection(&terms),
                Some(v1.matching_row_selection(&terms)),
                "selection {terms:?}"
            );
        }

        for substring in [
            "rareneedle",
            "needle",
            "ervic",
            "ueen",
            "chec",
            "zzz",
            "0000",
        ] {
            assert_eq!(
                reader.rows_containing(substring),
                v1.rows_containing(substring),
                "substring {substring}"
            );
        }
        // Not answerable either way: a delimiter-bearing / too-short substring.
        assert_eq!(reader.rows_containing("row-0"), None);
        assert_eq!(v1.rows_containing("row-0"), None);
        assert_eq!(reader.postings("ro"), Lookup::Unanswerable);
    }

    #[test]
    fn a_term_that_straddles_row_groups_returns_file_physical_ordinals() {
        // `queen` is on every fiftieth row, so it appears in every group; the
        // ordinals must be the file's, not each group's.
        let rows = corpus(1_000);
        let v1 = InvertedIndex::from_rows(rows.iter().map(String::as_str));
        let reader = open(encoded(&rows, 137)); // deliberately not a divisor
        assert_eq!(
            reader.group_rows(),
            vec![137, 137, 137, 137, 137, 137, 137, 41]
        );
        assert_eq!(
            reader.postings("queen"),
            Lookup::Rows(v1.postings("queen").unwrap().to_vec())
        );
        // The last group's rows are reachable: the final row's unique token.
        assert_eq!(reader.postings("000999"), Lookup::Rows(vec![999]));
    }

    #[test]
    fn a_group_the_caller_rejected_costs_no_read() {
        let rows = corpus(1_000);
        let reader = open(encoded(&rows, 100));
        assert_eq!(reader.n_groups(), 10);

        reader.source().reset_counters();
        let all = reader.postings("queen");
        let whole_file_reads = reader.source().reads();
        assert!(
            whole_file_reads >= 20,
            "a block and a postings read per group"
        );

        reader.source().reset_counters();
        let restricted = reader.postings_in_groups("queen", Some(&[3, 4]));
        assert!(
            reader.source().reads() * 4 < whole_file_reads,
            "two of ten groups must not read like ten: {} vs {whole_file_reads}",
            reader.source().reads()
        );
        let Lookup::Rows(restricted) = restricted else {
            panic!("expected rows");
        };
        let Lookup::Rows(all) = all else {
            panic!("expected rows");
        };
        assert_eq!(
            restricted,
            all.iter()
                .copied()
                .filter(|row| (300..500).contains(row))
                .collect::<Vec<_>>(),
            "restriction is exactly the rows of those groups"
        );
    }

    #[test]
    fn a_point_lookup_reads_a_sliver_of_the_blob() {
        let rows = corpus(20_000);
        let blob = encoded(&rows, 5_000);
        let blob_len = blob.len() as u64;
        let reader = open(blob);
        reader.source().reset_counters();
        let Lookup::Rows(hits) = reader.postings("rareneedle") else {
            panic!("the sparse term is present");
        };
        assert_eq!(hits.len(), 21, "one row in 997");
        let fetched = reader.source().bytes_read();
        assert!(
            fetched * 20 < blob_len,
            "a point lookup fetched {fetched} of {blob_len} bytes"
        );
        // Resident state is the directory, not the dictionary.
        assert!(
            (reader.resident_bytes() as u64) * 10 < blob_len,
            "resident {} against a {blob_len}-byte blob",
            reader.resident_bytes()
        );
    }

    #[test]
    fn an_absent_term_is_a_definitive_no_match_and_a_malformed_one_is_not() {
        let rows = corpus(300);
        let reader = open(encoded(&rows, 100));
        assert_eq!(reader.postings("absentterm"), Lookup::Absent);
        assert_eq!(reader.matching_rows_all(&["absentterm"]), Some(Vec::new()));
        // An unanswerable term must not be read as "no rows match".
        assert_eq!(reader.matching_rows_all(&["ab"]), None);
        assert_eq!(reader.matching_rows_all(&[]), Some(Vec::new()));
    }

    #[test]
    fn an_empty_group_and_an_all_empty_file_round_trip() {
        let mut writer = SegmentedWriter::default();
        writer.push_group_rows(["alpha beta"]);
        writer.push_group_rows(Vec::<&str>::new()); // a group with no rows
        writer.push_group_rows(["", ""]); // rows with no indexable token
        writer.push_group_rows(["beta gamma"]);
        let reader = open(writer.finish());
        assert_eq!(reader.n_rows(), 4);
        assert_eq!(reader.group_rows(), vec![1, 0, 2, 1]);
        assert_eq!(reader.postings("alpha"), Lookup::Rows(vec![0]));
        assert_eq!(reader.postings("beta"), Lookup::Rows(vec![0, 3]));
        assert_eq!(reader.postings("gamma"), Lookup::Rows(vec![3]));
        assert_eq!(reader.postings("delta"), Lookup::Absent);

        let empty = open(SegmentedWriter::default().finish());
        assert_eq!(empty.n_rows(), 0);
        assert_eq!(empty.n_groups(), 0);
        assert_eq!(empty.postings("alpha"), Lookup::Absent);
        assert_eq!(empty.matching_row_selection(&["alpha"]), Some(Vec::new()));
    }

    #[test]
    fn a_repeated_term_in_one_row_is_one_posting() {
        let mut writer = SegmentedWriter::default();
        writer.push_group_rows(["error error error", "calm"]);
        let reader = open(writer.finish());
        assert_eq!(reader.postings("error"), Lookup::Rows(vec![0]));
    }

    #[test]
    fn the_directory_states_every_row_group_not_one_stamped_size() {
        let rows = corpus(1_000);
        let reader = open(encoded(&rows, 400));
        assert!(reader.matches_row_groups(&[400, 400, 200]));
        assert!(!reader.matches_row_groups(&[400, 400, 199]));
        assert!(!reader.matches_row_groups(&[400, 400]));
        assert!(!reader.matches_row_groups(&[1_000]));
    }

    #[test]
    fn a_v1_blob_and_a_segmented_blob_are_not_confusable() {
        let rows = corpus(200);
        let v1 = InvertedIndex::from_rows(rows.iter().map(String::as_str)).to_bytes();
        let seg = encoded(&rows, 64);
        // Each decoder refuses the other's bytes, so a reader that only knows
        // one format falls back to scanning rather than misreading.
        assert!(SegmentedReader::open(SliceSource::new(v1.clone())).is_none());
        assert!(InvertedIndex::from_bytes(&seg).is_none());
        assert!(InvertedIndex::from_bytes(&v1).is_some());
        assert!(SegmentedReader::open(SliceSource::new(seg)).is_some());
    }

    #[test]
    fn an_unknown_version_is_refused() {
        let rows = corpus(200);
        let mut blob = encoded(&rows, 64);
        let version = blob.len() - 5;
        blob[version] = SEGMENTED_VERSION + 1;
        assert!(SegmentedReader::open(SliceSource::new(blob)).is_none());
    }

    #[test]
    fn a_truncated_or_corrupt_blob_opens_to_nothing_and_allocates_nothing_large() {
        let rows = corpus(1_000);
        let blob = encoded(&rows, 100);

        // Every prefix of the blob: the trailer is gone or the directory is cut.
        for cut in [0, 1, 5, 21, 26, 100, blob.len() / 2, blob.len() - 1] {
            assert!(
                SegmentedReader::open(SliceSource::new(blob[..cut].to_vec())).is_none(),
                "prefix of {cut} bytes"
            );
        }
        // A directory offset pointing past the blob, and one pointing into it
        // but not ending where the trailer starts.
        for offset in [u64::MAX, u64::MAX - 8, blob.len() as u64, 0, 5, 6] {
            let mut corrupt = blob.clone();
            let at = corrupt.len() - SEGMENTED_TRAILER_LEN;
            corrupt[at..at + 8].copy_from_slice(&offset.to_le_bytes());
            assert!(
                SegmentedReader::open(SliceSource::new(corrupt)).is_none(),
                "directory offset {offset}"
            );
        }
        // An enormous directory length: the reader must not try to read it.
        for len in [u64::MAX, u64::MAX / 2, 1 << 40] {
            let mut corrupt = blob.clone();
            let at = corrupt.len() - SEGMENTED_TRAILER_LEN + 8;
            corrupt[at..at + 8].copy_from_slice(&len.to_le_bytes());
            assert!(
                SegmentedReader::open(SliceSource::new(corrupt)).is_none(),
                "directory length {len}"
            );
        }
        // A garbled directory body: a huge group count, then random bytes.
        let dir_offset = u64::from_le_bytes(
            blob[blob.len() - SEGMENTED_TRAILER_LEN..blob.len() - SEGMENTED_TRAILER_LEN + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        for (index, byte) in [
            (dir_offset, 0xffu8),
            (dir_offset + 1, 0xff),
            (dir_offset + 2, 0x7f),
        ] {
            let mut corrupt = blob.clone();
            corrupt[index] = byte;
            // Either refused at open, or the directory still parses and the
            // lookups below stay inside the blob — never a panic or an OOM.
            if let Some(reader) = SegmentedReader::open(SliceSource::new(corrupt)) {
                let _ = reader.postings("queen");
                let _ = reader.rows_containing("ueen");
            }
        }
    }

    #[test]
    fn a_corrupt_section_makes_the_lookup_unanswerable_not_empty() {
        let rows = corpus(1_000);
        let blob = encoded(&rows, 250);
        let reader = open(blob.clone());
        let Lookup::Rows(truth) = reader.postings("queen") else {
            panic!("present");
        };

        // Flip bytes across the sections and check no corruption turns a
        // present term into a silent "no rows": every answer is either the
        // truth or `Unanswerable`.
        let mut unanswerable = 0usize;
        for index in (5..blob.len() - SEGMENTED_TRAILER_LEN).step_by(97) {
            let mut corrupt = blob.clone();
            corrupt[index] ^= 0xff;
            let Some(reader) = SegmentedReader::open(SliceSource::new(corrupt)) else {
                continue;
            };
            match reader.postings("queen") {
                Lookup::Rows(rows) => {
                    // A corrupt *other* term's bytes can leave `queen` intact;
                    // what must never happen is a wrong row set for it.
                    assert_eq!(rows, truth, "byte {index} produced different rows");
                }
                Lookup::Unanswerable => unanswerable += 1,
                Lookup::Absent => {
                    panic!("byte {index}: corruption reported the term absent")
                }
            }
        }
        assert!(
            unanswerable > 0,
            "the sweep must hit the term's own sections"
        );
    }

    #[test]
    fn a_malformed_block_is_rejected_by_the_walker() {
        // Hand-built blocks: a zero document frequency, a postings length
        // shorter than the count, non-ascending terms, and a trailing byte.
        for block in [
            vec![1, 0, 3, b'a', b'b', b'c', 0, 1],       // df 0
            vec![1, 0, 3, b'a', b'b', b'c', 5, 4],       // len < df
            vec![2, 0, 1, b'b', 1, 1, 0, 1, b'a', 1, 1], // 'b' then 'a'
            vec![1, 0, 3, b'a', b'b', b'c', 1, 1, 0xff], // trailing byte
            vec![1, 1, 3, b'a', b'b', b'c', 1, 1],       // first term claims a prefix
            vec![9, 0, 1, b'a', 1, 1],                   // count past the payload
        ] {
            assert!(
                scan_block(&block, 0, |_, _, _, _| true).is_err(),
                "block {block:?}"
            );
        }
        // The shape the writer emits is accepted.
        let mut good = Vec::new();
        encode_block(&mut good, &[("abc", 1, 1), ("abd", 2, 2)]);
        let mut seen = Vec::new();
        scan_block(&good, 10, |term, df, offset, len| {
            seen.push((String::from_utf8(term.to_vec()).unwrap(), df, offset, len));
            true
        })
        .unwrap();
        assert_eq!(
            seen,
            vec![("abc".to_string(), 1, 10, 1), ("abd".to_string(), 2, 11, 2)]
        );
    }

    #[test]
    fn the_varint_reader_rejects_overflow_and_non_canonical_forms() {
        assert_eq!(Reader::new(&[0x00]).varint(), Some(0));
        assert_eq!(Reader::new(&[0x7f]).varint(), Some(127));
        assert_eq!(Reader::new(&[0x80, 0x01]).varint(), Some(128));
        // Ten continuation bytes then a payload that cannot fit in u64.
        assert_eq!(
            Reader::new(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]).varint(),
            None
        );
        // Unterminated.
        assert_eq!(Reader::new(&[0x80]).varint(), None);
        // Non-canonical: a redundant zero group.
        assert_eq!(Reader::new(&[0x80, 0x00]).varint(), None);
        // The round trip the writer produces, at the boundaries.
        for value in [0u64, 1, 127, 128, 16_383, 16_384, u32::MAX as u64, u64::MAX] {
            let mut out = Vec::new();
            write_varint(&mut out, value);
            assert_eq!(Reader::new(&out).varint(), Some(value), "value {value}");
        }
    }

    #[test]
    fn block_size_trades_resident_directory_against_bytes_per_lookup() {
        let rows = corpus(20_000);
        let mut sizes = Vec::new();
        for target in [256usize, 4_096, 65_536] {
            let mut writer = SegmentedWriter::new(target);
            for chunk in rows.chunks(5_000) {
                writer.push_group_rows(chunk.iter().map(String::as_str));
            }
            let reader = open(writer.finish());
            reader.source().reset_counters();
            assert!(matches!(reader.postings("rareneedle"), Lookup::Rows(_)));
            sizes.push((
                target,
                reader.resident_bytes(),
                reader.source().bytes_read(),
            ));
        }
        // Bigger blocks: smaller directory, more bytes per lookup. Both
        // directions must hold, or the knob is not the trade it claims to be.
        assert!(
            sizes[0].1 > sizes[1].1 && sizes[1].1 > sizes[2].1,
            "resident directory should shrink as blocks grow: {sizes:?}"
        );
        assert!(
            sizes[0].2 < sizes[1].2 && sizes[1].2 < sizes[2].2,
            "bytes per lookup should grow as blocks grow: {sizes:?}"
        );
    }

    #[test]
    fn a_segmented_blob_can_be_built_from_an_existing_v1_index_per_group() {
        // The #4377 path: postings arrive one row group at a time and the blob
        // is written forward, never patched.
        let rows = corpus(600);
        let mut writer = SegmentedWriter::default();
        for chunk in rows.chunks(150) {
            let group = InvertedIndex::from_rows(chunk.iter().map(String::as_str));
            writer.push_group_index(&group);
        }
        let reader = open(writer.finish());
        let whole = InvertedIndex::from_rows(rows.iter().map(String::as_str));
        for (term, expected) in whole.terms() {
            assert_eq!(
                reader.postings(term),
                Lookup::Rows(expected.to_vec()),
                "term {term}"
            );
        }
    }
}
