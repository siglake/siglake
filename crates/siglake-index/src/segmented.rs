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
//!   that leaves is measured and priced in
//!   `docs/DESIGN_segmented_inverted_index.md`.
//! - **The directory cannot address bytes that are not its own.** The body is
//!   tiled exactly by the sections — group `i`'s postings, then its dictionary,
//!   in group order, from the header to the directory — and a block's postings
//!   base starts at 0, strictly ascends and ends before its section does. Both
//!   are checked at [`SegmentedReader::open`], because bounding each range
//!   against the directory's offset alone accepts a directory whose sections
//!   overlap, and a lookup that reads one term's postings out of another's
//!   bytes can answer with the wrong rows
//!   (`a_directory_that_is_consistent_and_lies_about_the_structure_is_refused`).
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
///
/// The contract, stated here because #4561's reader integration is written
/// against it and the v1 decoder has no equivalent (it returns `None` for both
/// "absent" and "unparseable"):
///
/// - **`Rows`** is *strictly ascending*, file-physical, and covers every group
///   the lookup was allowed to read. A caller may skip every row not in it.
/// - **`Absent`** is a definitive no-match over the groups the lookup covered:
///   the term normalizes, every group the caller kept was read, and none has
///   it. Skipping the whole file (or the kept groups) is licensed. An empty
///   group selection lands here — the caller pruned everything, so nothing in
///   the kept set matches.
/// - **`Unanswerable`** is "this index concluded nothing; scan". It covers a
///   term that does not normalize, a malformed section, a failed range read,
///   and a row-group selection this sidecar cannot serve (see
///   [`SegmentedReader::postings_in_groups`]). It is never partial: a lookup that
///   found rows in one group and could not read another returns
///   `Unanswerable`, not the rows it managed to get.
///
/// Two deliberate divergences from the shipped index. Where
/// [`InvertedIndex::postings`](crate::InvertedIndex::postings) returns `None`
/// for a term that does not normalize and
/// [`InvertedIndex::matching_rows_all`](crate::InvertedIndex::matching_rows_all)
/// then reads that as "no rows match", a segmented lookup answers
/// `Unanswerable` and its AND entry point returns `None` — an unindexable term
/// constrains nothing, so it must not license skipping rows. (The shipped
/// reader does not reach that hazard: it fills `RawPruneSpec` from tokenizer
/// output, which normalizes by construction. This format does not rely on the
/// caller for it.) And where the v1 decoder's failure is confined to
/// `from_bytes`, a partial reader can fail per lookup, which is why the third
/// case has to exist at all.
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
        let dir = encode_directory(self.next_row, &self.groups);
        append_directory_and_trailer(&mut self.out, &dir);
        self.out
    }
}

/// The directory body, as the trailer's `dir_offset`/`dir_len`/`dir_crc`
/// describe it. Factored out of [`SegmentedWriter::finish`] so the
/// malformed-directory fixtures re-encode a *real* directory through the
/// writer's own encoder rather than a second copy of it that can drift.
fn encode_directory(n_rows: u32, groups: &[GroupEntry]) -> Vec<u8> {
    let mut dir = Vec::new();
    write_varint(&mut dir, u64::from(n_rows));
    write_varint(&mut dir, groups.len() as u64);
    for group in groups {
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
    dir
}

/// Append `dir` at the current end of `out` and stamp the trailer that
/// addresses it.
fn append_directory_and_trailer(out: &mut Vec<u8>, dir: &[u8]) {
    let dir_offset = out.len() as u64;
    out.extend_from_slice(dir);
    out.extend_from_slice(&dir_offset.to_le_bytes());
    out.extend_from_slice(&(dir.len() as u64).to_le_bytes());
    out.extend_from_slice(&crc32(dir).to_le_bytes());
    out.push(SEGMENTED_VERSION);
    out.extend_from_slice(SEGMENTED_MAGIC);
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
        // The body between the header and the directory, which the sections
        // must tile exactly (see below).
        let mut body_cursor = 5u64;
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
            // The body is tiled exactly, in the order the one-forward-pass
            // writer emits: group `i`'s postings, then its dictionary, from the
            // end of the header to the start of the directory. Bounding each
            // range against `dir_offset` alone would accept sections that
            // overlap each other, and a block is bounded only by its own
            // group's `dict_len` — so an inflated one could address a
            // neighbouring group's postings as a dictionary block.
            if postings_offset != body_cursor
                || dict_offset != postings_offset.checked_add(postings_len)?
            {
                return None;
            }
            body_cursor = dict_offset.checked_add(group_dict_len)?;
            if n_blocks > c.remaining() {
                return None;
            }
            let mut blocks: Vec<BlockEntry> = Vec::with_capacity(n_blocks);
            let mut previous_term: Option<Box<str>> = None;
            let mut previous_base: Option<u64> = None;
            for _ in 0..n_blocks {
                let term_len = usize::try_from(c.varint()?).ok()?;
                let first_term: Box<str> = std::str::from_utf8(c.take(term_len)?).ok()?.into();
                let offset = u32::try_from(c.varint()?).ok()?;
                let len = u32::try_from(c.varint()?).ok()?;
                let postings_base = c.varint()?;
                let crc = u32::from_le_bytes(c.take(4)?.try_into().ok()?);
                // Blocks partition the group's dictionary in term order, and
                // their postings bases march forward inside its postings: the
                // first block's first term starts the section, every block
                // holds at least one term and every term at least one posting
                // byte, so the bases begin at 0, strictly ascend, and all sit
                // before the section's end. Checking only `<= postings_len`
                // would accept a base pointing anywhere inside it, which reads
                // one term's postings out of another's bytes.
                match &previous_term {
                    Some(previous) if previous.as_ref() >= first_term.as_ref() => return None,
                    _ => {}
                }
                let base_marches_forward = match previous_base {
                    None => postings_base == 0,
                    Some(previous) => postings_base > previous,
                };
                if u64::from(offset).checked_add(u64::from(len))? > group_dict_len
                    || !base_marches_forward
                    || postings_base >= postings_len
                {
                    return None;
                }
                previous_term = Some(first_term.clone());
                previous_base = Some(postings_base);
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
        if c.remaining() != 0 || expected_first_row != n_rows || body_cursor != dir_offset {
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

    /// [`Self::postings`] restricted to `groups` — the reject path: a group the
    /// scan already pruned costs no read at all.
    ///
    /// `groups` are indices into the file's row groups and must be **strictly
    /// ascending and in range**; anything else is [`Lookup::Unanswerable`]
    /// (see [`Self::group_indices`]). `Some(&[])` is not malformed — the caller
    /// pruned every group, so no kept row matches, which is
    /// [`Lookup::Absent`].
    pub fn postings_in_groups(&self, term: &str, groups: Option<&[usize]>) -> Lookup {
        let Some(normalized) = normalize_query_term(term) else {
            return Lookup::Unanswerable;
        };
        let Some(indices) = self.group_indices(groups) else {
            return Lookup::Unanswerable;
        };
        let mut rows: Vec<u32> = Vec::new();
        let mut found = false;
        for index in indices {
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

    /// [`Self::matching_rows_all`] restricted to `groups`, under the same
    /// selection contract [`Self::postings_in_groups`] states.
    ///
    /// Resolution is per group and **rarest first**, which is what the
    /// directory's document frequencies buy over the v1 index: every term is
    /// located in the group's dictionary first (one block read each, the read
    /// a point lookup pays anyway), and only then are postings fetched, in
    /// ascending `df`, stopping as soon as the running intersection empties. A
    /// term the group does not have ends the group before any posting section
    /// is read at all. On the measurement corpus that is the difference
    /// between reading a 2%-density term's 152.9 KiB of postings and not
    /// reading them (`docs/DESIGN_segmented_inverted_index.md`).
    pub fn matching_rows_all_in_groups(
        &self,
        terms: &[&str],
        groups: Option<&[usize]>,
    ) -> Option<Vec<u32>> {
        // Checked before the empty-term shortcut, so a selection this sidecar
        // cannot serve never gets an answer at all.
        let indices = self.group_indices(groups)?;
        if terms.is_empty() {
            return Some(Vec::new());
        }
        // Up front, so a term that does not normalize is `Unanswerable` for
        // the whole lookup rather than per group.
        let mut normalized: Vec<String> = Vec::with_capacity(terms.len());
        for term in terms {
            normalized.push(normalize_query_term(term)?);
        }
        let mut rows: Vec<u32> = Vec::new();
        for index in indices {
            let group = &self.groups[index];
            let mut located: Vec<(u64, u32, u32)> = Vec::with_capacity(normalized.len());
            for term in &normalized {
                match self.locate_term(group, term) {
                    Ok(Some(hit)) => located.push(hit),
                    // No group-wide match is possible, and no postings were
                    // read for the terms already located.
                    Ok(None) => break,
                    Err(()) => return None,
                }
            }
            if located.len() != normalized.len() {
                continue;
            }
            located.sort_by_key(|(_, _, df)| *df);
            let mut acc: Option<Vec<u32>> = None;
            for (offset, len, df) in located {
                let list = self.read_postings(group, offset, len, df).ok()?;
                acc = Some(match acc {
                    None => list,
                    Some(previous) => intersect_sorted(&previous, &list),
                });
                if acc.as_ref().is_some_and(Vec::is_empty) {
                    break;
                }
            }
            rows.extend(acc.unwrap_or_default());
        }
        Some(rows)
    }

    /// Rows matching **any** of `terms` — the disjunction the shipped reader
    /// builds for `RawPruneSpec::any_terms`, which the v1 index has no entry
    /// point for either (it unions `postings` per term and skips what it
    /// cannot answer).
    ///
    /// Under the three-outcome contract that skip is not available: a term this
    /// index cannot answer might have matched any row, so the disjunction as a
    /// whole concludes nothing and this returns `None`. An
    /// [`Absent`](Lookup::Absent) term contributes no rows, and an empty
    /// `terms` is a definitive no-match.
    pub fn matching_rows_any(&self, terms: &[&str]) -> Option<Vec<u32>> {
        self.matching_rows_any_in_groups(terms, None)
    }

    /// [`Self::matching_rows_any`] restricted to `groups`, under the same
    /// selection contract [`Self::postings_in_groups`] states.
    pub fn matching_rows_any_in_groups(
        &self,
        terms: &[&str],
        groups: Option<&[usize]>,
    ) -> Option<Vec<u32>> {
        self.group_indices(groups)?;
        let mut rows: Vec<u32> = Vec::new();
        for term in terms {
            match self.postings_in_groups(term, groups) {
                Lookup::Rows(list) => rows = union_sorted(&rows, &list),
                Lookup::Absent => {}
                Lookup::Unanswerable => return None,
            }
        }
        Some(rows)
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
        let indices = self.group_indices(groups)?;
        let mut rows: Vec<u32> = Vec::new();
        for index in indices {
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

    /// Validate a caller's row-group selection: strictly ascending, and every
    /// index inside this sidecar's groups. `None` — which every caller turns
    /// into "cannot answer, scan" — rather than skipping an out-of-range index
    /// or serving a repeated one, because both produce a row set the caller
    /// reads as complete: the first answers over fewer groups than it asked
    /// for, the second returns a group's postings twice and so is not
    /// ascending, and `intersect_sorted` / `row_selection_runs` both drop rows
    /// from a list that is not.
    fn group_indices(&self, groups: Option<&[usize]>) -> Option<Vec<usize>> {
        let Some(selected) = groups else {
            return Some((0..self.groups.len()).collect());
        };
        let mut previous: Option<usize> = None;
        for &index in selected {
            if index >= self.groups.len() || previous.is_some_and(|previous| index <= previous) {
                return None;
            }
            previous = Some(index);
        }
        Some(selected.to_vec())
    }

    /// `Ok(None)` = this group does not have the term; `Err(())` = malformed.
    fn group_postings(&self, group: &GroupEntry, normalized: &str) -> Result<Option<Vec<u32>>, ()> {
        match self.locate_term(group, normalized)? {
            None => Ok(None),
            Some((offset, len, df)) => self.read_postings(group, offset, len, df).map(Some),
        }
    }

    /// The term's `(postings offset, length, document frequency)` in this
    /// group, from one dictionary-block read — the half of a lookup that costs
    /// the block and not the postings, which is what lets an AND order its
    /// posting fetches by `df` and skip a group that is missing a term.
    ///
    /// `Ok(None)` = this group does not have the term; `Err(())` = malformed.
    fn locate_term(
        &self,
        group: &GroupEntry,
        normalized: &str,
    ) -> Result<Option<(u64, u32, u32)>, ()> {
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
        Ok(hit)
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

/// CRC-32 (IEEE 802.3, reflected, `0xedb8_8320`) — the format's checksum,
/// wherever it is computed.
///
/// `crc32fast` rather than the prototype's bytewise table: the writer
/// checksums every dictionary block of every file (35.9 MiB per 7.34M-row file
/// on the measurement corpus) and a cold open checksums the whole directory
/// (474.9 KiB), which the table version does at ~0.5 GB/s against ~50 GB/s
/// hardware-accelerated. The crate was already in the tree behind flate2, so
/// taking it directly resolves no new package. `crc32_reference` in this
/// module's tests pins the value against the algorithm and the standard check
/// vector, so the bytes on disk do not depend on which implementation computes
/// them.
fn crc32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
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

    /// One row of the measurement corpus, with the sparse term's period taken
    /// from a knob — the same text
    /// `tests/segmented_measure.rs` generates, so the report below is
    /// comparable with the tables in
    /// `docs/DESIGN_segmented_inverted_index.md`.
    fn measurement_row(row: usize, rare_every: usize) -> String {
        let queen = if row.is_multiple_of(50) { " queen" } else { "" };
        let checkout = if row.is_multiple_of(20) {
            " checkout"
        } else {
            ""
        };
        let rare = if rare_every > 0 && row.is_multiple_of(rare_every) {
            " rareneedle"
        } else {
            ""
        };
        format!(
            "service-{} status {}{queen}{checkout}{rare} row-{row:06}",
            row % 20,
            200 + row % 5
        )
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
    fn an_and_skips_a_group_that_is_missing_a_term_before_reading_postings() {
        let rows = corpus(20_000);
        let v1 = InvertedIndex::from_rows(rows.iter().map(String::as_str));
        let reader = open(encoded(&rows, 5_000));

        // `000001` is one row's unique token, so three of the four groups
        // cannot match the conjunction at all. `status` is on every row, so
        // its posting sections are the bytes worth not reading.
        reader.source().reset_counters();
        let common = reader.postings("status");
        let common_bytes = reader.source().bytes_read();
        assert!(matches!(common, Lookup::Rows(_)));

        reader.source().reset_counters();
        let matching = reader.matching_rows_all(&["status", "000001"]);
        let and_bytes = reader.source().bytes_read();

        assert_eq!(matching, Some(vec![1]));
        assert_eq!(matching, Some(v1.matching_rows_all(&["status", "000001"])));
        assert!(
            and_bytes < common_bytes,
            "the conjunction fetched {and_bytes} bytes, more than the common \
             term's own {common_bytes}: a missing term did not stop the group"
        );
    }

    #[test]
    fn an_and_stops_fetching_once_the_intersection_is_empty() {
        let rows = corpus(20_000);
        let v1 = InvertedIndex::from_rows(rows.iter().map(String::as_str));
        let reader = open(encoded(&rows, 5_000));
        // Row 1 carries neither `queen` (every 50th) nor `checkout` (every
        // 20th), so the rarest list empties the intersection in the one group
        // that has `000001`, before the densest term's section is read.
        let terms = ["status", "queen", "000001"];

        reader.source().reset_counters();
        let common = reader.postings("status");
        let common_bytes = reader.source().bytes_read();
        assert!(matches!(common, Lookup::Rows(_)));

        reader.source().reset_counters();
        let matching = reader.matching_rows_all(&terms);
        let and_bytes = reader.source().bytes_read();

        assert_eq!(matching, Some(Vec::new()));
        assert_eq!(matching, Some(v1.matching_rows_all(&terms)));
        assert!(
            and_bytes < common_bytes,
            "the conjunction fetched {and_bytes} bytes against {common_bytes} \
             for one of its common terms alone"
        );
    }

    #[test]
    fn an_or_unions_what_it_can_answer_and_refuses_what_it_cannot() {
        let rows = corpus(1_100);
        let v1 = InvertedIndex::from_rows(rows.iter().map(String::as_str));
        let reader = open(encoded(&rows, 256));

        for terms in [
            vec!["queen", "checkout"],
            vec!["rareneedle", "absentterm"],
            vec!["absentterm", "alsoabsent"],
            vec!["000001", "000999", "queen"],
        ] {
            let expected = terms.iter().fold(Vec::new(), |acc: Vec<u32>, term| {
                union_sorted(&acc, v1.postings(term).unwrap_or(&[]))
            });
            assert_eq!(
                reader.matching_rows_any(&terms),
                Some(expected),
                "OR {terms:?}"
            );
        }

        // A term that does not normalize might have matched any row, so the
        // disjunction concludes nothing — it does not quietly drop the term
        // the way the v1 reader's per-term union does.
        assert_eq!(reader.matching_rows_any(&["queen", "qu"]), None);
        // And the row-group selection contract holds for the OR entry point.
        assert_eq!(
            reader.matching_rows_any_in_groups(&["queen"], Some(&[99])),
            None
        );
        assert_eq!(
            reader.matching_rows_any_in_groups(&["queen"], Some(&[])),
            Some(Vec::new())
        );
        assert_eq!(reader.matching_rows_any(&[]), Some(Vec::new()));
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
        // An unanswerable term must not be read as "no rows match" — the
        // divergence from v1, which conflates the two and so lets a term too
        // short to index skip every row in the file.
        assert_eq!(reader.postings("ab"), Lookup::Unanswerable);
        assert_eq!(reader.matching_rows_all(&["ab"]), None);
        let v1 = InvertedIndex::from_rows(rows.iter().map(String::as_str));
        assert_eq!(v1.postings("ab"), None);
        assert_eq!(v1.matching_rows_all(&["ab"]), Vec::<u32>::new());
        assert_eq!(reader.matching_rows_all(&[]), Some(Vec::new()));
    }

    #[test]
    fn a_row_group_selection_the_sidecar_cannot_serve_is_unanswerable() {
        let rows = corpus(1_000);
        let reader = open(encoded(&rows, 250));
        assert_eq!(reader.n_groups(), 4);
        let Lookup::Rows(truth) = reader.postings("queen") else {
            panic!("present");
        };

        // The contract a caller has to hold: ascending, no repeats, every index
        // inside the sidecar's groups.
        assert_eq!(
            reader.postings_in_groups("queen", Some(&[0, 1, 2, 3])),
            Lookup::Rows(truth)
        );
        // Out of range means the caller's row-group map and the sidecar
        // disagree — the case `matches_row_groups` exists to catch. Answering
        // over the groups that do exist would look complete and silently drop
        // the rest.
        assert_eq!(
            reader.postings_in_groups("queen", Some(&[0, 1, 9])),
            Lookup::Unanswerable
        );
        assert_eq!(
            reader.matching_rows_all_in_groups(&["queen"], Some(&[0, 9])),
            None
        );
        assert_eq!(reader.rows_containing_in_groups("ueen", Some(&[9])), None);
        // A repeat would count a group's postings twice and a descending pair
        // would return them out of order; every consumer of the result
        // (`intersect_sorted`, `row_selection_runs`) drops rows from a list
        // that is not strictly ascending, so neither may be served.
        assert_eq!(
            reader.postings_in_groups("queen", Some(&[1, 1])),
            Lookup::Unanswerable
        );
        assert_eq!(
            reader.postings_in_groups("queen", Some(&[2, 0])),
            Lookup::Unanswerable
        );
        assert_eq!(
            reader.rows_containing_in_groups("ueen", Some(&[0, 0])),
            None
        );
        // An empty selection is not a malformed one: the caller pruned every
        // group, so no row in the kept set matches.
        assert_eq!(
            reader.postings_in_groups("queen", Some(&[])),
            Lookup::Absent
        );
        assert_eq!(
            reader.matching_rows_all_in_groups(&["queen"], Some(&[])),
            Some(Vec::new())
        );
        assert_eq!(
            reader.rows_containing_in_groups("ueen", Some(&[])),
            Some(Vec::new())
        );
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

    /// A blob with several blocks per group, so the block-order and
    /// block-range fixtures have more than one block to get wrong.
    fn multi_block_blob(rows: &[String], group_rows: usize) -> Vec<u8> {
        let mut writer = SegmentedWriter::new(256);
        for chunk in rows.chunks(group_rows) {
            writer.push_group_rows(chunk.iter().map(String::as_str));
        }
        writer.finish()
    }

    fn trailer_dir_offset(blob: &[u8]) -> usize {
        let at = blob.len() - SEGMENTED_TRAILER_LEN;
        u64::from_le_bytes(blob[at..at + 8].try_into().unwrap()) as usize
    }

    /// Re-encode a blob's directory after `edit` has changed the structure the
    /// reader parses, and re-stamp the trailer's length and CRC.
    ///
    /// Every fixture that mutates directory *bytes* is refused for one reason —
    /// the `dir_crc` no longer matches — so the directory parser's own checks
    /// (groups tiling the row domain, ranges inside the blob, blocks in term
    /// order with bounded offsets) are never reached by one. Recomputing the
    /// CRC is what makes those checks reachable: each case arrives at `open`
    /// with a directory that is internally consistent as bytes and wrong as a
    /// structure, which is also the shape a writer bug produces.
    fn with_directory(blob: &[u8], edit: impl FnOnce(&mut u32, &mut Vec<GroupEntry>)) -> Vec<u8> {
        let reader = SegmentedReader::open(SliceSource::new(blob.to_vec()))
            .expect("a fixture starts from a well-formed blob");
        let mut n_rows = reader.n_rows;
        let mut groups = reader.groups.clone();
        edit(&mut n_rows, &mut groups);
        let mut out = blob[..trailer_dir_offset(blob)].to_vec();
        append_directory_and_trailer(&mut out, &encode_directory(n_rows, &groups));
        out
    }

    /// The same, with the directory body written by hand — for the claims no
    /// `GroupEntry` list can express (a count larger than the bytes behind it,
    /// a truncated entry, a term that is not UTF-8).
    fn with_directory_bytes(blob: &[u8], dir: Vec<u8>) -> Vec<u8> {
        let mut out = blob[..trailer_dir_offset(blob)].to_vec();
        append_directory_and_trailer(&mut out, &dir);
        out
    }

    #[test]
    fn a_directory_that_is_consistent_and_lies_about_the_structure_is_refused() {
        let rows = corpus(1_000);
        let blob = multi_block_blob(&rows, 250);
        let reader = open(blob.clone());
        assert_eq!(reader.n_groups(), 4);
        assert!(
            reader.groups.iter().all(|group| group.blocks.len() > 2),
            "the fixture needs several blocks per group: {:?}",
            reader.group_rows()
        );

        // Positive control: re-encoding the directory unchanged reproduces the
        // blob byte for byte. Without it, every case below could be passing
        // because the harness breaks the blob rather than because a check
        // fires.
        assert_eq!(with_directory(&blob, |_, _| {}), blob);
        assert!(
            SegmentedReader::open(SliceSource::new(with_directory(&blob, |_, _| {}))).is_some()
        );

        type Edit = fn(&mut u32, &mut Vec<GroupEntry>);
        let cases: [(&str, Edit); 15] = [
            ("a gap between two groups", |_, groups| {
                groups[1].first_row += 1;
            }),
            ("overlapping groups", |_, groups| {
                groups[1].first_row -= 1;
            }),
            ("groups that do not sum to n_rows", |_, groups| {
                groups[3].n_rows -= 1;
            }),
            ("n_rows past the tiled groups", |n_rows, _| {
                *n_rows += 1;
            }),
            ("a dictionary overrunning its group", |_, groups| {
                groups[0].dict_len += 1;
            }),
            ("a section not where the previous one ended", |_, groups| {
                groups[1].postings_offset += 1;
            }),
            ("a gap before the directory", |_, groups| {
                groups[3].dict_len -= 1;
            }),
            ("postings starting inside the header", |_, groups| {
                groups[0].postings_offset = 0;
            }),
            ("a block reaching past its dictionary", |_, groups| {
                groups[0].blocks[0].len += groups[0].dict_len as u32;
            }),
            ("a postings base past the group's postings", |_, groups| {
                groups[0].blocks[0].postings_base = groups[0].postings_len + 1;
            }),
            ("a postings base not starting the section", |_, groups| {
                groups[0].blocks[0].postings_base += 1;
            }),
            ("postings bases out of order", |_, groups| {
                let blocks = &mut groups[0].blocks;
                let (first, second) = (blocks[1].postings_base, blocks[2].postings_base);
                blocks[1].postings_base = second;
                blocks[2].postings_base = first;
            }),
            ("a postings base at the end of the section", |_, groups| {
                let last = groups[0].blocks.len() - 1;
                groups[0].blocks[last].postings_base = groups[0].postings_len;
            }),
            ("blocks out of term order", |_, groups| {
                let first = groups[0].blocks[0].first_term.clone();
                let second = groups[0].blocks[1].first_term.clone();
                groups[0].blocks[0].first_term = second;
                groups[0].blocks[1].first_term = first;
            }),
            ("a repeated block term", |_, groups| {
                groups[0].blocks[1].first_term = groups[0].blocks[0].first_term.clone();
            }),
        ];
        for (name, edit) in cases {
            let corrupt = with_directory(&blob, edit);
            assert!(
                SegmentedReader::open(SliceSource::new(corrupt)).is_none(),
                "{name} must be refused at open"
            );
        }

        // Hand-written directory bodies: a count that cannot be backed by the
        // bytes behind it must never be allocated from, and a body the reader
        // does not consume exactly is corrupt.
        let group = &reader.groups[0];
        let mut group_header = Vec::new();
        write_varint(&mut group_header, u64::from(group.first_row));
        write_varint(&mut group_header, u64::from(group.n_rows));
        write_varint(&mut group_header, group.dict_offset);
        write_varint(&mut group_header, group.dict_len);
        write_varint(&mut group_header, group.postings_offset);
        write_varint(&mut group_header, group.postings_len);

        let mut huge_groups = Vec::new();
        write_varint(&mut huge_groups, 1_000);
        write_varint(&mut huge_groups, u64::MAX / 2);

        let mut huge_blocks = Vec::new();
        write_varint(&mut huge_blocks, 1_000);
        write_varint(&mut huge_blocks, 1);
        huge_blocks.extend_from_slice(&group_header);
        write_varint(&mut huge_blocks, u64::MAX / 2);

        let mut truncated = Vec::new();
        write_varint(&mut truncated, 1_000);
        write_varint(&mut truncated, 2);
        truncated.extend_from_slice(&group_header);

        let mut trailing = encode_directory(reader.n_rows, &reader.groups);
        trailing.push(0);

        let mut not_utf8 = encode_directory(reader.n_rows, &reader.groups);
        let term = reader.groups[0].blocks[0].first_term.as_bytes();
        let at = not_utf8
            .windows(term.len())
            .position(|window| window == term)
            .expect("the directory carries the block's first term");
        not_utf8[at] = 0xff;

        for (name, dir) in [
            ("a group count past the directory", huge_groups),
            ("a block count past the directory", huge_blocks),
            ("a directory ending mid-group", truncated),
            ("a trailing byte after the last group", trailing),
            ("a block term that is not UTF-8", not_utf8),
        ] {
            let corrupt = with_directory_bytes(&blob, dir);
            assert!(
                SegmentedReader::open(SliceSource::new(corrupt)).is_none(),
                "{name} must be refused at open"
            );
        }
    }

    #[test]
    fn a_directory_that_misaddresses_a_block_is_unanswerable_not_absent() {
        // The two mutations that survive every structural check: both ranges
        // stay inside the group, the blocks stay in term order, and the
        // directory's own CRC is recomputed — so `open` accepts and the
        // per-block CRC and the document-frequency cross-check are the only
        // things standing between a writer bug and a silently short answer.
        let rows = corpus(1_000);
        let blob = multi_block_blob(&rows, 250);
        let reader = open(blob.clone());
        let term = reader.groups[0].blocks[0].first_term.to_string();
        assert!(matches!(reader.postings(&term), Lookup::Rows(_)));

        // Two blocks' byte ranges swapped: each block's recorded CRC now
        // belongs to the other one's payload.
        let swapped = with_directory(&blob, |_, groups| {
            let (first, second) = (groups[0].blocks[0].clone(), groups[0].blocks[1].clone());
            groups[0].blocks[0].offset = second.offset;
            groups[0].blocks[0].len = second.len;
            groups[0].blocks[1].offset = first.offset;
            groups[0].blocks[1].len = first.len;
        });
        let swapped = SegmentedReader::open(SliceSource::new(swapped))
            .expect("a swap inside the group's dictionary still opens");
        assert_eq!(swapped.postings(&term), Lookup::Unanswerable);

        // A middle block's postings base moved by one byte: it still ascends
        // from its predecessor and still sits inside the section, so the
        // directory's checks pass, the block itself verifies, and the term's
        // postings are decoded from the wrong offset. This is the residual the
        // format knowingly carries — only a checksum over the postings
        // themselves sees it, and what that costs is measured in
        // `docs/DESIGN_segmented_inverted_index.md`, "Do posting sections need
        // their own checksum?".
        let middle = reader.groups[0].blocks[1].first_term.to_string();
        let Lookup::Rows(middle_truth) = reader.postings(&middle) else {
            panic!("the block's own first term is present");
        };
        let shifted = with_directory(&blob, |_, groups| {
            groups[0].blocks[1].postings_base += 1;
        });
        let shifted = SegmentedReader::open(SliceSource::new(shifted))
            .expect("a one-byte shift inside the group's postings still opens");
        let answer = shifted.postings(&middle);
        assert_ne!(
            answer,
            Lookup::Absent,
            "a misaddressed posting range must not report a present term absent"
        );
        assert_ne!(
            answer,
            Lookup::Rows(middle_truth),
            "the fixture is meant to address the wrong bytes"
        );
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

    /// The format's checksum, written out: reflected CRC-32 with polynomial
    /// `0xedb8_8320`, initial value `0xffff_ffff`, final inversion. This is the
    /// specification `crc32` has to keep agreeing with, and the reason
    /// swapping in an accelerated implementation cannot change a byte on disk.
    fn crc32_reference(bytes: &[u8]) -> u32 {
        let mut crc = 0xffff_ffffu32;
        for &byte in bytes {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = if crc & 1 == 1 {
                    0xedb8_8320 ^ (crc >> 1)
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }

    #[test]
    fn the_checksum_is_ieee_crc32_whoever_computes_it() {
        // The standard check value: CRC-32 of "123456789".
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
        assert_eq!(crc32(b""), 0);
        // Agreement with the written-out algorithm over the shapes the format
        // actually checksums — a directory, a dictionary block, and the sizes
        // where a chunked implementation's boundaries would show.
        let rows = corpus(500);
        let blob = encoded(&rows, 125);
        let reader = open(blob.clone());
        let directory = encode_directory(reader.n_rows(), &reader.groups);
        assert_eq!(crc32(&directory), crc32_reference(&directory));
        for group in &reader.groups {
            for block in &group.blocks {
                let bytes = reader
                    .read_block(group, block)
                    .expect("the fixture's blocks verify");
                assert_eq!(crc32(&bytes), crc32_reference(&bytes));
                assert_eq!(crc32(&bytes), block.crc);
            }
        }
        for len in [1usize, 7, 8, 15, 16, 31, 63, 64, 127, 128, 1_024, 4_096] {
            let bytes: Vec<u8> = (0..len)
                .map(|index| (index as u8).wrapping_mul(31))
                .collect();
            assert_eq!(crc32(&bytes), crc32_reference(&bytes), "{len} bytes");
        }
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

    /// The two open format questions #4560 owes, priced at the scale
    /// `docs/DESIGN_segmented_inverted_index.md` reports: whether posting
    /// sections need their own checksum, and what per-section compression
    /// would cost. Both turn on the same number — the bytes behind one
    /// dictionary block's terms — because both can only be done at a
    /// granularity the reader fetches whole.
    ///
    /// ```
    /// cargo test -p siglake-index --release --lib \
    ///   report_posting_checksum_and_compression_options -- --ignored --nocapture
    /// ```
    ///
    /// Sized by `SIGLAKE_SEG_ROWS`, `SIGLAKE_SEG_GROUP_ROWS`,
    /// `SIGLAKE_SEG_RARE_EVERY` and `SIGLAKE_SEG_BLOCK_BYTES`.
    #[test]
    #[ignore = "measurement: builds a multi-million-row sidecar"]
    fn report_posting_checksum_and_compression_options() {
        fn knob(name: &str, default: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        fn mib(bytes: u64) -> f64 {
            bytes as f64 / (1024.0 * 1024.0)
        }
        // Puffin registers a compressed blob at zstd level 3
        // (`third_party/iceberg/src/compression.rs`), so every ratio here is
        // taken at the level the shipped sidecar actually pays.
        fn zstd_len(bytes: &[u8]) -> u64 {
            zstd::encode_all(bytes, 3).expect("zstd").len() as u64
        }

        let rows = knob("SIGLAKE_SEG_ROWS", 7_340_000);
        let group_rows = knob("SIGLAKE_SEG_GROUP_ROWS", 1_048_576);
        let rare_every = knob("SIGLAKE_SEG_RARE_EVERY", 100_000);
        let block_bytes = knob("SIGLAKE_SEG_BLOCK_BYTES", DEFAULT_TARGET_BLOCK_BYTES);

        let mut writer = SegmentedWriter::new(block_bytes);
        let mut n_terms = 0usize;
        let mut group_start = 0usize;
        while group_start < rows {
            let group_end = (group_start + group_rows).min(rows);
            // One group's text at a time: the whole corpus materialized would
            // be ~400 MB of `String` at the default size.
            let text: Vec<String> = (group_start..group_end)
                .map(|row| measurement_row(row, rare_every))
                .collect();
            let group = InvertedIndex::from_rows(text.iter().map(String::as_str));
            n_terms += group.terms().len();
            writer.push_group_index(&group);
            group_start = group_end;
        }
        let blob = writer.finish();
        let blob_len = blob.len() as u64;
        let reader = open(blob.clone());
        let directory = encode_directory(reader.n_rows(), &reader.groups);
        let n_blocks: usize = reader.groups.iter().map(|group| group.blocks.len()).sum();

        println!(
            "corpus {rows} rows, {} groups of {group_rows}, {n_terms} terms, \
{n_blocks} blocks at {block_bytes} B",
            reader.n_groups()
        );
        println!(
            "blob {:.1} MiB = dictionary {:.1} + postings {:.1} + directory {:.1} KiB",
            mib(blob_len),
            mib(reader.dictionary_bytes()),
            mib(reader.postings_bytes()),
            directory.len() as f64 / 1024.0
        );

        // --- what one block's postings weigh -------------------------------
        // A block's posting span runs from its own base to the next block's
        // (the last one's to the end of the section). It is the unit a reader
        // would have to fetch whole to verify a checksum over postings, and
        // the unit a per-section codec could compress.
        let mut spans: Vec<u64> = Vec::with_capacity(n_blocks);
        let mut per_term: Vec<u64> = Vec::new();
        for group in &reader.groups {
            for (index, block) in group.blocks.iter().enumerate() {
                let end = match group.blocks.get(index + 1) {
                    Some(next) => next.postings_base,
                    None => group.postings_len,
                };
                spans.push(end - block.postings_base);
                let bytes = reader.read_block(group, block).expect("block verifies");
                scan_block(&bytes, block.postings_base, |_, _, _, len| {
                    per_term.push(u64::from(len));
                    true
                })
                .expect("block walks");
            }
        }
        spans.sort_unstable();
        per_term.sort_unstable();
        let median = |sorted: &[u64]| sorted[sorted.len() / 2];
        println!(
            "posting bytes per block: min {} median {} max {} | per term: median {} max {}",
            spans[0],
            median(&spans),
            spans[spans.len() - 1],
            median(&per_term),
            per_term[per_term.len() - 1],
        );

        // --- checksum options ----------------------------------------------
        let per_term_cost = 4 * n_terms as u64;
        let per_block_cost = 4 * n_blocks as u64;
        println!(
            "checksum over postings: per term {:.1} MiB (+{:.1}% of blob), \
per block {:.1} KiB (+{:.3}% of blob)",
            mib(per_term_cost),
            100.0 * per_term_cost as f64 / blob_len as f64,
            per_block_cost as f64 / 1024.0,
            100.0 * per_block_cost as f64 / blob_len as f64,
        );
        // What that costs a point lookup, which already fetches a whole
        // dictionary block per group: the comparison the decision turns on is
        // total fetched bytes, not the posting slice in isolation.
        let mut block_lens: Vec<u64> = reader
            .groups
            .iter()
            .flat_map(|group| group.blocks.iter().map(|block| u64::from(block.len)))
            .collect();
        block_lens.sort_unstable();
        let today = median(&block_lens) + median(&per_term);
        let verified = median(&block_lens) + median(&spans);
        println!(
            "per-block verification fetches the block's whole span, not one \
term's slice: postings {} -> {} B, and a point lookup's bytes per group \
{today} -> {verified} B ({:.2}x, dictionary block median {} B)",
            median(&per_term),
            median(&spans),
            verified as f64 / today as f64,
            median(&block_lens),
        );

        // --- compression ----------------------------------------------------
        // Whole-blob zstd is what a Puffin-registered v1 sidecar pays and what
        // a segmented one cannot use: `PuffinReader::blob` decompresses the
        // whole thing, which is exactly the property the format exists to
        // avoid. Per-block is the finest granularity that stays
        // range-addressable.
        let whole = zstd_len(&blob);
        let mut dict_compressed = 0u64;
        let mut postings_compressed = 0u64;
        for group in &reader.groups {
            for (index, block) in group.blocks.iter().enumerate() {
                let bytes = reader.read_block(group, block).expect("block verifies");
                dict_compressed += zstd_len(&bytes);
                let end = match group.blocks.get(index + 1) {
                    Some(next) => next.postings_base,
                    None => group.postings_len,
                };
                let span = reader
                    .source()
                    .read(
                        group.postings_offset + block.postings_base,
                        (end - block.postings_base) as usize,
                    )
                    .expect("the span is inside the blob");
                postings_compressed += zstd_len(&span);
            }
        }
        let per_block_total = dict_compressed + postings_compressed + directory.len() as u64;
        println!(
            "zstd-3 whole blob {:.1} MiB ({:.2}x) | per block: dictionary \
{:.1} MiB ({:.2}x), postings {:.1} MiB ({:.2}x), total with the directory \
{:.1} MiB ({:.2}x)",
            mib(whole),
            whole as f64 / blob_len as f64,
            mib(dict_compressed),
            dict_compressed as f64 / reader.dictionary_bytes() as f64,
            mib(postings_compressed),
            postings_compressed as f64 / reader.postings_bytes() as f64,
            mib(per_block_total),
            per_block_total as f64 / blob_len as f64,
        );

        // --- the checksum's own cost ----------------------------------------
        // What the writer and a cold open spend on CRCs: the writer checksums
        // every dictionary block, an open checksums the directory.
        let timed = |bytes: &[u8], f: fn(&[u8]) -> u32| {
            let started = std::time::Instant::now();
            let mut sink = 0u32;
            let mut total = 0u64;
            while started.elapsed() < std::time::Duration::from_millis(200) {
                sink ^= f(bytes);
                total += bytes.len() as u64;
            }
            std::hint::black_box(sink);
            total as f64 / started.elapsed().as_secs_f64() / 1e9
        };
        // Over the blob itself, not a cache-resident sample: the writer's CRCs
        // stream tens of MiB and the accelerated implementation is then
        // bandwidth-bound rather than issue-bound.
        let fast = timed(&blob, crc32);
        let reference = timed(&blob, crc32_reference);
        println!(
            "crc32 over {:.1} MiB: crc32fast {fast:.2} GB/s, bytewise reference \
{reference:.3} GB/s | per file: dictionary {:.1} ms vs {:.1} ms, directory \
{:.2} ms vs {:.2} ms",
            mib(blob_len),
            reader.dictionary_bytes() as f64 / (fast * 1e9) * 1e3,
            reader.dictionary_bytes() as f64 / (reference * 1e9) * 1e3,
            directory.len() as f64 / (fast * 1e9) * 1e3,
            directory.len() as f64 / (reference * 1e9) * 1e3,
        );
    }
}
