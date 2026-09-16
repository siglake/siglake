//! Raw-content token bloom filter — foundation for cheap `raw LIKE '%term%'`
//! pruning (see `docs/DESIGN_raw_content_index.md`).
//!
//! The idea: at compaction, tokenize each row's `raw` text and build a compact
//! bloom of all terms in a file (or row group); at query time, a single-term
//! `LIKE '%term%'` is lowered to a bloom probe so files/row groups that cannot
//! contain the term are skipped without decoding `raw`.
//!
//! This module is the path-independent, fully-tested core: a deterministic
//! tokenizer and a self-contained serializable bloom. It is NOT yet wired into
//! the compactor write path or the query scan — that core-path integration is
//! a deliberate, feature-flagged follow-up (the read side needs Parquet bloom
//! access the iceberg-rust 0.9 reader does not expose; see the design doc).
//!
//! The bloom uses FNV-1a + double hashing so the serialized form is stable and
//! portable (independent of `std`'s `DefaultHasher`, which is not stable).

/// Parquet KV key for the per-file **trigram** bloom (hex), which powers
/// arbitrary substring (`LIKE '%...%'`) pruning.
///
/// See `docs/DESIGN_file_formats.md` for the versioning contract. Blooms are in
/// the PRUNING class: a misread bloom can drop rows from a result, so the
/// payload carries its own magic + version and an unrecognized one is ignored
/// (the file is scanned) rather than probed.
pub const RAW_TRIGRAM_BLOOM_KV_KEY: &str = "siglake.raw_trigram_bloom.v1";

/// Parquet footer KV key holding the hex-encoded list of per-row-group
/// **trigram** blooms (one [`TokenBloom`] per row group, in order). Lets the
/// reader prune individual row groups, not just whole files.
pub const RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY: &str = "siglake.raw_trigram_rowgroup_blooms.v1";

pub mod group_counts;

pub use group_counts::{ColumnCounts, GroupCounts, GROUP_COUNTS_KV_KEY};

/// Parquet footer KV key holding the per-file time-bucket histogram: the row
/// count per fixed [`TIME_BUCKET_BASE_NS`]-aligned timestamp bucket. Lets a
/// `date_histogram` (date_bin/date_trunc) re-bucket from footers instead of
/// scanning the timestamp column of boundary-straddling files. JSON shape:
/// `{"base_ns":<n>,"buckets":{"<bucket_start_ns>":<count>,…},"nulls":<n>}`.
pub const TIME_BUCKETS_KV_KEY: &str = "siglake.time_buckets.v1";

/// Base granularity of the per-file time-bucket footer (1 minute). A
/// `date_histogram` whose interval is a whole multiple of this — minute, hour,
/// day, etc. — is served by re-bucketing the footer; finer (sub-minute) intervals
/// fall back to scanning timestamps. 1 minute keeps the footer small (a file's
/// bucket count is its time span in minutes) while covering the common dashboard
/// granularities.
pub const TIME_BUCKET_BASE_NS: i64 = 60_000_000_000;

/// Magic for a serialized [`TokenBloom`] payload.
const BLOOM_MAGIC: [u8; 4] = *b"LKBF";
/// Magic for the per-row-group bloom LIST container.
const ROWGROUP_LIST_MAGIC: [u8; 4] = *b"LKBL";
/// On-disk version of both. Bump together with any change to the hash function,
/// the trigram definition, `k`/`m` sizing, or the bit layout — see
/// `docs/DESIGN_file_formats.md`.
const BLOOM_FORMAT_VERSION: u8 = 1;
/// magic + version + k.
const BLOOM_HEADER_LEN: usize = BLOOM_MAGIC.len() + 2;

/// Trigram window length (characters).
pub const TRIGRAM_LEN: usize = 3;

/// Minimum token length to index. Single/short tokens are too common to prune
/// usefully and inflate the bloom.
pub const MIN_TOKEN_LEN: usize = 3;

/// Cap on indexed tokens per row, to bound build cost on pathological lines.
pub const MAX_TOKENS_PER_ROW: usize = 64;

/// Canonical text tokenizers shared by blooms, inverted indexes, and query
/// analysis. `Default` is the legacy on-disk behavior and must stay byte-for-
/// byte compatible with existing bloom/index data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Tokenizer {
    Default,
    Raw,
    /// Dependency-free English "lite stemming": strips a small suffix set
    /// (`s`/`es`/`ed`/`ing`) with conservative guards, then reapplies the
    /// standard minimum-token-length filter.
    Stem,
}

impl Tokenizer {
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "default" => Some(Self::Default),
            "raw" => Some(Self::Raw),
            "stem" => Some(Self::Stem),
            _ => None,
        }
    }

    pub fn tokenize(&self, text: &str) -> Vec<String> {
        match self {
            Self::Default => default_tokenize(text),
            Self::Raw => vec![text.to_ascii_lowercase()],
            Self::Stem => default_tokenize(text)
                .into_iter()
                .filter_map(|token| lite_stem(&token))
                .collect(),
        }
    }
}

/// Tokenize a `raw` string into lowercased alphanumeric terms of at least
/// [`MIN_TOKEN_LEN`], yielding at most [`MAX_TOKENS_PER_ROW`] tokens in order.
/// Splitting on non-alphanumeric matches how single-word `LIKE '%term%'`
/// searches are issued against log text.
pub fn tokenize(raw: &str) -> Vec<String> {
    Tokenizer::Default.tokenize(raw)
}

fn default_tokenize(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    for piece in raw.split(|c: char| !c.is_ascii_alphanumeric()) {
        if piece.len() < MIN_TOKEN_LEN {
            continue;
        }
        out.push(piece.to_ascii_lowercase());
        if out.len() >= MAX_TOKENS_PER_ROW {
            break;
        }
    }
    out
}

fn lite_stem(token: &str) -> Option<String> {
    let stemmed = if let Some(stem) = strip_suffix_with_guard(token, "ing", 4) {
        stem
    } else if let Some(stem) = strip_suffix_with_guard(token, "ed", 3) {
        stem
    } else if let Some(stem) = strip_plural_es(token) {
        stem
    } else if let Some(stem) = strip_plural_s(token) {
        stem
    } else {
        token.to_string()
    };
    (stemmed.len() >= MIN_TOKEN_LEN).then_some(stemmed)
}

fn strip_suffix_with_guard(token: &str, suffix: &str, min_stem_len: usize) -> Option<String> {
    let stem = token.strip_suffix(suffix)?;
    let has_vowel = stem
        .bytes()
        .any(|b| matches!(b, b'a' | b'e' | b'i' | b'o' | b'u' | b'y'));
    (stem.len() >= min_stem_len && has_vowel).then(|| stem.to_string())
}

fn strip_plural_es(token: &str) -> Option<String> {
    let stem = token.strip_suffix("es")?;
    (stem.len() >= MIN_TOKEN_LEN && !stem.is_empty()).then(|| stem.to_string())
}

fn strip_plural_s(token: &str) -> Option<String> {
    let stem = token.strip_suffix('s')?;
    (!token.ends_with("ss") && stem.len() >= MIN_TOKEN_LEN).then(|| stem.to_string())
}

/// Character trigrams (3-char sliding windows) over the ASCII-lowercased `raw`
/// string, **including spaces and punctuation** so substrings that span token
/// boundaries (e.g. `"error 500"`) are indexed. This is the substring-search
/// generalization of [`tokenize`]: a query substring of length ≥ [`TRIGRAM_LEN`]
/// occurs in a row only if every one of its trigrams occurs in that row, so a
/// row group whose bloom lacks any of the query's trigrams cannot contain the
/// substring and is safely skipped (the bloom never false-negates). Duplicates
/// are returned in order; callers dedup when building.
pub fn trigrams(raw: &str) -> Vec<String> {
    let chars: Vec<char> = raw.chars().map(|c| c.to_ascii_lowercase()).collect();
    if chars.len() < TRIGRAM_LEN {
        return Vec::new();
    }
    chars
        .windows(TRIGRAM_LEN)
        .map(|w| w.iter().collect::<String>())
        .collect()
}

/// Accumulates the DISTINCT trigrams of many strings, reusing its buffers.
///
/// [`trigrams`] allocates a `Vec<char>`, a `Vec<String>` and one `String` per
/// gram for EVERY string — roughly one allocation per character — and a caller
/// building a set then discards nearly all of them as duplicates. The trigram
/// universe of log text is a few thousand entries, so a caller indexing a whole
/// row group saturates its set within the first few hundred rows and every
/// allocation after that is waste.
///
/// This type exists because that cost was found and fixed TWICE, four thousand
/// lines and one crate apart, and the second site went unfixed for two weeks
/// while running inside compaction's write stage. Measured 2026-08-14 at 11.9%
/// of drain cycle time; measured 2026-08-27 at 1.35-1.49x of whole compaction
/// merge wall. One implementation, one test, both call sites.
///
/// Equivalent to inserting every [`trigrams`] result into a set — pinned by
/// `trigram_set_matches_the_reference_implementation`.
#[derive(Debug, Default)]
pub struct TrigramSet {
    grams: std::collections::HashSet<Box<str>>,
    chars: Vec<char>,
    gram: String,
}

impl TrigramSet {
    pub fn new() -> Self {
        Self {
            grams: std::collections::HashSet::new(),
            chars: Vec::with_capacity(512),
            gram: String::with_capacity(8),
        }
    }

    /// Add every trigram of `value`. Allocates only for grams not seen before.
    pub fn add(&mut self, value: &str) {
        self.chars.clear();
        self.chars
            .extend(value.chars().map(|c| c.to_ascii_lowercase()));
        if self.chars.len() < TRIGRAM_LEN {
            return;
        }
        for w in self.chars.windows(TRIGRAM_LEN) {
            self.gram.clear();
            self.gram.extend(w.iter());
            // contains-then-insert: `insert` would allocate a Box<str> for the
            // probe and drop it on a hit, which is the whole cost being removed.
            if !self.grams.contains(self.gram.as_str()) {
                self.grams.insert(self.gram.as_str().into());
            }
        }
    }

    pub fn len(&self) -> usize {
        self.grams.len()
    }

    pub fn is_empty(&self) -> bool {
        self.grams.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.grams.iter().map(|g| &**g)
    }
}

/// The **distinct** trigrams a query substring must all match for a row to
/// contain it (mirrors [`trigrams`] normalization). `None` when the substring
/// is shorter than [`TRIGRAM_LEN`] — then the bloom cannot prune and the caller
/// must scan. Used by the reader to decompose a `LIKE '%substr%'` predicate.
pub fn query_trigrams(substr: &str) -> Option<Vec<String>> {
    let tris = trigrams(substr);
    if tris.is_empty() {
        return None;
    }
    let mut seen = std::collections::HashSet::new();
    Some(
        tris.into_iter()
            .filter(|t| seen.insert(t.clone()))
            .collect(),
    )
}

/// Normalize a query term the same way [`tokenize`] normalizes indexed tokens.
/// Returns `None` when the term is too short to have been indexed (callers must
/// then fall back to a full scan — the bloom can only prove absence of indexed
/// terms).
pub fn normalize_query_term(term: &str) -> Option<String> {
    let trimmed = term.trim();
    if trimmed.len() < MIN_TOKEN_LEN || !trimmed.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

/// A compact, serializable token bloom filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenBloom {
    bits: Vec<u8>,
    /// Number of 64-bit-derived hash probes per key.
    k: u8,
}

impl TokenBloom {
    /// Build a bloom sized for roughly `expected_tokens` distinct terms at the
    /// given false-positive probability, then insert every token.
    pub fn build<'a, I>(expected_tokens: usize, fpp: f64, tokens: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let n = expected_tokens.max(1);
        let fpp = fpp.clamp(1e-6, 0.5);
        // m = -n ln(p) / (ln2)^2 bits; k = (m/n) ln2.
        let ln2 = std::f64::consts::LN_2;
        let m_bits = (-(n as f64) * fpp.ln() / (ln2 * ln2)).ceil() as usize;
        let m_bits = m_bits.max(64);
        let bytes = m_bits.div_ceil(8);
        let k = (((bytes * 8) as f64 / n as f64) * ln2)
            .round()
            .clamp(1.0, 16.0) as u8;
        let mut bloom = TokenBloom {
            bits: vec![0u8; bytes],
            k,
        };
        for token in tokens {
            bloom.insert(token);
        }
        bloom
    }

    fn insert(&mut self, token: &str) {
        let nbits = (self.bits.len() * 8) as u64;
        let (h1, h2) = double_hash(token);
        for i in 0..self.k as u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % nbits;
            self.bits[(bit / 8) as usize] |= 1u8 << (bit % 8);
        }
    }

    /// Returns `false` only if the token is DEFINITELY not present (safe to
    /// prune); `true` means "maybe present" (must read). Never false-negatives.
    pub fn maybe_contains(&self, token: &str) -> bool {
        let nbits = (self.bits.len() * 8) as u64;
        let (h1, h2) = double_hash(token);
        for i in 0..self.k as u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % nbits;
            if self.bits[(bit / 8) as usize] & (1u8 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize as `[k][bits...]` for storage in file/row-group metadata.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bits.len() + BLOOM_HEADER_LEN);
        out.extend_from_slice(&BLOOM_MAGIC);
        out.push(BLOOM_FORMAT_VERSION);
        out.push(self.k);
        out.extend_from_slice(&self.bits);
        out
    }

    /// Inverse of [`TokenBloom::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        // Fail CLOSED. A bloom is consulted to SKIP data, so a payload we can't
        // positively identify must not be probed: guessing risks a false
        // negative, which silently drops rows from a query result. Returning
        // None makes the caller scan — slower, never wrong.
        let rest = bytes.strip_prefix(&BLOOM_MAGIC[..])?;
        let (&version, rest) = rest.split_first()?;
        if version != BLOOM_FORMAT_VERSION {
            return None;
        }
        let (&k, bits) = rest.split_first()?;
        if k == 0 || bits.is_empty() {
            return None;
        }
        Some(TokenBloom {
            bits: bits.to_vec(),
            k,
        })
    }

    /// Hex-encode the serialized bloom for storage in a (UTF-8-only) Parquet
    /// key-value metadata string.
    pub fn to_hex(&self) -> String {
        let bytes = self.to_bytes();
        let mut out = String::with_capacity(bytes.len() * 2);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in bytes {
            out.push(HEX[usize::from(byte >> 4)] as char);
            out.push(HEX[usize::from(byte & 0x0f)] as char);
        }
        out
    }

    /// Inverse of [`TokenBloom::to_hex`]. Returns `None` on malformed input.
    pub fn from_hex(s: &str) -> Option<Self> {
        let s = s.as_bytes();
        if !s.len().is_multiple_of(2) {
            return None;
        }
        fn nibble(c: u8) -> Option<u8> {
            match c {
                b'0'..=b'9' => Some(c - b'0'),
                b'a'..=b'f' => Some(c - b'a' + 10),
                b'A'..=b'F' => Some(c - b'A' + 10),
                _ => None,
            }
        }
        let mut bytes = Vec::with_capacity(s.len() / 2);
        for pair in s.as_chunks::<2>().0 {
            bytes.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
        }
        TokenBloom::from_bytes(&bytes)
    }
}

/// Serialize a per-row-group bloom list as
/// `[count u32-LE]( [len u32-LE][TokenBloom::to_bytes] )*`, then hex-encode for a
/// (UTF-8-only) Parquet KV string. Row-group order is preserved.
pub fn rowgroup_blooms_to_hex(blooms: &[TokenBloom]) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&ROWGROUP_LIST_MAGIC);
    bytes.push(BLOOM_FORMAT_VERSION);
    bytes.extend_from_slice(&(blooms.len() as u32).to_le_bytes());
    for bloom in blooms {
        let b = bloom.to_bytes();
        bytes.extend_from_slice(&(b.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&b);
    }
    let mut out = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in &bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    out
}

/// Inverse of [`rowgroup_blooms_to_hex`]. Returns `None` on malformed input.
pub fn rowgroup_blooms_from_hex(s: &str) -> Option<Vec<TokenBloom>> {
    let s = s.as_bytes();
    if !s.len().is_multiple_of(2) {
        return None;
    }
    fn nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for pair in s.as_chunks::<2>().0 {
        bytes.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }

    // Same fail-closed rule as `TokenBloom::from_bytes`: an unrecognized
    // container is ignored, never parsed on a guess.
    let rest = bytes.strip_prefix(&ROWGROUP_LIST_MAGIC[..])?;
    if *rest.first()? != BLOOM_FORMAT_VERSION {
        return None;
    }
    let bytes = rest[1..].to_vec();

    let mut pos = 0usize;
    let read_u32 = |bytes: &[u8], pos: &mut usize| -> Option<usize> {
        let end = pos.checked_add(4)?;
        let v = u32::from_le_bytes(bytes.get(*pos..end)?.try_into().ok()?);
        *pos = end;
        Some(v as usize)
    };
    let count = read_u32(&bytes, &mut pos)?;
    let mut blooms = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32(&bytes, &mut pos)?;
        let end = pos.checked_add(len)?;
        let chunk = bytes.get(pos..end)?;
        pos = end;
        blooms.push(TokenBloom::from_bytes(chunk)?);
    }
    Some(blooms)
}

fn fnv1a(data: &[u8], seed: u64) -> u64 {
    let mut hash = seed;
    for &byte in data {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn double_hash(token: &str) -> (u64, u64) {
    let bytes = token.as_bytes();
    let h1 = fnv1a(bytes, 0xcbf2_9ce4_8422_2325);
    // Second independent hash via a different seed; force odd so it is coprime
    // with the power-of-two-ish bit count and spreads probes.
    let h2 = fnv1a(bytes, 0x1000_0000_0000_01b3) | 1;
    (h1, h2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_splits_lowercases_and_filters_short() {
        let toks = tokenize("GET /api/v1 status=500 OK");
        // "api", "v1"(<3 dropped), "status", "500"(digits ok), "get", "ok"(<3)
        assert!(toks.contains(&"api".to_string()));
        assert!(toks.contains(&"status".to_string()));
        assert!(toks.contains(&"500".to_string()));
        assert!(toks.contains(&"get".to_string()));
        assert!(!toks.contains(&"v1".to_string())); // too short
        assert!(!toks.contains(&"ok".to_string())); // too short
    }

    #[test]
    fn normalize_query_term_rejects_short_and_nonword() {
        assert_eq!(normalize_query_term("Status"), Some("status".to_string()));
        assert_eq!(normalize_query_term("ab"), None);
        assert_eq!(normalize_query_term("a b"), None);
    }

    #[test]
    fn tokenizer_raw_keeps_the_whole_input_as_one_token() {
        assert_eq!(
            Tokenizer::Raw.tokenize("GET /Api/V1 status=500"),
            vec!["get /api/v1 status=500".to_string()]
        );
        assert_eq!(Tokenizer::Raw.tokenize(""), vec![String::new()]);
    }

    #[test]
    fn tokenizer_stem_strips_lite_english_suffixes() {
        assert_eq!(
            Tokenizer::Stem.tokenize("timeouts retried connecting classes"),
            vec![
                "timeout".to_string(),
                "retri".to_string(),
                "connect".to_string(),
                "class".to_string(),
            ]
        );
    }

    #[test]
    fn tokenizer_stem_guards_short_or_noninflected_roots() {
        assert_eq!(
            Tokenizer::Stem.tokenize("bring thing tuned red"),
            vec![
                "bring".to_string(),
                "thing".to_string(),
                "tun".to_string(),
                "red".to_string(),
            ]
        );
        assert_eq!(Tokenizer::Stem.tokenize("ping"), vec!["ping".to_string()]);
        assert_eq!(Tokenizer::Stem.tokenize("sing"), vec!["sing".to_string()]);
    }

    #[test]
    fn bloom_no_false_negatives_and_prunes_absent() {
        let tokens = ["status", "500", "nginx", "access", "error", "timeout"];
        let bloom = TokenBloom::build(tokens.len(), 0.01, tokens.iter().copied());
        for t in tokens {
            assert!(bloom.maybe_contains(t), "false negative for {t}");
        }
        // A token never inserted should usually be pruned. Check a few; bloom
        // guarantees no false negatives but allows rare false positives.
        let absent = ["zzzzzzzz", "qwertyui", "absent_term"];
        let pruned = absent.iter().filter(|t| !bloom.maybe_contains(t)).count();
        assert!(pruned >= 1, "expected to prune at least one absent token");
    }

    #[test]
    fn bloom_roundtrips_through_bytes() {
        let tokens = ["status", "nginx", "timeout"];
        let bloom = TokenBloom::build(tokens.len(), 0.01, tokens.iter().copied());
        let restored = TokenBloom::from_bytes(&bloom.to_bytes()).unwrap();
        assert_eq!(bloom, restored);
        for t in tokens {
            assert!(restored.maybe_contains(t));
        }
    }

    #[test]
    fn bloom_roundtrips_through_hex() {
        // The write side hex-encodes into Parquet KV metadata; the read side
        // decodes. They MUST agree exactly or queries would drop matching rows.
        let tokens = ["status", "nginx", "timeout", "12345"];
        let bloom = TokenBloom::build(tokens.len(), 0.01, tokens.iter().copied());
        let hex = bloom.to_hex();
        let restored = TokenBloom::from_hex(&hex).unwrap();
        assert_eq!(bloom, restored);
        for t in tokens {
            assert!(restored.maybe_contains(t), "lost token {t} through hex");
        }
        assert_eq!(TokenBloom::from_hex("xyz"), None);
        assert_eq!(TokenBloom::from_hex("0"), None);
    }

    #[test]
    fn rowgroup_bloom_list_roundtrips_and_preserves_order() {
        // Distinct content per row group; the list must round-trip in order so
        // the reader checks the right bloom for each row group.
        let rg0 = TokenBloom::build(3, 0.01, ["alpha", "bravo", "charlie"]);
        let rg1 = TokenBloom::build(2, 0.01, ["delta", "echo"]);
        let rg2 = TokenBloom::build(1, 0.01, ["foxtrot"]);
        let list = vec![rg0.clone(), rg1.clone(), rg2.clone()];

        let restored = rowgroup_blooms_from_hex(&rowgroup_blooms_to_hex(&list)).unwrap();
        assert_eq!(restored, list);

        // Per-row-group membership must hold (no false negatives) and the order
        // must be preserved so pruning targets the correct row group.
        assert!(restored[0].maybe_contains("alpha"));
        assert!(restored[1].maybe_contains("delta"));
        assert!(restored[2].maybe_contains("foxtrot"));

        assert_eq!(
            rowgroup_blooms_from_hex(&rowgroup_blooms_to_hex(&[])),
            Some(vec![])
        );
        assert_eq!(rowgroup_blooms_from_hex("zz"), None);
        assert_eq!(rowgroup_blooms_from_hex("00"), None); // truncated count
    }
}

#[cfg(test)]
mod trigram_tests {
    use super::*;

    /// [`TrigramSet`] must produce exactly what inserting every [`trigrams`]
    /// result into a set produces.
    ///
    /// It is a performance rewrite of an INDEX, and it now backs both the drain
    /// path's file bloom and the compaction writer's row-group bloom. A bloom
    /// that differs by one bit prunes a row group holding matching rows and the
    /// reader returns nothing — rows silently missing from a substring search,
    /// no error anywhere. That is the one artifact class where being wrong is
    /// invisible, so the equivalence is pinned rather than argued.
    #[test]
    fn trigram_set_matches_the_reference_implementation() {
        // Adversarial on purpose: repeated lines so the dedup path dominates as
        // it does after a row group's first few hundred rows; mixed ASCII case
        // that must fold; multi-byte characters so char-vs-byte windowing is
        // exercised; and rows at and below TRIGRAM_LEN.
        let corpus: Vec<&str> = [
            "GET /api/v1/logs 200 in 13ms service=ingest",
            "get /API/v1/logs 200 in 13ms service=ingest",
            "POST /v1/logs 503 in 4210ms service=drain",
            "abc",
            "ab",
            "a",
            "",
            "   ",
            "naïve café — ünïcode ✓ 日本語",
            "😀 emoji lead",
        ]
        .into_iter()
        .cycle()
        .take(300)
        .collect();

        let mut want: std::collections::HashSet<String> = std::collections::HashSet::new();
        for v in &corpus {
            for g in trigrams(v) {
                want.insert(g);
            }
        }

        let mut set = TrigramSet::new();
        for v in &corpus {
            set.add(v);
        }

        assert!(
            !want.is_empty(),
            "fixture produced no grams — it tests nothing"
        );
        assert_eq!(set.len(), want.len(), "distinct gram COUNT differs");
        let got: std::collections::HashSet<String> = set.iter().map(str::to_string).collect();
        assert_eq!(
            got, want,
            "TrigramSet produced a different gram set than trigrams()"
        );

        // And the bloom bytes, which is what actually ships in the footer:
        // `build` sizes from the count and ORs bits per token, so equal sets
        // must give equal bytes. Asserting on the BYTES is what makes this a
        // claim about the index rather than about a HashSet.
        let mut a: Vec<&str> = want.iter().map(String::as_str).collect();
        let mut b: Vec<&str> = set.iter().collect();
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(
            TokenBloom::build(want.len(), 0.01, a.into_iter()).to_hex(),
            TokenBloom::build(set.len(), 0.01, b.into_iter()).to_hex(),
            "same gram set produced a different bloom"
        );
    }

    #[test]
    fn trigrams_slide_over_full_string_incl_separators() {
        // Spaces/punctuation are included so cross-token substrings index.
        let t = trigrams("ab cd");
        assert_eq!(t, vec!["ab ", "b c", " cd"]);
        // ASCII-lowercased.
        assert_eq!(trigrams("ABC"), vec!["abc"]);
        // Too short → none.
        assert!(trigrams("ab").is_empty());
    }

    #[test]
    fn substring_pruning_is_sound() {
        // A bloom of a row group's trigrams must report every trigram of any
        // substring it actually contains; a substring it lacks must be
        // detectable by a missing trigram.
        let text = "connection refused error 500 on host-7";
        let mut tris = std::collections::HashSet::new();
        for t in trigrams(text) {
            tris.insert(t);
        }
        let bloom = TokenBloom::build(tris.len(), 0.01, tris.iter().map(|s| s.as_str()));

        // A real cross-token substring: all trigrams present.
        for t in query_trigrams("refused error").unwrap() {
            assert!(bloom.maybe_contains(&t), "present trigram missing: {t}");
        }
        // A partial-token substring ("rror" ⊂ "error"): present.
        for t in query_trigrams("rror").unwrap() {
            assert!(
                bloom.maybe_contains(&t),
                "partial-token trigram missing: {t}"
            );
        }
        // An absent substring: at least one trigram must be missing (the bloom
        // can false-positive but the constructing text genuinely lacks "zzz").
        let absent = query_trigrams("zzqx").unwrap();
        assert!(
            absent.iter().any(|t| !bloom.maybe_contains(t)),
            "absent substring should miss at least one trigram"
        );
    }

    #[test]
    fn query_trigrams_distinct_and_short_none() {
        assert!(query_trigrams("ab").is_none());
        // "aaaa" → {"aaa"} distinct.
        assert_eq!(query_trigrams("aaaa").unwrap(), vec!["aaa"]);
    }
}

#[cfg(test)]
mod proptests {
    //! Property tests for the bloom invariants the read-path pruning relies on.
    //! The load-bearing one is **no false negatives**: a bloom may say "maybe
    //! present" for something absent (false positive — costs a scan), but it must
    //! NEVER say "absent" for something present, or the reader would skip a file
    //! / row group / page that actually matches the query and silently lose rows.
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Every token that was inserted must be reported present. (Token-search
        /// soundness: a row containing an indexed term is never pruned.)
        #[test]
        fn no_false_negatives_for_inserted_tokens(raw in "[a-zA-Z0-9 _/=.-]{0,200}") {
            let tokens = tokenize(&raw);
            if tokens.is_empty() {
                return Ok(());
            }
            let distinct: std::collections::HashSet<&str> =
                tokens.iter().map(|s| s.as_str()).collect();
            let bloom = TokenBloom::build(distinct.len(), 0.01, distinct.iter().copied());
            for t in &distinct {
                prop_assert!(bloom.maybe_contains(t), "false negative for inserted token {t:?}");
            }
        }

        /// **Substring-search soundness.** For any `q` that is a contiguous
        /// substring of `raw`, the bloom built from `raw`'s trigrams must report
        /// every trigram of `q` present — so a `raw LIKE '%q%'` row is never
        /// pruned. (ASCII-restricted so char index == byte index for slicing.)
        #[test]
        fn substring_search_never_prunes_a_matching_row(
            raw in "[a-z0-9 ]{3,80}",
            a in 0usize..80,
            b in 0usize..80,
        ) {
            let tris = trigrams(&raw);
            if tris.is_empty() {
                return Ok(());
            }
            let distinct: std::collections::HashSet<&str> =
                tris.iter().map(|s| s.as_str()).collect();
            let bloom = TokenBloom::build(distinct.len(), 0.01, distinct.iter().copied());

            // Derive a real substring of `raw` from the two random offsets.
            let n = raw.len();
            let start = a % n;
            let end = (start + (b % (n - start)) + 1).min(n);
            let q = &raw[start..end];
            if let Some(qt) = query_trigrams(q) {
                for t in &qt {
                    prop_assert!(
                        bloom.maybe_contains(t),
                        "false negative: substring {q:?} of {raw:?} missing trigram {t:?}",
                    );
                }
            }
        }

        /// `from_bytes ∘ to_bytes` and `from_hex ∘ to_hex` are the identity on a
        /// built bloom (encode/decode round-trip — guards the on-disk format).
        #[test]
        fn token_bloom_roundtrips(raw in "[a-zA-Z0-9 ]{0,200}") {
            let tokens = tokenize(&raw);
            let distinct: std::collections::HashSet<&str> =
                tokens.iter().map(|s| s.as_str()).collect();
            let bloom = TokenBloom::build(distinct.len().max(1), 0.01, distinct.iter().copied());
            let from_bytes = TokenBloom::from_bytes(&bloom.to_bytes());
            let from_hex = TokenBloom::from_hex(&bloom.to_hex());
            prop_assert_eq!(from_bytes.as_ref(), Some(&bloom));
            prop_assert_eq!(from_hex.as_ref(), Some(&bloom));
        }

        /// A vector of per-row-group blooms survives the hex container round-trip.
        #[test]
        fn rowgroup_blooms_hex_roundtrip(raws in proptest::collection::vec("[a-z0-9 ]{0,60}", 0..6)) {
            let blooms: Vec<TokenBloom> = raws
                .iter()
                .map(|r| {
                    let tris = trigrams(r);
                    let distinct: std::collections::HashSet<&str> =
                        tris.iter().map(|s| s.as_str()).collect();
                    TokenBloom::build(distinct.len().max(1), 0.01, distinct.iter().copied())
                })
                .collect();
            let hex = rowgroup_blooms_to_hex(&blooms);
            prop_assert_eq!(rowgroup_blooms_from_hex(&hex), Some(blooms));
        }
    }
}
