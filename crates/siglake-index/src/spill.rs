//! Disk-backed construction of the unchanged KIDX v1 format.
//!
//! Runs have local row ordinals. Hierarchical merges add checked contiguous
//! bases while streaming each posting, and retain at most sixteen term heads.
//! No complete-file posting list or serialized index is collected in memory.
//! The memory budget bounds conservatively charged index work, not Arrow,
//! allocator RSS, OS page cache, or the caller. Oversized rows fail explicitly.
//! Scratch counts every live run/body/output byte, including merge overlap.
//! Tempfiles use the process temporary directory (which must be disk-backed
//! for a memory-constrained caller); they are private and removed on drop.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::{Arc, Mutex};

use siglake_bloom::{Tokenizer, MAX_TOKENS_PER_ROW};
use tempfile::NamedTempFile;

use crate::{IndexBuilder, INDEX_MAGIC, INDEX_VERSION};

const FAN_IN: usize = 16;
const MIN_MEMORY: usize = 64 * 1024;
const TERM_OVERHEAD: usize = 256;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[derive(Debug)]
struct ScratchBudget {
    used: u64,
    limit: u64,
}

struct ScratchFile {
    file: NamedTempFile,
    budget: Arc<Mutex<ScratchBudget>>,
    reserved: u64,
    len: u64,
    checksum: crc32fast::Hasher,
}

impl ScratchFile {
    fn new(budget: Arc<Mutex<ScratchBudget>>) -> io::Result<Self> {
        Ok(Self {
            file: NamedTempFile::new()?,
            budget,
            reserved: 0,
            len: 0,
            checksum: crc32fast::Hasher::new(),
        })
    }

    fn reopen(&self) -> io::Result<File> {
        if self.file.as_file().metadata()?.len() != self.len {
            return Err(invalid("spill length changed"));
        }
        // reopen creates an independent cursor; try_clone shares the offset.
        self.file.reopen()
    }
}

impl Write for ScratchFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let offered = u64::try_from(bytes.len()).map_err(|_| invalid("spill length overflow"))?;
        {
            let mut budget = self
                .budget
                .lock()
                .map_err(|_| invalid("spill budget poisoned"))?;
            let next = budget
                .used
                .checked_add(offered)
                .ok_or_else(|| invalid("spill size overflow"))?;
            if next > budget.limit {
                return Err(io::Error::other("index scratch limit exceeded"));
            }
            budget.used = next;
            self.reserved += offered;
        }
        // On an ambiguous write error retain the reservation until RAII drop.
        let count = self.file.as_file_mut().write(bytes)?;
        self.checksum.update(&bytes[..count]);
        let unused = offered - count as u64;
        self.reserved -= unused;
        self.budget
            .lock()
            .map_err(|_| invalid("spill budget poisoned"))?
            .used -= unused;
        self.len = self
            .len
            .checked_add(count as u64)
            .ok_or_else(|| invalid("spill length overflow"))?;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.as_file_mut().flush()
    }
}

impl Drop for ScratchFile {
    fn drop(&mut self) {
        if let Ok(mut budget) = self.budget.lock() {
            budget.used -= self.reserved;
        }
    }
}

/// Owns the complete raw v1 bytes on private scratch storage.
pub struct SpilledIndex {
    raw: Arc<ScratchFile>,
    n_rows: u32,
    n_terms: u64,
}

impl SpilledIndex {
    pub fn n_rows(&self) -> u32 {
        self.n_rows
    }
    pub fn n_terms(&self) -> u64 {
        self.n_terms
    }
    pub fn raw_len(&self) -> u64 {
        self.raw.len
    }
    pub fn reopen_reader(&self) -> io::Result<SpilledReader> {
        Ok(SpilledReader {
            file: self.raw.reopen()?,
            owner: Arc::clone(&self.raw),
            checksum: crc32fast::Hasher::new(),
            consumed: 0,
        })
    }
}

/// Independent sequential cursor retaining the tempfile and scratch reservation.
/// Length and CRC32 are verified at EOF; callers must consume through EOF.
pub struct SpilledReader {
    file: File,
    owner: Arc<ScratchFile>,
    checksum: crc32fast::Hasher,
    consumed: u64,
}

impl Read for SpilledReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let count = self.file.read(bytes)?;
        self.consumed = self
            .consumed
            .checked_add(count as u64)
            .ok_or_else(|| invalid("spill read overflow"))?;
        self.checksum.update(&bytes[..count]);
        if self.consumed > self.owner.len
            || (count == 0
                && (self.consumed != self.owner.len
                    || self.checksum.clone().finalize() != self.owner.checksum.clone().finalize()))
        {
            return Err(invalid("spill byte length or checksum mismatch"));
        }
        Ok(count)
    }
}

/// Fallible bounded construction; an error permanently poisons this builder.
pub struct SpillingIndexBuilder {
    tokenizer: Tokenizer,
    memory: usize,
    charge: usize,
    rows: u32,
    current: IndexBuilder,
    levels: Vec<Vec<SpilledIndex>>,
    budget: Arc<Mutex<ScratchBudget>>,
    poisoned: bool,
}

impl SpillingIndexBuilder {
    pub fn new(
        tokenizer: Tokenizer,
        memory_budget_bytes: usize,
        scratch_limit_bytes: u64,
    ) -> io::Result<Self> {
        if memory_budget_bytes < MIN_MEMORY || scratch_limit_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid index spill budgets",
            ));
        }
        Ok(Self {
            tokenizer,
            memory: memory_budget_bytes,
            charge: 0,
            rows: 0,
            current: IndexBuilder::with_tokenizer(tokenizer),
            levels: Vec::new(),
            budget: Arc::new(Mutex::new(ScratchBudget {
                used: 0,
                limit: scratch_limit_bytes,
            })),
            poisoned: false,
        })
    }

    pub fn push_row(&mut self, raw: &str) -> io::Result<()> {
        if self.poisoned {
            return Err(invalid("index spill builder poisoned"));
        }
        let result = self.push_inner(raw);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn push_inner(&mut self, raw: &str) -> io::Result<()> {
        self.rows
            .checked_add(1)
            .ok_or_else(|| invalid("v1 row domain overflow"))?;
        // Bound the input before tokenizer allocation. Every tokenizer emits
        // at most 64 strings whose total bytes do not exceed raw.len(). The
        // same limit also bounds each merge head (16 heads, two during swap).
        let max_term = self.memory / (FAN_IN * 4);
        let precharge = raw
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(MAX_TOKENS_PER_ROW * TERM_OVERHEAD))
            .ok_or_else(|| invalid("row memory charge overflow"))?;
        if raw.len() > max_term || precharge > self.memory / 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "single index row exceeds spill memory budget",
            ));
        }
        let mut tokens = self.tokenizer.tokenize(raw);
        tokens.sort();
        tokens.dedup();
        let mut charge = 16usize; // Counts empty rows too, keeping run length bounded.
        for term in &tokens {
            charge = charge
                .checked_add(
                    term.len()
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(TERM_OVERHEAD + 8))
                        .ok_or_else(|| invalid("term memory charge overflow"))?,
                )
                .ok_or_else(|| invalid("run memory charge overflow"))?;
        }
        if self
            .charge
            .checked_add(charge)
            .ok_or_else(|| invalid("run charge overflow"))?
            > self.memory / 2
        {
            self.flush_run()?;
        }
        if charge > self.memory / 2 {
            return Err(invalid("row charge exceeds run budget"));
        }
        let ordinal = self.current.n_rows;
        for token in tokens {
            self.current
                .postings
                .entry(token)
                .or_default()
                .push(ordinal);
        }
        self.current.n_rows += 1;
        self.rows += 1;
        self.charge += charge;
        Ok(())
    }

    fn buffer_size(&self) -> usize {
        (self.memory / (FAN_IN * 16)).min(8192)
    }

    fn flush_run(&mut self) -> io::Result<()> {
        if self.current.n_rows == 0 {
            return Ok(());
        }
        let index = std::mem::replace(
            &mut self.current,
            IndexBuilder::with_tokenizer(self.tokenizer),
        )
        .build();
        self.charge = 0;
        let mut raw = ScratchFile::new(Arc::clone(&self.budget))?;
        {
            let mut out = BufWriter::with_capacity(self.buffer_size(), &mut raw);
            header(&mut out, index.n_rows, index.postings.len() as u64)?;
            for (term, postings) in &index.postings {
                varint(&mut out, term.len() as u64)?;
                out.write_all(term.as_bytes())?;
                varint(&mut out, postings.len() as u64)?;
                let mut previous = 0;
                for &row in postings {
                    varint(&mut out, u64::from(row - previous))?;
                    previous = row;
                }
            }
            out.flush()?;
        }
        let run = SpilledIndex {
            raw: Arc::new(raw),
            n_rows: index.n_rows,
            n_terms: index.postings.len() as u64,
        };
        drop(index);
        self.place(run, 0)
    }

    fn place(&mut self, mut run: SpilledIndex, mut level: usize) -> io::Result<()> {
        loop {
            if level > 8 {
                return Err(invalid("too many v1 spill levels"));
            }
            if self.levels.len() <= level {
                self.levels.push(Vec::new());
            }
            self.levels[level].push(run);
            if self.levels[level].len() < FAN_IN {
                return Ok(());
            }
            let inputs = std::mem::take(&mut self.levels[level]);
            run = merge(inputs, self.memory, Arc::clone(&self.budget))?;
            level += 1;
        }
    }

    pub fn finish(mut self) -> io::Result<SpilledIndex> {
        if self.poisoned {
            return Err(invalid("index spill builder poisoned"));
        }
        self.flush_run()?;
        // Higher levels contain the oldest contiguous ranges.
        let mut runs: Vec<_> = self.levels.drain(..).rev().flatten().collect();
        if runs.is_empty() {
            let mut raw = ScratchFile::new(Arc::clone(&self.budget))?;
            header(&mut raw, 0, 0)?;
            return Ok(SpilledIndex {
                raw: Arc::new(raw),
                n_rows: 0,
                n_terms: 0,
            });
        }
        while runs.len() > 1 {
            let count = runs.len().min(FAN_IN);
            let inputs = runs.drain(..count).collect();
            let merged = merge(inputs, self.memory, Arc::clone(&self.budget))?;
            runs.insert(0, merged);
        }
        let result = runs.pop().ok_or_else(|| invalid("missing final spill"))?;
        if result.n_rows != self.rows {
            return Err(invalid("spill lost row ordinals"));
        }
        Ok(result)
    }
}

fn header(out: &mut impl Write, rows: u32, terms: u64) -> io::Result<()> {
    out.write_all(INDEX_MAGIC)?;
    out.write_all(&[INDEX_VERSION])?;
    varint(out, u64::from(rows))?;
    varint(out, terms)
}

fn varint(out: &mut impl Write, mut value: u64) -> io::Result<()> {
    let mut bytes = [0u8; 10];
    let mut n = 0;
    while value >= 128 {
        bytes[n] = (value as u8 & 127) | 128;
        value >>= 7;
        n += 1;
    }
    bytes[n] = value as u8;
    out.write_all(&bytes[..=n])
}

fn read_varint(input: &mut impl Read) -> io::Result<u64> {
    let mut result = 0u64;
    for index in 0..10 {
        let mut byte = [0u8];
        input.read_exact(&mut byte)?;
        if index == 9 && byte[0] > 1 {
            return Err(invalid("spill varint overflow"));
        }
        result |= u64::from(byte[0] & 127) << (index * 7);
        if byte[0] & 128 == 0 {
            if index > 0 && byte[0] == 0 {
                return Err(invalid("noncanonical spill varint"));
            }
            return Ok(result);
        }
    }
    Err(invalid("unterminated spill varint"))
}

struct RunReader {
    input: BufReader<SpilledReader>,
    rows: u32,
    terms_left: u64,
    term: Option<Vec<u8>>,
    postings: u64,
    remaining: u64,
    previous: u32,
    max_term: usize,
}

impl RunReader {
    fn open(run: &SpilledIndex, memory: usize) -> io::Result<Self> {
        let mut input =
            BufReader::with_capacity((memory / (FAN_IN * 16)).min(8192), run.reopen_reader()?);
        let mut magic = [0u8; 5];
        input.read_exact(&mut magic)?;
        if &magic[..4] != INDEX_MAGIC || magic[4] != INDEX_VERSION {
            return Err(invalid("invalid spill magic/version"));
        }
        let rows =
            u32::try_from(read_varint(&mut input)?).map_err(|_| invalid("spill row overflow"))?;
        let terms = read_varint(&mut input)?;
        if rows != run.n_rows || terms != run.n_terms {
            return Err(invalid("spill header mismatch"));
        }
        let mut result = Self {
            input,
            rows,
            terms_left: terms,
            term: None,
            postings: 0,
            remaining: 0,
            previous: 0,
            max_term: memory / (FAN_IN * 4),
        };
        result.advance()?;
        Ok(result)
    }

    fn advance(&mut self) -> io::Result<()> {
        if self.remaining != 0 {
            return Err(invalid("unconsumed spill postings"));
        }
        let prior = self.term.take();
        if self.terms_left == 0 {
            let mut byte = [0];
            if self.input.read(&mut byte)? != 0 {
                return Err(invalid("trailing spill bytes"));
            }
            return Ok(());
        }
        let length = usize::try_from(read_varint(&mut self.input)?)
            .map_err(|_| invalid("spill term length overflow"))?;
        if length > self.max_term {
            return Err(invalid("oversized spill term"));
        }
        let mut term = vec![0; length];
        self.input.read_exact(&mut term)?;
        std::str::from_utf8(&term).map_err(|_| invalid("invalid spill term UTF-8"))?;
        if prior.as_ref().is_some_and(|old| old >= &term) {
            return Err(invalid("unordered spill terms"));
        }
        let count = read_varint(&mut self.input)?;
        if count == 0 || count > u64::from(self.rows) {
            return Err(invalid("invalid spill posting count"));
        }
        self.postings = count;
        self.remaining = count;
        self.previous = 0;
        self.term = Some(term);
        self.terms_left -= 1;
        Ok(())
    }

    fn posting(&mut self) -> io::Result<u32> {
        if self.remaining == 0 {
            return Err(invalid("missing spill posting"));
        }
        let delta = u32::try_from(read_varint(&mut self.input)?)
            .map_err(|_| invalid("posting delta overflow"))?;
        if self.remaining != self.postings && delta == 0 {
            return Err(invalid("duplicate spill posting"));
        }
        let row = self
            .previous
            .checked_add(delta)
            .ok_or_else(|| invalid("posting ordinal overflow"))?;
        if row >= self.rows {
            return Err(invalid("posting outside spill row domain"));
        }
        self.previous = row;
        self.remaining -= 1;
        Ok(row)
    }
}

fn merge(
    inputs: Vec<SpilledIndex>,
    memory: usize,
    budget: Arc<Mutex<ScratchBudget>>,
) -> io::Result<SpilledIndex> {
    if inputs.is_empty() || inputs.len() > FAN_IN {
        return Err(invalid("invalid spill fan-in"));
    }
    let mut bases = Vec::with_capacity(inputs.len());
    let mut rows = 0u32;
    let mut readers = Vec::with_capacity(inputs.len());
    for run in &inputs {
        bases.push(rows);
        rows = rows
            .checked_add(run.n_rows)
            .ok_or_else(|| invalid("merged row domain overflow"))?;
        readers.push(RunReader::open(run, memory)?);
    }
    let buffer = (memory / (FAN_IN * 16)).min(8192);
    let mut body = ScratchFile::new(Arc::clone(&budget))?;
    let mut terms = 0u64;
    {
        let mut out = BufWriter::with_capacity(buffer, &mut body);
        loop {
            let Some(term) = readers
                .iter()
                .filter_map(|reader| reader.term.as_ref())
                .min()
                .cloned()
            else {
                break;
            };
            let mut count = 0u64;
            for reader in &readers {
                if reader.term.as_ref() == Some(&term) {
                    count = count
                        .checked_add(reader.postings)
                        .ok_or_else(|| invalid("merged posting count overflow"))?;
                }
            }
            varint(&mut out, term.len() as u64)?;
            out.write_all(&term)?;
            varint(&mut out, count)?;
            let mut previous = 0u32;
            let mut emitted = 0u64;
            for (reader, base) in readers.iter_mut().zip(&bases) {
                if reader.term.as_ref() != Some(&term) {
                    continue;
                }
                while reader.remaining > 0 {
                    let row = base
                        .checked_add(reader.posting()?)
                        .ok_or_else(|| invalid("global posting overflow"))?;
                    if emitted != 0 && row <= previous {
                        return Err(invalid("unordered merged postings"));
                    }
                    varint(&mut out, u64::from(row - previous))?;
                    previous = row;
                    emitted += 1;
                }
                reader.advance()?;
            }
            if emitted != count {
                return Err(invalid("merged posting count mismatch"));
            }
            terms = terms
                .checked_add(1)
                .ok_or_else(|| invalid("merged term overflow"))?;
        }
        out.flush()?;
    }
    drop(readers);
    drop(inputs);
    let mut raw = ScratchFile::new(budget)?;
    {
        let mut out = BufWriter::with_capacity(buffer, &mut raw);
        header(&mut out, rows, terms)?;
        let mut source = body.reopen()?;
        let mut bytes = vec![0u8; buffer];
        let mut copied = 0u64;
        let mut checksum = crc32fast::Hasher::new();
        loop {
            let n = source.read(&mut bytes)?;
            if n == 0 {
                break;
            }
            out.write_all(&bytes[..n])?;
            checksum.update(&bytes[..n]);
            copied += n as u64;
        }
        if copied != body.len || checksum.finalize() != body.checksum.clone().finalize() {
            return Err(invalid("truncated spill body"));
        }
        out.flush()?;
    }
    Ok(SpilledIndex {
        raw: Arc::new(raw),
        n_rows: rows,
        n_terms: terms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InvertedIndex;

    fn build(rows: &[String], tokenizer: Tokenizer, force_runs: bool) -> SpilledIndex {
        let mut builder =
            SpillingIndexBuilder::new(tokenizer, MIN_MEMORY, 32 * 1024 * 1024).unwrap();
        for row in rows {
            builder.push_row(row).unwrap();
            if force_runs {
                builder.flush_run().unwrap();
            }
        }
        builder.finish().unwrap()
    }

    fn bytes(index: &SpilledIndex) -> Vec<u8> {
        let mut bytes = Vec::new();
        index
            .reopen_reader()
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        bytes
    }

    fn budget(limit: u64) -> Arc<Mutex<ScratchBudget>> {
        Arc::new(Mutex::new(ScratchBudget { used: 0, limit }))
    }

    fn raw_run(
        bytes: &[u8],
        rows: u32,
        terms: u64,
        budget: Arc<Mutex<ScratchBudget>>,
    ) -> SpilledIndex {
        let mut raw = ScratchFile::new(budget).unwrap();
        raw.write_all(bytes).unwrap();
        SpilledIndex {
            raw: Arc::new(raw),
            n_rows: rows,
            n_terms: terms,
        }
    }

    fn malformed(bytes: &[u8], rows: u32, terms: u64) {
        let budget = budget(1024 * 1024);
        let run = raw_run(bytes, rows, terms, Arc::clone(&budget));
        assert!(merge(vec![run], MIN_MEMORY, Arc::clone(&budget)).is_err());
        assert_eq!(budget.lock().unwrap().used, 0);
    }

    #[test]
    fn identical_bytes_all_tokenizers_empty_null_unicode_and_capped_tokens() {
        let rows = vec![
            "".into(),
            " ".into(),
            "a ab abc ABC abc".into(),
            "ÄBC 世界 CAFÉ running runs classes".into(),
            "\0\u{10ffff} maximum\nrow".into(),
            (0..100).map(|n| format!("token{n} ")).collect::<String>(),
        ];
        for tokenizer in [Tokenizer::Default, Tokenizer::Raw, Tokenizer::Stem] {
            let index = build(&rows, tokenizer, true);
            let expected =
                InvertedIndex::from_rows_with_tokenizer(rows.iter().map(String::as_str), tokenizer);
            assert_eq!(bytes(&index), expected.to_bytes());
            assert_eq!(index.n_rows(), rows.len() as u32);
            assert_eq!(index.n_terms(), expected.n_terms() as u64);
        }
    }

    #[test]
    fn byte_equivalence_across_multiple_merge_levels_and_empty_runs() {
        let rows: Vec<String> = (0..FAN_IN * FAN_IN + 37)
            .map(|i| {
                if i % 7 == 0 {
                    String::new()
                } else {
                    format!("shared term{} row{} shared", i % 19, i)
                }
            })
            .collect();
        let spilled = build(&rows, Tokenizer::Default, true);
        assert_eq!(
            bytes(&spilled),
            InvertedIndex::from_rows(rows.iter().map(String::as_str)).to_bytes()
        );
    }

    #[test]
    fn automatic_budget_flush_matches_reference_with_deterministic_random_rows() {
        let mut state = 9u64;
        let rows: Vec<_> = (0..4000)
            .map(|i| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                format!(
                    "field{} field{} term{} item{}",
                    state % 17,
                    i % 5,
                    state % 73,
                    i
                )
            })
            .collect();
        for tokenizer in [Tokenizer::Default, Tokenizer::Raw, Tokenizer::Stem] {
            let index = build(&rows, tokenizer, false);
            assert_eq!(
                bytes(&index),
                InvertedIndex::from_rows_with_tokenizer(rows.iter().map(String::as_str), tokenizer)
                    .to_bytes()
            );
        }
    }

    #[test]
    fn empty_file_and_all_empty_rows_preserve_row_domain() {
        let empty = build(&[], Tokenizer::Default, false);
        assert_eq!(bytes(&empty), InvertedIndex::default().to_bytes());
        let rows = vec![String::new(); 33];
        let index = build(&rows, Tokenizer::Default, true);
        assert_eq!(index.n_rows(), 33);
        assert_eq!(index.n_terms(), 0);
        assert_eq!(
            bytes(&index),
            InvertedIndex::from_rows(rows.iter().map(String::as_str)).to_bytes()
        );
    }

    #[test]
    fn oversize_row_is_explicit_and_poisoned_not_truncated() {
        let mut builder =
            SpillingIndexBuilder::new(Tokenizer::Raw, MIN_MEMORY, 1024 * 1024).unwrap();
        builder.push_row("before").unwrap();
        assert!(builder
            .push_row(&"x".repeat(MIN_MEMORY / (FAN_IN * 4) + 1))
            .is_err());
        assert!(builder.push_row("after").is_err());
        assert!(builder.finish().is_err());
    }

    #[test]
    fn exact_row_size_boundary_is_retained() {
        let rows = vec!["X".repeat(MIN_MEMORY / (FAN_IN * 4))];
        let spilled = build(&rows, Tokenizer::Raw, true);
        assert_eq!(
            bytes(&spilled),
            InvertedIndex::from_rows_with_tokenizer(
                rows.iter().map(String::as_str),
                Tokenizer::Raw
            )
            .to_bytes()
        );
    }

    #[test]
    fn row_and_merge_domains_check_u32_overflow() {
        let mut builder = SpillingIndexBuilder::new(Tokenizer::Default, MIN_MEMORY, 1024).unwrap();
        builder.rows = u32::MAX;
        assert!(builder.push_row("").is_err());
        let budget = budget(1024);
        let mut a = Vec::new();
        header(&mut a, u32::MAX, 0).unwrap();
        let mut b = Vec::new();
        header(&mut b, 1, 0).unwrap();
        let inputs = vec![
            raw_run(&a, u32::MAX, 0, Arc::clone(&budget)),
            raw_run(&b, 1, 0, Arc::clone(&budget)),
        ];
        assert!(merge(inputs, MIN_MEMORY, Arc::clone(&budget)).is_err());
        assert_eq!(budget.lock().unwrap().used, 0);
    }

    #[test]
    fn scratch_limit_includes_final_raw_bytes_and_all_overlap() {
        let mut builder = SpillingIndexBuilder::new(Tokenizer::Default, MIN_MEMORY, 1).unwrap();
        builder.push_row("needle").unwrap();
        let budget = Arc::clone(&builder.budget);
        assert!(builder.finish().is_err());
        assert_eq!(budget.lock().unwrap().used, 0);
        let index = build(&["needle haystack".into()], Tokenizer::Default, false);
        assert_eq!(index.raw.budget.lock().unwrap().used, index.raw_len());
    }

    #[test]
    fn readers_have_independent_offsets_and_own_scratch_lifetime() {
        let index = build(&["needle haystack".into()], Tokenizer::Default, false);
        let path = index.raw.file.path().to_path_buf();
        let budget = Arc::clone(&index.raw.budget);
        let mut a = index.reopen_reader().unwrap();
        let mut b = index.reopen_reader().unwrap();
        let mut magic = [0; 4];
        a.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, INDEX_MAGIC);
        b.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, INDEX_MAGIC);
        drop(index);
        assert!(path.exists());
        assert!(budget.lock().unwrap().used > 0);
        drop(a);
        assert!(path.exists());
        drop(b);
        assert!(!path.exists());
        assert_eq!(budget.lock().unwrap().used, 0);
    }

    #[test]
    fn dropping_unfinished_builder_removes_private_scratch() {
        use std::os::unix::fs::PermissionsExt;
        let mut builder =
            SpillingIndexBuilder::new(Tokenizer::Default, MIN_MEMORY, 1024 * 1024).unwrap();
        builder.push_row("needle").unwrap();
        builder.flush_run().unwrap();
        let path = builder.levels[0][0].raw.file.path().to_path_buf();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let budget = Arc::clone(&builder.budget);
        drop(builder);
        assert!(!path.exists());
        assert_eq!(budget.lock().unwrap().used, 0);
    }

    #[test]
    fn real_readonly_file_write_error_retains_then_releases_reservation() {
        let budget = budget(1024);
        let mut file = ScratchFile::new(Arc::clone(&budget)).unwrap();
        let readonly = File::open(file.file.path()).unwrap();
        *file.file.as_file_mut() = readonly;
        assert!(file.write_all(b"fails").is_err());
        assert_eq!(budget.lock().unwrap().used, 5);
        drop(file);
        assert_eq!(budget.lock().unwrap().used, 0);
    }

    #[test]
    fn external_length_change_is_not_eof_success() {
        let index = build(&["needle".into()], Tokenizer::Default, false);
        index.raw.file.as_file().set_len(0).unwrap();
        assert!(index.reopen_reader().is_err());
    }

    #[test]
    fn same_length_valid_structure_corruption_is_rejected_by_checksum() {
        use std::io::{Seek, SeekFrom};
        let index = build(&["needle".into()], Tokenizer::Default, false);
        let mut writer = index.raw.file.reopen().unwrap();
        // Changing the first term letter remains valid UTF-8 and valid v1 structure.
        writer.seek(SeekFrom::Start(8)).unwrap();
        writer.write_all(b"x").unwrap();
        writer.flush().unwrap();
        let mut reader = index.reopen_reader().unwrap();
        let mut bytes = Vec::new();
        assert!(reader.read_to_end(&mut bytes).is_err());
        let budget = Arc::clone(&index.raw.budget);
        assert!(merge(vec![index], MIN_MEMORY, budget).is_err());
    }

    #[test]
    fn charged_work_and_live_run_descriptor_counts_stay_bounded() {
        let mut builder =
            SpillingIndexBuilder::new(Tokenizer::Default, MIN_MEMORY, 32 * 1024 * 1024).unwrap();
        for i in 0..1000 {
            builder
                .push_row(&format!("term{i} shared repeated repeated"))
                .unwrap();
            builder.flush_run().unwrap();
            assert!(builder.charge <= MIN_MEMORY / 2);
            assert!(builder.levels.len() <= 9);
            assert!(builder.levels.iter().all(|runs| runs.len() < FAN_IN));
            assert!(builder.levels.iter().map(Vec::len).sum::<usize>() <= 9 * (FAN_IN - 1));
        }
        let index = builder.finish().unwrap();
        assert_eq!(index.n_rows(), 1000);
    }

    #[test]
    fn truncated_or_overflowing_varint_io_errors_propagate() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected read failure",
                ))
            }
        }
        assert_eq!(
            read_varint(&mut Broken).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        let mut raw = Vec::new();
        header(&mut raw, 1, 1).unwrap();
        varint(&mut raw, (MIN_MEMORY / (FAN_IN * 4) + 1) as u64).unwrap();
        malformed(&raw, 1, 1);
    }

    #[test]
    fn truncated_header_and_posting_streams_refuse() {
        for raw in [
            b"K".as_slice(),
            b"KIDX\x01\x01\x01\x06needle\x01".as_slice(),
        ] {
            malformed(raw, 1, 1);
        }
    }

    #[test]
    fn bad_magic_version_utf8_order_and_trailing_bytes_refuse() {
        for raw in [
            b"BADX\x01\x01\x00".as_slice(),
            b"KIDX\x02\x01\x00".as_slice(),
            b"KIDX\x01\x01\x01\x01\xff\x01\x00".as_slice(),
            b"KIDX\x01\x01\x02\x01b\x01\x00\x01a\x01\x00".as_slice(),
            b"KIDX\x01\x01\x02\x01a\x01\x00\x01a\x01\x00".as_slice(),
        ] {
            let terms = if raw.get(6) == Some(&2) { 2 } else { 1 };
            malformed(raw, 1, terms);
        }
        malformed(b"KIDX\x01\x01\x00x", 1, 0);
    }

    #[test]
    fn zero_duplicate_out_of_domain_and_overflow_postings_refuse() {
        malformed(b"KIDX\x01\x02\x01\x01a\x00", 2, 1);
        malformed(b"KIDX\x01\x02\x01\x01a\x02\x00\x00", 2, 1);
        malformed(b"KIDX\x01\x02\x01\x01a\x01\x02", 2, 1);
        let mut raw = Vec::new();
        header(&mut raw, u32::MAX, 1).unwrap();
        varint(&mut raw, 1).unwrap();
        raw.push(b'a');
        varint(&mut raw, 2).unwrap();
        varint(&mut raw, u64::from(u32::MAX - 2)).unwrap();
        varint(&mut raw, 10).unwrap();
        malformed(&raw, u32::MAX, 1);
    }

    #[test]
    fn varint_extrema_and_malformed_values() {
        for value in [0, 1, 127, 128, u64::from(u32::MAX), u64::MAX] {
            let mut raw = Vec::new();
            varint(&mut raw, value).unwrap();
            assert_eq!(read_varint(&mut raw.as_slice()).unwrap(), value);
        }
        for raw in [vec![0x80], vec![0x80, 0], vec![0xff; 10], vec![0x80; 11]] {
            assert!(read_varint(&mut raw.as_slice()).is_err());
        }
    }

    #[test]
    fn explicit_budget_and_fan_in_refusals() {
        assert!(SpillingIndexBuilder::new(Tokenizer::Default, MIN_MEMORY - 1, 1024).is_err());
        assert!(SpillingIndexBuilder::new(Tokenizer::Default, MIN_MEMORY, 0).is_err());
        assert!(merge(Vec::new(), MIN_MEMORY, budget(1024)).is_err());
        let budget = budget(1024);
        let inputs = (0..17)
            .map(|_| raw_run(b"KIDX\x01\x00\x00", 0, 0, Arc::clone(&budget)))
            .collect();
        assert!(merge(inputs, MIN_MEMORY, Arc::clone(&budget)).is_err());
        assert_eq!(budget.lock().unwrap().used, 0);
    }
}
