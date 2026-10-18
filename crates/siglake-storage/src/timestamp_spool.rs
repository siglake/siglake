//! Fixed-width timestamp scratch files for page-bounded merge planning.
//!
//! Each pass owns independent file descriptions and at most 64 KiB per input.
//! Scratch files are private (0600 on Unix) and removed by RAII. This bounds
//! retained timestamp buffers, not total decoder/writer memory. TMPDIR may be
//! tmpfs: operators must choose disk-backed scratch to avoid charging the same
//! bytes to the memory cgroup. Disk exhaustion is an error, never a RAM fallback.
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{Read, Write};

use anyhow::{ensure, Context, Result};

use crate::merge::MergeRun;

pub(crate) const CURSOR_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_SPOOL_BYTES: u64 = 8 * 1024 * 1024 * 1024;

pub(crate) fn spool_budget_bytes(rows: usize) -> Result<u64> {
    let bytes = u64::try_from(rows)?
        .checked_mul(8)
        .context("timestamp spool length overflow")?;
    let limit = match std::env::var("SIGLAKE_MERGE_TIMESTAMP_SPOOL_MAX_BYTES") {
        Ok(value) => value
            .parse::<u64>()
            .context("invalid timestamp spool byte limit")?,
        Err(std::env::VarError::NotPresent) => DEFAULT_MAX_SPOOL_BYTES,
        Err(error) => return Err(error.into()),
    };
    check_spool_budget(rows, limit)?;
    tracing::info!(required_bytes = bytes, limit_bytes = limit, scratch_directory = ?std::env::var_os("SIGLAKE_MERGE_TIMESTAMP_SPOOL_DIR").map(std::path::PathBuf::from).unwrap_or_else(std::env::temp_dir), "timestamp scratch admission (tmpfs charges memory; disk exhaustion propagates)");
    Ok(bytes)
}

fn check_spool_budget(rows: usize, limit: u64) -> Result<u64> {
    let bytes = u64::try_from(rows)?
        .checked_mul(8)
        .context("timestamp spool length overflow")?;
    ensure!(
        bytes <= limit,
        "timestamp spool requires {bytes} bytes, limit {limit}"
    );
    Ok(bytes)
}

pub(crate) struct TimestampSpool {
    file: tempfile::NamedTempFile,
    rows: usize,
    expected_rows: usize,
    descending: bool,
    previous: Option<i64>,
}

impl TimestampSpool {
    pub(crate) fn new(expected_rows: usize, descending: bool) -> Result<Self> {
        let mut builder = tempfile::Builder::new();
        builder.prefix("siglake-merge-timestamps-");
        let file = match std::env::var_os("SIGLAKE_MERGE_TIMESTAMP_SPOOL_DIR") {
            Some(directory) => builder.tempfile_in(directory),
            None => builder.tempfile(),
        }
        .context("create private timestamp spool")?;
        Ok(Self {
            file,
            rows: 0,
            expected_rows,
            descending,
            previous: None,
        })
    }

    pub(crate) fn push(&mut self, values: &[i64]) -> Result<()> {
        let end = self
            .rows
            .checked_add(values.len())
            .context("timestamp spool row overflow")?;
        ensure!(
            end <= self.expected_rows,
            "timestamp stream exceeds metadata row count"
        );
        let mut encoded = [0u8; CURSOR_BYTES];
        for block in values.chunks(CURSOR_BYTES / 8) {
            for (slot, &value) in encoded.chunks_exact_mut(8).zip(block) {
                if let Some(previous) = self.previous {
                    ensure!(
                        if self.descending {
                            previous >= value
                        } else {
                            previous <= value
                        },
                        "timestamp input not sorted in declared direction"
                    );
                }
                slot.copy_from_slice(&value.to_le_bytes());
                self.previous = Some(value);
            }
            self.file
                .as_file_mut()
                .write_all(&encoded[..block.len() * 8])
                .context("write timestamp spool")?;
        }
        self.rows = end;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Self> {
        ensure!(
            self.rows == self.expected_rows,
            "timestamp stream count differs from metadata"
        );
        ensure!(
            self.file.as_file().metadata()?.len() == self.byte_len()?,
            "timestamp spool length mismatch"
        );
        Ok(self)
    }

    fn byte_len(&self) -> Result<u64> {
        u64::try_from(self.rows)?
            .checked_mul(8)
            .context("timestamp spool byte length overflow")
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    fn cursor(&self) -> Result<Cursor> {
        // reopen, NOT try_clone: simultaneous census/execution cursors must
        // never share a kernel seek offset.
        let file = self.file.reopen().context("reopen timestamp spool")?;
        ensure!(
            file.metadata()?.len() == self.byte_len()?,
            "timestamp spool changed length"
        );
        Ok(Cursor {
            file,
            buffer: vec![0; CURSOR_BYTES],
            offset: 0,
            valid: 0,
            consumed: 0,
            rows: self.rows,
        })
    }
}

struct Cursor {
    file: File,
    buffer: Vec<u8>,
    offset: usize,
    valid: usize,
    consumed: usize,
    rows: usize,
}
impl Cursor {
    fn next_timestamp(&mut self) -> Result<Option<i64>> {
        if self.consumed == self.rows {
            ensure!(
                self.file.metadata()?.len()
                    == u64::try_from(self.rows)?
                        .checked_mul(8)
                        .context("timestamp cursor length overflow")?,
                "timestamp spool changed length during read"
            );
            return Ok(None);
        }
        if self.offset == self.valid {
            self.valid = (self.rows - self.consumed).min(CURSOR_BYTES / 8) * 8;
            self.file
                .read_exact(&mut self.buffer[..self.valid])
                .context("read complete timestamp spool page")?;
            self.offset = 0;
        }
        let value = i64::from_le_bytes(self.buffer[self.offset..self.offset + 8].try_into()?);
        self.offset += 8;
        self.consumed += 1;
        Ok(Some(value))
    }
}

/// One uncut run at a time, coalescing across scratch-page boundaries.
/// The i128 key avoids negating i64::MIN while keeping the lowest-input tie.
pub(crate) struct SpoolMergeRuns {
    cursors: Vec<Cursor>,
    heap: BinaryHeap<(i128, Reverse<usize>)>,
    positions: Vec<usize>,
    descending: bool,
    failed: bool,
}
impl SpoolMergeRuns {
    pub(crate) fn new(spools: &[TimestampSpool], descending: bool) -> Result<Self> {
        let mut cursors = spools
            .iter()
            .map(TimestampSpool::cursor)
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        for (index, cursor) in cursors.iter_mut().enumerate() {
            if let Some(value) = cursor.next_timestamp()? {
                heap.push((Self::key(value, descending), Reverse(index)));
            }
        }
        Ok(Self {
            cursors,
            heap,
            positions: vec![0; spools.len()],
            descending,
            failed: false,
        })
    }
    fn key(value: i64, descending: bool) -> i128 {
        if descending {
            i128::from(value)
        } else {
            -i128::from(value)
        }
    }
    fn next_run(&mut self) -> Result<Option<MergeRun>> {
        let Some((_, Reverse(index))) = self.heap.pop() else {
            return Ok(None);
        };
        let start = self.positions[index];
        let mut count = 1usize;
        while let Some(value) = self.cursors[index].next_timestamp()? {
            let candidate = (Self::key(value, self.descending), Reverse(index));
            if self.heap.peek().is_some_and(|rival| candidate < *rival) {
                self.heap.push(candidate);
                break;
            }
            count = count
                .checked_add(1)
                .context("timestamp run length overflow")?;
        }
        self.positions[index] = start
            .checked_add(count)
            .context("timestamp position overflow")?;
        Ok(Some(MergeRun {
            input_index: index,
            start_row: start,
            row_count: count,
        }))
    }
}
impl Iterator for SpoolMergeRuns {
    type Item = Result<MergeRun>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.next_run() {
            Ok(Some(run)) => Some(Ok(run)),
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

pub(crate) struct SpoolMergeChunks {
    runs: SpoolMergeRuns,
    remaining: Option<MergeRun>,
    chunk_rows: usize,
    failed: bool,
}
impl SpoolMergeChunks {
    pub(crate) fn new(runs: SpoolMergeRuns, chunk_rows: usize) -> Self {
        Self {
            runs,
            remaining: None,
            chunk_rows: chunk_rows.max(1),
            failed: false,
        }
    }
    fn next_chunk(&mut self) -> Result<Option<Vec<MergeRun>>> {
        let mut chunk = Vec::new();
        let mut rows = 0usize;
        while rows < self.chunk_rows {
            let run = match self.remaining.take() {
                Some(run) => Some(run),
                None => self.runs.next().transpose()?,
            };
            let Some(mut run) = run else {
                break;
            };
            let take = run.row_count.min(self.chunk_rows - rows);
            chunk.push(MergeRun {
                row_count: take,
                ..run
            });
            rows += take;
            run.start_row += take;
            run.row_count -= take;
            if run.row_count > 0 {
                self.remaining = Some(run);
            }
        }
        Ok((!chunk.is_empty()).then_some(chunk))
    }
}
impl Iterator for SpoolMergeChunks {
    type Item = Result<Vec<MergeRun>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.next_chunk() {
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge::{MergeChunks, MergeRuns};
    use proptest::prelude::*;

    fn make_spools(inputs: &[Vec<i64>], descending: bool) -> Vec<TimestampSpool> {
        inputs
            .iter()
            .map(|input| {
                let mut spool = TimestampSpool::new(input.len(), descending).unwrap();
                for batch in input.chunks(257) {
                    spool.push(batch).unwrap();
                }
                spool.finish().unwrap()
            })
            .collect()
    }
    fn expand(runs: &[MergeRun]) -> Vec<(usize, usize)> {
        runs.iter()
            .flat_map(|run| {
                (run.start_row..run.start_row + run.row_count)
                    .map(move |row| (run.input_index, row))
            })
            .collect()
    }

    #[test]
    fn spool_runs_and_chunks_match_slice_planner_across_pages() {
        for descending in [false, true] {
            let mut inputs = vec![
                vec![],
                (0..20_000).map(|v| v / 4).collect(),
                (0..20_000).map(|v| v / 3).collect(),
                vec![9000; 17_000],
            ];
            if descending {
                for input in &mut inputs {
                    input.reverse();
                }
            }
            let spools = make_spools(&inputs, descending);
            let slices: Vec<_> = inputs.iter().map(Vec::as_slice).collect();
            let expected: Vec<_> = MergeRuns::new(&slices, descending).collect();
            let actual = SpoolMergeRuns::new(&spools, descending)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            assert_eq!(
                actual, expected,
                "uncut runs must cross pages without artificial splits"
            );
            for size in [1, 257, 8192, 261_632] {
                let actual =
                    SpoolMergeChunks::new(SpoolMergeRuns::new(&spools, descending).unwrap(), size)
                        .collect::<Result<Vec<_>>>()
                        .unwrap();
                assert_eq!(
                    actual,
                    MergeChunks::new(MergeRuns::new(&slices, descending), size).collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn spool_extrema_ties_preserve_exact_global_order_and_ordinals() {
        for descending in [false, true] {
            let mut inputs = vec![
                vec![i64::MIN, -1, 0, i64::MAX],
                vec![i64::MIN, i64::MIN, 0, i64::MAX],
                vec![],
            ];
            if descending {
                for input in &mut inputs {
                    input.reverse();
                }
            }
            let spools = make_spools(&inputs, descending);
            let actual = SpoolMergeRuns::new(&spools, descending)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            let mut expected: Vec<_> = inputs
                .iter()
                .enumerate()
                .flat_map(|(index, input)| {
                    input
                        .iter()
                        .enumerate()
                        .map(move |(row, &value)| (value, index, row))
                })
                .collect();
            expected.sort_by(|a, b| {
                let order = if descending {
                    b.0.cmp(&a.0)
                } else {
                    a.0.cmp(&b.0)
                };
                order.then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2))
            });
            assert_eq!(
                expand(&actual),
                expected
                    .iter()
                    .map(|&(_, input, row)| (input, row))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn spool_cursors_are_independent_and_scratch_is_private_and_removed() {
        let spools = make_spools(&[vec![1, 2, 3]], false);
        let path = spools[0].file.path().to_path_buf();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let mut one = spools[0].cursor().unwrap();
        let mut two = spools[0].cursor().unwrap();
        assert_eq!(one.next_timestamp().unwrap(), Some(1));
        assert_eq!(one.next_timestamp().unwrap(), Some(2));
        assert_eq!(two.next_timestamp().unwrap(), Some(1));
        drop(one);
        drop(two);
        drop(spools);
        assert!(!path.exists());
    }

    #[test]
    fn spool_refuses_short_extra_unsorted_and_unwritable_inputs() {
        let mut short = TimestampSpool::new(2, false).unwrap();
        short.push(&[1]).unwrap();
        assert!(short.finish().is_err());
        let mut extra = TimestampSpool::new(1, false).unwrap();
        assert!(extra.push(&[1, 2]).is_err());
        let mut unsorted = TimestampSpool::new(2, false).unwrap();
        unsorted.push(&[2]).unwrap();
        assert!(unsorted.push(&[1]).is_err());
        let mut wrong_direction = TimestampSpool::new(2, true).unwrap();
        assert!(wrong_direction.push(&[1, 2]).is_err());
        let mut unwritable = TimestampSpool::new(1, false).unwrap();
        let read_only = File::open(unwritable.file.path()).unwrap();
        *unwritable.file.as_file_mut() = read_only;
        assert!(
            unwritable.push(&[1]).is_err(),
            "write failure must propagate without fallback"
        );
    }

    #[test]
    fn spool_truncated_and_extended_files_never_become_successful_eof() {
        let spools = make_spools(&[(0..20_000).collect()], false);
        let mut runs = SpoolMergeRuns::new(&spools, false).unwrap();
        spools[0].file.as_file().set_len(8).unwrap();
        assert!(runs.next().unwrap().is_err());
        assert!(runs.next().is_none(), "failed planner is terminal");
        assert!(SpoolMergeRuns::new(&spools, false).is_err());
        let extended = make_spools(&[vec![1, 2]], false);
        extended[0].file.as_file().set_len(24).unwrap();
        assert!(SpoolMergeRuns::new(&extended, false).is_err());
    }

    #[test]
    fn spool_chunk_propagates_late_read_failure_instead_of_partial_chunk() {
        let spools = make_spools(&[(0..20_000).collect()], false);
        let runs = SpoolMergeRuns::new(&spools, false).unwrap();
        spools[0].file.as_file().set_len(64).unwrap();
        let mut chunks = SpoolMergeChunks::new(runs, 261_632);
        assert!(chunks.next().unwrap().is_err());
        assert!(chunks.next().is_none());
    }

    #[test]
    fn spool_resident_buffers_are_bounded_independent_of_input_rows() {
        let inputs: Vec<_> = (0..64).map(|_| (0..16_385).collect()).collect();
        let spools = make_spools(&inputs, false);
        let planner = SpoolMergeRuns::new(&spools, false).unwrap();
        assert_eq!(
            planner
                .cursors
                .iter()
                .map(|c| c.buffer.capacity())
                .sum::<usize>(),
            64 * CURSOR_BYTES
        );
        assert_eq!(planner.heap.len(), 64);
        assert_eq!(
            spools.iter().map(|s| s.byte_len().unwrap()).sum::<u64>(),
            64 * 16_385 * 8
        );
        assert!(planner
            .cursors
            .iter()
            .all(|c| c.buffer.len() == CURSOR_BYTES));
    }

    #[test]
    fn spool_disk_budget_exact_boundary_and_overflow_refuse() {
        assert_eq!(check_spool_budget(10, 80).unwrap(), 80);
        assert!(check_spool_budget(10, 79).is_err());
        assert!(check_spool_budget(usize::MAX, u64::MAX).is_err());
        assert_eq!(check_spool_budget(0, 0).unwrap(), 0);
    }

    proptest! {
        #[test]
        fn spool_matches_sorted_slice_runs(mut inputs in prop::collection::vec(prop::collection::vec(-1000i64..1000, 0..40), 0..6), descending in any::<bool>(), chunk_rows in 1usize..100) {
            for input in &mut inputs { input.sort(); if descending { input.reverse(); } }
            let spools = make_spools(&inputs, descending);
            let slices: Vec<_> = inputs.iter().map(Vec::as_slice).collect();
            let expected: Vec<_> = MergeChunks::new(MergeRuns::new(&slices, descending), chunk_rows).collect();
            let actual = SpoolMergeChunks::new(SpoolMergeRuns::new(&spools, descending).unwrap(), chunk_rows).collect::<Result<Vec<_>>>().unwrap();
            prop_assert_eq!(actual, expected);
        }
    }
}
