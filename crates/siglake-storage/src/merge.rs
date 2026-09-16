//! Streaming, page-bounded k-way merge of already-timestamp-sorted inputs.
//!
//! siglake's time-ordered storage invariant means every data file is physically
//! sorted by its `timestamp` column in the table's **declared** direction
//! (ascending for tables created by current code; descending for a legacy table
//! whose stored sort order predates the ASC default). Merging K such files is
//! therefore a k-way merge of sorted runs — and because the inputs are
//! pre-sorted, it can be done with **bounded memory**: at any moment we hold
//! only the K "current" input batches plus one output batch's worth of row
//! indices, regardless of how large the inputs are. This is what lets compaction
//! merge files bigger than a single in-RAM concat (the `read_files_concatenated`
//! ceiling) and is the prerequisite for the large-file age-tier ladder.
//!
//! The merge direction MUST match the inputs' physical direction (and the
//! `SortingColumn` footer the caller stamps on the output): the caller derives
//! it from the table's declared sort order. Merging descending inputs as if
//! ascending would conserve the row multiset but emit them mis-ordered under a
//! footer claiming the declared order — a footer lie the WS-3 ordered-scan
//! early-stop would then trust, returning wrong rows.
//!
//! [`TimestampKwayMerge`] consumes the inputs as **async** `Stream`s (the
//! object-store-backed Parquet reader is async) and yields merged batches one at
//! a time via [`next_batch`](TimestampKwayMerge::next_batch), so it can be driven
//! straight into the Parquet writer with bounded memory end-to-end. The merge
//! algorithm is IO-agnostic — the tests drive it over in-memory streams — so its
//! two load-bearing invariants are exhaustively property-tested: the output is
//! globally sorted, and it conserves the exact multiset of input rows (no drop,
//! no duplicate, no payload mis-pairing).

use anyhow::{Context, Result};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use arrow::compute::interleave_record_batch;
use arrow_array::{Int64Array, RecordBatch};
use futures::{Stream, StreamExt};

/// A contiguous run of rows taken from a single input in the merged output —
/// the unit of the RLE merge plan. Because inputs are pre-sorted, the k-way
/// merge emits *runs*, not per-row picks: near-disjoint inputs collapse to one
/// run per input, and re-merging already-merged files produces ever-longer
/// runs, so planning (and the interleave it drives) gets **cheaper as data
/// ages** — the self-reinforcing property the leveled ladder relies on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeRun {
    /// Which input file the run comes from.
    pub input_index: usize,
    /// First row within that input.
    pub start_row: usize,
    /// Consecutive rows taken from this input.
    pub row_count: usize,
}

/// Compute the RLE merge plan for `inputs`, where `inputs[i]` is input `i`'s
/// full timestamp column, already sorted in the merge direction. The plan,
/// executed run by run, yields exactly the row order [`TimestampKwayMerge`]
/// would produce (globally sorted, ties broken by lowest input index) — the
/// differential property test below pins that equivalence.
///
/// Instead of a heap pop per ROW, this pops per RUN: the winning input
/// binary-searches how far it stays ahead of the strongest rival (the heap
/// peek), so cost is O(runs · (log k + log rows)) — for the sorted,
/// near-disjoint inputs compaction feeds it, runs ≈ k and this is effectively
/// free, no matter how many rows the bin holds.
pub fn compute_merge_plan(inputs: &[&[i64]], descending: bool) -> Vec<MergeRun> {
    // Direction normalized into one key (negated for ASC) so a single max-heap
    // serves both directions; `Reverse(i)` makes the lowest input index win
    // ties — both exactly as in `TimestampKwayMerge`.
    let key = |ts: i64| if descending { ts } else { ts.wrapping_neg() };
    let mut heap: BinaryHeap<(i64, Reverse<usize>)> = inputs
        .iter()
        .enumerate()
        .filter(|(_, a)| !a.is_empty())
        .map(|(i, a)| (key(a[0]), Reverse(i)))
        .collect();
    let mut plan: Vec<MergeRun> = Vec::new();
    let mut pos = vec![0usize; inputs.len()];
    while let Some((_, Reverse(i))) = heap.pop() {
        let a = inputs[i];
        let start = pos[i];
        let end = match heap.peek() {
            // No rival left: the winner takes everything it has.
            None => a.len(),
            // The peek is the strongest rival (best key, then lowest index).
            // Input `i` keeps winning while its key beats the rival's — or
            // ties it with a lower input index. `key(a[..])` is non-increasing
            // (inputs are sorted in the merge direction), so the win predicate
            // is monotone and `partition_point` finds the handover row.
            Some(&(rival_key, Reverse(rival))) => {
                start
                    + a[start..].partition_point(|&ts| {
                        let k = key(ts);
                        k > rival_key || (k == rival_key && i < rival)
                    })
            }
        };
        debug_assert!(end > start, "heap winner must contribute at least one row");
        plan.push(MergeRun {
            input_index: i,
            start_row: start,
            row_count: end - start,
        });
        pos[i] = end;
        if end < a.len() {
            heap.push((key(a[end]), Reverse(i)));
        }
    }
    plan
}

/// Split a merge plan into chunks of at most `chunk_rows` output rows, cutting
/// runs at chunk boundaries. Each chunk is what the page-bounded executor
/// materializes at once, so `chunk_rows` — not fan-in, not bin bytes — bounds
/// the merge's decoded working set. Within a chunk every input's rows form one
/// contiguous range (inputs are consumed strictly forward), which is what lets
/// the executor fetch each input's contribution with a single `RowSelection`.
pub fn split_plan_chunks(plan: &[MergeRun], chunk_rows: usize) -> Vec<Vec<MergeRun>> {
    let chunk_rows = chunk_rows.max(1);
    let mut chunks: Vec<Vec<MergeRun>> = Vec::new();
    let mut cur: Vec<MergeRun> = Vec::new();
    let mut cur_rows = 0usize;
    for run in plan {
        let mut start = run.start_row;
        let mut remaining = run.row_count;
        while remaining > 0 {
            let take = remaining.min(chunk_rows - cur_rows);
            cur.push(MergeRun {
                input_index: run.input_index,
                start_row: start,
                row_count: take,
            });
            cur_rows += take;
            start += take;
            remaining -= take;
            if cur_rows == chunk_rows {
                chunks.push(std::mem::take(&mut cur));
                cur_rows = 0;
            }
        }
    }
    if !cur.is_empty() {
        chunks.push(cur);
    }
    chunks
}

/// Wall-clock attribution of a merge, split by what the time was spent *on*
/// rather than by which function ran. Compaction throughput was measured in
/// aggregate for the first time on 2026-08-04 (~95K rows/s) with no breakdown,
/// so every proposed lever — verbatim block copy, chunk pipelining, bin
/// parallelism — was priced against an unattributed number.
///
/// The three buckets are disjoint and together account for the merge:
/// - `input`: awaiting input batches (object-store fetch + Parquet decode)
/// - `assemble`: merge logic proper (heap/plan, `interleave`, slicing, schema
///   alignment) — pure CPU, no IO
/// - `write`: handing batches to the writer (Parquet encode + upload)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeStageNanos {
    pub input: u64,
    pub assemble: u64,
    pub write: u64,
}

impl MergeStageNanos {
    pub fn total(&self) -> u64 {
        self.input + self.assemble + self.write
    }

    /// Share of the accounted time in each bucket, as fractions summing to ~1.
    /// Returns zeros when nothing was recorded.
    pub fn shares(&self) -> (f64, f64, f64) {
        let total = self.total() as f64;
        if total <= 0.0 {
            return (0.0, 0.0, 0.0);
        }
        (
            self.input as f64 / total,
            self.assemble as f64 / total,
            self.write as f64 / total,
        )
    }
}

/// A streaming k-way merge over async batch sources, each already sorted by the
/// `ts_col` column in `descending` order (within and across its batches). Call
/// [`next_batch`](Self::next_batch) repeatedly until it returns `None`; each call
/// yields the next output batch (≤ `target_rows` rows), globally sorted by
/// timestamp in the same direction, conserving the exact multiset of input rows.
pub struct TimestampKwayMerge<S> {
    sources: Vec<S>,
    /// Current batch per source. Once a source is exhausted its last batch is
    /// kept as a never-referenced placeholder so `interleave`'s source array
    /// list stays aligned to source index.
    current: Vec<RecordBatch>,
    /// The `ts_col` of `current[i]` as unix nanoseconds, resolved once per
    /// (re)fill — so the hot loop reads timestamps without re-doing the
    /// downcast per row. `ts_col` should be the exact `timestamp_ns` sibling
    /// where the table has one: `timestamp` is only microsecond-precise since
    /// the 2026-09-06 contract, and merging on it would leave microsecond ties
    /// ordered arbitrarily across inputs, contradicting the declared
    /// `(timestamp, timestamp_ns)` sort order the output claims.
    ts_arrays: Vec<Int64Array>,
    pos: Vec<usize>,
    /// Min-priority queue of the live sources' current heads, keyed so that
    /// `pop()` yields the next output row in `descending` order, ties broken by
    /// lowest source index. Replaces an O(k)-per-row linear scan with O(log k):
    /// at high fan-in (a whole partition's files) the linear scan dominated
    /// compaction. Key normalizes direction into a single i64 (negated for ASC)
    /// so one max-heap serves both directions; `Reverse(idx)` makes the lowest
    /// source index win ties.
    heap: BinaryHeap<(i64, Reverse<usize>)>,
    ts_col: usize,
    target_rows: usize,
    /// Merge direction; MUST match the inputs' physical sort direction.
    descending: bool,
    started: bool,
    /// Time attribution. Only `input` and `assemble` are filled here — the
    /// caller owns the writer and records `write` itself. Sampled once per
    /// source refill and once per output batch, never per row.
    stages: MergeStageNanos,
}

async fn pull_nonempty<S>(s: &mut S) -> Result<Option<RecordBatch>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    while let Some(b) = s.next().await {
        let b = b?;
        if b.num_rows() > 0 {
            return Ok(Some(b));
        }
    }
    Ok(None)
}

impl<S> TimestampKwayMerge<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    /// `descending` selects the merge direction; it MUST match the physical
    /// sort direction of every input (the table's declared sort order).
    pub fn new(sources: Vec<S>, ts_col: usize, target_rows: usize, descending: bool) -> Self {
        Self {
            sources,
            current: Vec::new(),
            ts_arrays: Vec::new(),
            pos: Vec::new(),
            heap: BinaryHeap::new(),
            ts_col,
            target_rows: target_rows.max(1),
            descending,
            started: false,
            stages: MergeStageNanos::default(),
        }
    }

    /// Time spent awaiting inputs vs. assembling output batches so far.
    pub fn stages(&self) -> MergeStageNanos {
        self.stages
    }

    /// Downcast `current[i]`'s timestamp column and cache it for the hot loop.
    fn cache_ts_array(&mut self, i: usize) -> Result<()> {
        let arr = siglake_core::column_nanos(self.current[i].column(self.ts_col))
            .context("k-way merge: timestamp column is not a timestamp or an ns long")?;
        if i < self.ts_arrays.len() {
            self.ts_arrays[i] = arr;
        } else {
            self.ts_arrays.push(arr);
        }
        Ok(())
    }

    /// Heap key for source `i`'s current head row: direction normalized into one
    /// i64 (negated for ASC so the min timestamp sorts highest in the max-heap),
    /// `Reverse(i)` so the lowest source index wins ties.
    fn head_key(&self, i: usize) -> (i64, Reverse<usize>) {
        let ts = self.ts_arrays[i].value(self.pos[i]);
        let ord = if self.descending {
            ts
        } else {
            ts.wrapping_neg()
        };
        (ord, Reverse(i))
    }

    /// One-time init: pull the first non-empty batch from each source, dropping
    /// any source that yields none (it contributes no rows and would have no
    /// valid placeholder batch for `interleave`), then seed the heap with each
    /// live source's head.
    async fn start(&mut self) -> Result<()> {
        let t0 = std::time::Instant::now();
        let sources = std::mem::take(&mut self.sources);
        for mut s in sources {
            if let Some(b) = pull_nonempty(&mut s).await? {
                let i = self.current.len();
                self.current.push(b);
                self.pos.push(0);
                self.sources.push(s);
                self.cache_ts_array(i)?;
                self.heap.push(self.head_key(i));
            }
        }
        self.started = true;
        // The first pull from every source is object-store fetch + decode of a
        // first row group, which at high fan-in is a large one-off cost.
        self.stages.input += t0.elapsed().as_nanos() as u64;
        Ok(())
    }

    /// Produce the next merged output batch, or `None` when all inputs are
    /// drained. Each batch ends at `target_rows` or when a source crosses a batch
    /// boundary (whichever first), so output batches never exceed `target_rows`.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.started {
            self.start().await?;
        }
        if self.current.is_empty() {
            return Ok(None);
        }

        let t_assemble = std::time::Instant::now();
        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(self.target_rows);
        // Index of a source that crossed its batch boundary and must be refilled
        // *after* this batch is built (the indices still reference its old batch).
        let mut refill: Option<usize> = None;

        // O(log k) per row: pop the next source from the heap, emit its head, and
        // re-seed it with its next head (or, at a batch boundary, end the batch and
        // refill below). Exhausted sources are simply absent from the heap.
        while let Some((_, Reverse(i))) = self.heap.pop() {
            indices.push((i, self.pos[i]));
            self.pos[i] += 1;
            if self.pos[i] >= self.current[i].num_rows() {
                refill = Some(i); // boundary: end this batch, refill source i after
                break;
            }
            self.heap.push(self.head_key(i));
            if indices.len() >= self.target_rows {
                break;
            }
        }

        if indices.is_empty() {
            self.stages.assemble += t_assemble.elapsed().as_nanos() as u64;
            return Ok(None);
        }
        let src: Vec<&RecordBatch> = self.current.iter().collect();
        let batch = interleave_record_batch(&src, &indices).context("interleave merge batch")?;
        // The heap loop and the per-row gather are the merge logic proper — the
        // cost a run-aware plan (or a verbatim block copy) would remove.
        self.stages.assemble += t_assemble.elapsed().as_nanos() as u64;

        // Now that the batch is built, refill the boundary-crossing source and
        // re-seed the heap with its new head (or leave it out of the heap — and
        // keep its old batch as an interleave placeholder — once drained).
        if let Some(i) = refill {
            let t_input = std::time::Instant::now();
            let pulled = pull_nonempty(&mut self.sources[i]).await?;
            self.stages.input += t_input.elapsed().as_nanos() as u64;
            if let Some(b) = pulled {
                self.current[i] = b;
                self.pos[i] = 0;
                self.cache_ts_array(i)?;
                self.heap.push(self.head_key(i));
            }
            // else: drained — leave current[i] as an interleave placeholder, and
            // simply don't re-seed it into the heap (the heap tracks live sources).
        }
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use proptest::prelude::*;

    /// Build a batch of `(timestamp, id)` rows. `id` rides along so a merge that
    /// conserves timestamps but drops/duplicates/mis-pairs payload is caught.
    fn batch(rows: &[(i64, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("id", DataType::Int64, false),
        ]));
        let ts = arrow_array::TimestampNanosecondArray::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        );
        let id = Int64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(id)]).unwrap()
    }

    /// Merge materialized inputs (each `Vec<RecordBatch>` as an in-memory async
    /// stream) and collect the emitted batches — drives the async merger
    /// synchronously via `futures::executor::block_on` (no real IO).
    fn merge_to_vec(
        inputs: Vec<Vec<RecordBatch>>,
        ts_col: usize,
        target_rows: usize,
    ) -> Vec<RecordBatch> {
        merge_to_vec_dir(inputs, ts_col, target_rows, false)
    }

    fn merge_to_vec_dir(
        inputs: Vec<Vec<RecordBatch>>,
        ts_col: usize,
        target_rows: usize,
        descending: bool,
    ) -> Vec<RecordBatch> {
        let sources: Vec<_> = inputs
            .into_iter()
            .map(|b| futures::stream::iter(b.into_iter().map(Ok)))
            .collect();
        let mut merge = TimestampKwayMerge::new(sources, ts_col, target_rows, descending);
        let mut out = Vec::new();
        futures::executor::block_on(async {
            while let Some(b) = merge.next_batch().await.unwrap() {
                out.push(b);
            }
        });
        out
    }

    /// Flatten output batches into `(timestamp, id)` rows in output order.
    fn rows_of(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
        let mut out = Vec::new();
        for b in batches {
            let ts = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                .unwrap();
            let id = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for r in 0..b.num_rows() {
                out.push((ts.value(r), id.value(r)));
            }
        }
        out
    }

    /// Microbenchmark: k-way merge throughput vs fan-in. Run with
    /// `cargo test -p siglake-storage --lib --release merge::tests::bench_kway -- --ignored --nocapture`.
    /// Builds K perfectly-interleaved sorted runs (worst case: every output row
    /// switches source) totaling ~N rows, and reports rows/s per fan-in. Exposes
    /// the O(k)-per-row selection cost as K grows (compaction merges a whole
    /// partition's files = high fan-in at scale).
    #[test]
    #[ignore]
    fn bench_kway_merge_throughput() {
        const N: i64 = 4_000_000;
        for &k in &[4usize, 16, 64, 256, 512] {
            let rows_per = (N as usize) / k;
            // Source i holds timestamps i, i+k, i+2k, ... → globally interleaved.
            let inputs: Vec<Vec<RecordBatch>> = (0..k)
                .map(|i| {
                    // chunk each source into 8192-row batches (like the real reader)
                    let rows: Vec<(i64, i64)> = (0..rows_per)
                        .map(|j| ((j * k + i) as i64, i as i64))
                        .collect();
                    rows.chunks(8192).map(batch).collect()
                })
                .collect();
            let total: usize = inputs.iter().flatten().map(|b| b.num_rows()).sum();
            let start = std::time::Instant::now();
            let out = merge_to_vec(inputs, 0, 8192);
            let elapsed = start.elapsed();
            let emitted: usize = out.iter().map(|b| b.num_rows()).sum();
            assert_eq!(emitted, total);
            let rps = total as f64 / elapsed.as_secs_f64();
            eprintln!(
                "BENCH kway k={k:>4} rows={total} elapsed={:?} -> {:.0} rows/s ({:.1} M/s)",
                elapsed,
                rps,
                rps / 1e6
            );
        }
    }

    #[test]
    fn merges_two_simple_runs() {
        let a = vec![batch(&[(1, 0), (3, 1), (5, 2)])];
        let b = vec![batch(&[(2, 3), (4, 4), (6, 5)])];
        let out = merge_to_vec(vec![a, b], 0, 10);
        assert_eq!(
            rows_of(&out),
            vec![(1, 0), (2, 3), (3, 1), (4, 4), (5, 2), (6, 5)]
        );
    }

    #[test]
    fn merges_two_descending_runs() {
        // DESC inputs (each sorted high→low) merge high→low — the legacy /
        // future DESC-declared events table shape.
        let a = vec![batch(&[(5, 2), (3, 1), (1, 0)])];
        let b = vec![batch(&[(6, 5), (4, 4), (2, 3)])];
        let out = merge_to_vec_dir(vec![a, b], 0, 10, true);
        assert_eq!(
            rows_of(&out),
            vec![(6, 5), (5, 2), (4, 4), (3, 1), (2, 3), (1, 0)]
        );
    }

    #[test]
    fn respects_target_rows_and_multi_batch_inputs() {
        // Two inputs, each split across batches; small target forces several
        // output batches and several boundary crossings.
        let a = vec![batch(&[(1, 0), (2, 1)]), batch(&[(7, 2), (9, 3)])];
        let b = vec![batch(&[(3, 4)]), batch(&[(4, 5), (8, 6)])];
        let out = merge_to_vec(vec![a, b], 0, 2);
        for b in &out {
            assert!(b.num_rows() <= 2, "output batch exceeds target_rows");
        }
        let ts: Vec<i64> = rows_of(&out).iter().map(|r| r.0).collect();
        assert_eq!(ts, vec![1, 2, 3, 4, 7, 8, 9]);
    }

    #[test]
    fn handles_empty_and_single_inputs() {
        let empty: Vec<Vec<RecordBatch>> = vec![];
        assert!(merge_to_vec(empty, 0, 8).is_empty());
        assert!(merge_to_vec(vec![vec![]], 0, 8).is_empty());
        let only = vec![batch(&[(1, 0), (2, 1)])];
        assert_eq!(
            rows_of(&merge_to_vec(vec![only], 0, 8)),
            vec![(1, 0), (2, 1)]
        );
    }

    /// Expand a merge plan into the per-row `(input, row)` pick sequence.
    fn expand_plan(plan: &[MergeRun]) -> Vec<(usize, usize)> {
        plan.iter()
            .flat_map(|r| (r.start_row..r.start_row + r.row_count).map(|row| (r.input_index, row)))
            .collect()
    }

    /// Reference per-row k-way merge with `TimestampKwayMerge`'s exact
    /// semantics (best key wins, ties to lowest input index) — the oracle the
    /// gallop-based planner is checked against.
    fn reference_pick_order(inputs: &[&[i64]], descending: bool) -> Vec<(usize, usize)> {
        let key = |ts: i64| if descending { ts } else { ts.wrapping_neg() };
        let mut heap: BinaryHeap<(i64, Reverse<usize>)> = inputs
            .iter()
            .enumerate()
            .filter(|(_, a)| !a.is_empty())
            .map(|(i, a)| (key(a[0]), Reverse(i)))
            .collect();
        let mut pos = vec![0usize; inputs.len()];
        let mut out = Vec::new();
        while let Some((_, Reverse(i))) = heap.pop() {
            out.push((i, pos[i]));
            pos[i] += 1;
            if pos[i] < inputs[i].len() {
                heap.push((key(inputs[i][pos[i]]), Reverse(i)));
            }
        }
        out
    }

    #[test]
    fn plan_disjoint_inputs_collapse_to_one_run_each() {
        // Time-disjoint inputs — the shape mature levels feed the merge — must
        // plan as exactly one run per input, in range order.
        let a: Vec<i64> = (0..100).collect();
        let b: Vec<i64> = (100..250).collect();
        let c: Vec<i64> = (250..300).collect();
        let plan = compute_merge_plan(&[&c, &a, &b], false);
        assert_eq!(
            plan,
            vec![
                MergeRun {
                    input_index: 1,
                    start_row: 0,
                    row_count: 100
                },
                MergeRun {
                    input_index: 2,
                    start_row: 0,
                    row_count: 150
                },
                MergeRun {
                    input_index: 0,
                    start_row: 0,
                    row_count: 50
                },
            ]
        );
    }

    #[test]
    fn plan_handles_empty_inputs_and_ties() {
        // Empty inputs contribute nothing; full-tie blocks go lowest-index-first.
        let plan = compute_merge_plan(&[&[], &[5, 5], &[5, 5]], false);
        assert_eq!(
            plan,
            vec![
                MergeRun {
                    input_index: 1,
                    start_row: 0,
                    row_count: 2
                },
                MergeRun {
                    input_index: 2,
                    start_row: 0,
                    row_count: 2
                },
            ]
        );
        assert!(compute_merge_plan(&[], false).is_empty());
        assert!(compute_merge_plan(&[&[], &[]], true).is_empty());
    }

    #[test]
    fn split_chunks_cuts_runs_and_preserves_order() {
        let plan = vec![
            MergeRun {
                input_index: 0,
                start_row: 0,
                row_count: 5,
            },
            MergeRun {
                input_index: 1,
                start_row: 0,
                row_count: 2,
            },
            MergeRun {
                input_index: 0,
                start_row: 5,
                row_count: 4,
            },
        ];
        let chunks = split_plan_chunks(&plan, 4);
        // 11 rows in chunks of ≤4, runs cut at boundaries, order preserved.
        assert_eq!(chunks.len(), 3);
        for c in &chunks[..2] {
            assert_eq!(c.iter().map(|r| r.row_count).sum::<usize>(), 4);
        }
        assert_eq!(chunks[2].iter().map(|r| r.row_count).sum::<usize>(), 3);
        let rejoined: Vec<(usize, usize)> = chunks.iter().flat_map(|c| expand_plan(c)).collect();
        assert_eq!(rejoined, expand_plan(&plan));
        // Within each chunk, each input's rows form ONE contiguous range — the
        // property the executor's single-RowSelection-per-input fetch relies on.
        for c in &chunks {
            let mut seen: std::collections::HashMap<usize, (usize, usize)> =
                std::collections::HashMap::new();
            for r in c {
                let e = seen
                    .entry(r.input_index)
                    .or_insert((r.start_row, r.start_row));
                assert_eq!(
                    r.start_row, e.1,
                    "input rows within a chunk must be contiguous"
                );
                e.1 = r.start_row + r.row_count;
            }
        }
    }

    proptest! {
        /// Differential: the gallop-based RLE planner must reproduce the exact
        /// per-row pick order of the per-row heap merge (the semantics
        /// `TimestampKwayMerge` ships), over random sorted inputs with heavy tie
        /// pressure, in both directions. Also: runs must be maximal enough to
        /// never split within one input without an interleaving cause — checked
        /// implicitly by exact sequence equality.
        #[test]
        fn plan_matches_reference_pick_order(
            run_ts in proptest::collection::vec(
                proptest::collection::vec(0i64..30, 0..50), 0..6),
            descending in proptest::bool::ANY,
            chunk_rows in 1usize..17,
        ) {
            let sorted: Vec<Vec<i64>> = run_ts.into_iter().map(|mut ts| {
                ts.sort_unstable();
                if descending { ts.reverse(); }
                ts
            }).collect();
            let inputs: Vec<&[i64]> = sorted.iter().map(|v| v.as_slice()).collect();
            let plan = compute_merge_plan(&inputs, descending);
            let expected = reference_pick_order(&inputs, descending);
            prop_assert_eq!(expand_plan(&plan), expected.clone());
            // Chunking must be a pure re-partitioning of the same pick order.
            let chunks = split_plan_chunks(&plan, chunk_rows);
            let rejoined: Vec<(usize, usize)> =
                chunks.iter().flat_map(|c| expand_plan(c)).collect();
            prop_assert_eq!(rejoined, expected);
            for c in &chunks {
                let rows: usize = c.iter().map(|r| r.row_count).sum();
                prop_assert!(rows <= chunk_rows);
            }
        }
    }

    proptest! {
        /// The two load-bearing merge invariants over random sorted inputs, in
        /// BOTH directions: (1) the output is globally sorted by timestamp in the
        /// merge direction, and (2) it conserves the EXACT multiset of input
        /// `(timestamp, id)` rows — no drop, no duplicate, no payload mis-pairing.
        #[test]
        fn merge_is_sorted_and_conserves_multiset(
            run_ts in proptest::collection::vec(
                proptest::collection::vec(0i64..50, 0..40), 0..5),
            batch_size in 1usize..6,
            target_rows in 1usize..16,
            descending in proptest::bool::ANY,
        ) {
            let mut next_id = 0i64;
            let mut inputs: Vec<Vec<RecordBatch>> = Vec::new();
            let mut expected: Vec<(i64, i64)> = Vec::new();
            for mut ts in run_ts {
                // Each input is pre-sorted in the merge direction.
                ts.sort_unstable();
                if descending {
                    ts.reverse();
                }
                let rows: Vec<(i64, i64)> = ts.into_iter().map(|t| {
                    let id = next_id; next_id += 1; (t, id)
                }).collect();
                expected.extend(rows.iter().copied());
                let batches: Vec<RecordBatch> = rows.chunks(batch_size).map(batch).collect();
                inputs.push(batches);
            }

            let got = rows_of(&merge_to_vec_dir(inputs, 0, target_rows, descending));

            for w in got.windows(2) {
                if descending {
                    prop_assert!(w[0].0 >= w[1].0, "output not desc-sorted: {:?} then {:?}", w[0], w[1]);
                } else {
                    prop_assert!(w[0].0 <= w[1].0, "output not asc-sorted: {:?} then {:?}", w[0], w[1]);
                }
            }
            let mut got_sorted = got.clone();
            got_sorted.sort_unstable();
            let mut exp_sorted = expected.clone();
            exp_sorted.sort_unstable();
            prop_assert_eq!(got_sorted, exp_sorted);
        }
    }
}
