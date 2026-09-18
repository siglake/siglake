//! #4376 prototype measurement: the shipped whole-file index against the
//! block-compressed seg2 layout, at the compacted 50G layout's per-file scale.
//!
//! `report_segmented_vs_whole_file` is `#[ignore]`d — it builds a multi-million
//! row index and holds a 1 GiB cache. What it reports, and why each column is
//! here:
//!
//! - **size and build**: the format's cost at rest and at write time.
//! - **residency**: `InvertedIndex::heap_size_bytes` against
//!   `SegmentedReader::resident_bytes`. This is the number that decides whether
//!   a 14-file text plan fits a 1 GiB parsed-index budget at all.
//! - **per-shape reads**: range reads and fetched bytes per lookup, from the
//!   [`SliceSource`] counters. Local instrumentation of what a reader *asks*
//!   for — not object-store latency, which needs a prepared round.
//! - **the 14-file plan**: the same lookups driven through a byte-bounded LRU
//!   at the deployed 1 GiB parsed budget, which is where the shipped format
//!   loses (#4329 measured 388 evictions and one resident index).
//!
//! Every arm answers the same rows as the whole-file index, and the harness
//! asserts that before it reports a timing.
//!
//! ```
//! cargo test -p siglake-index --release --test segmented_measure \
//!   report_segmented_vs_whole_file -- --ignored --nocapture
//! ```
//!
//! Sized by `SIGLAKE_SEG_{ROWS_PER_FILE,GROUP_ROWS,FILES,RUNS,RARE_EVERY,PARSED_BYTES,BLOCK_BYTES}`.

use std::time::{Duration, Instant};

use siglake_index::segmented::{
    Lookup, SegmentedReader, SegmentedWriter, SliceSource, DEFAULT_TARGET_BLOCK_BYTES,
};
use siglake_index::{IndexBuilder, InvertedIndex};

/// The measurement corpus's text, row by row, without materializing it: the
/// same shape as `puffin_rebuild.rs`'s `ab_shaped_event` (`bench_shaped_event`
/// plus the sparse term). One token per row is unique to that row, which is
/// what makes a parsed dictionary proportional to the file's rows.
fn raw(row: usize, rare_every: usize) -> String {
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

fn knob(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

/// Bytes at the scale they land: a directory in KiB beside a parsed index in
/// GiB, so a 543x ratio is legible in the same table.
fn mib(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    let bytes = bytes as f64;
    if bytes >= KIB * KIB * KIB {
        format!("{:.2} GiB", bytes / (KIB * KIB * KIB))
    } else if bytes >= KIB * KIB {
        format!("{:.1} MiB", bytes / (KIB * KIB))
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else {
        format!("{bytes:.0} B")
    }
}

/// One shape: the lookup a query's text predicate turns into, and the row
/// groups a time window left for it.
struct Shape {
    name: &'static str,
    term: &'static str,
    /// `None` = the whole file; `Some(fraction)` = the last `fraction` of the
    /// file's row groups, which is what a `last 25%` window predicate prunes
    /// the scan down to.
    tail_fraction: Option<f64>,
    substring: bool,
}

const SHAPES: &[Shape] = &[
    Shape {
        name: "rare_scan",
        term: "rareneedle",
        tail_fraction: None,
        substring: false,
    },
    Shape {
        name: "rare_scan_last25",
        term: "rareneedle",
        tail_fraction: Some(0.25),
        substring: false,
    },
    Shape {
        name: "keyword",
        term: "queen",
        tail_fraction: None,
        substring: false,
    },
    Shape {
        name: "keyword_last25",
        term: "queen",
        tail_fraction: Some(0.25),
        substring: false,
    },
    Shape {
        name: "unique_token",
        term: "000000",
        tail_fraction: None,
        substring: false,
    },
    Shape {
        // A substring no tokenizer produces, so it forces the whole-dictionary
        // sweep rather than a term lookup. It occurs only inside `checkout`.
        name: "substring_scan",
        term: "eckou",
        tail_fraction: None,
        substring: true,
    },
];

fn tail_groups(n_groups: usize, fraction: Option<f64>) -> Option<Vec<usize>> {
    let fraction = fraction?;
    let keep = ((n_groups as f64) * fraction).ceil().max(1.0) as usize;
    Some((n_groups - keep..n_groups).collect())
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

/// A parsed-index cache bounded by bytes, evicting least-recently-used — the
/// shape of the reader's own `ParsedIndexCacheInner`, small enough to drive a
/// 14-file plan here.
struct ByteLru<V> {
    max_bytes: usize,
    bytes: usize,
    clock: u64,
    entries: Vec<(usize, std::sync::Arc<V>, usize, u64)>, // key, value, bytes, last use
    hits: usize,
    misses: usize,
    evictions: usize,
}

impl<V> ByteLru<V> {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            bytes: 0,
            clock: 0,
            entries: Vec::new(),
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    fn get(&mut self, key: usize) -> Option<std::sync::Arc<V>> {
        self.clock += 1;
        let clock = self.clock;
        match self.entries.iter_mut().find(|entry| entry.0 == key) {
            Some(entry) => {
                entry.3 = clock;
                self.hits += 1;
                Some(std::sync::Arc::clone(&entry.1))
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    fn put(&mut self, key: usize, value: std::sync::Arc<V>, bytes: usize) {
        if bytes > self.max_bytes {
            return; // oversized: the reader skips these too
        }
        self.clock += 1;
        while self.bytes + bytes > self.max_bytes && !self.entries.is_empty() {
            let (index, _) = self
                .entries
                .iter()
                .enumerate()
                .min_by_key(|(_, entry)| entry.3)
                .expect("non-empty");
            self.bytes -= self.entries[index].2;
            self.entries.remove(index);
            self.evictions += 1;
        }
        self.bytes += bytes;
        self.entries.push((key, value, bytes, self.clock));
    }
}

#[test]
#[ignore]
fn report_segmented_vs_whole_file() {
    let rows_per_file = knob("SIGLAKE_SEG_ROWS_PER_FILE", 1_000_000);
    let group_rows = knob("SIGLAKE_SEG_GROUP_ROWS", 1_048_576).max(1);
    let files = knob("SIGLAKE_SEG_FILES", 14).max(1);
    let runs = knob("SIGLAKE_SEG_RUNS", 9).max(1);
    let rare_every = knob("SIGLAKE_SEG_RARE_EVERY", 100_000).max(1);
    let parsed_bytes = knob("SIGLAKE_SEG_PARSED_BYTES", 1024 * 1024 * 1024);
    // The plan section re-parses a whole-file index per miss, so it gets its
    // own, smaller execution count.
    let plan_runs = knob("SIGLAKE_SEG_PLAN_RUNS", 3).max(1);
    let block_bytes = knob("SIGLAKE_SEG_BLOCK_BYTES", DEFAULT_TARGET_BLOCK_BYTES);

    println!(
        "\n# segmented sidecar seg2 (#4988)\n\
         rows/file={rows_per_file} group_rows={group_rows} files={files} runs={runs} \
         rare_every={rare_every} parsed_budget={} block_bytes={block_bytes}",
        mib(parsed_bytes as u64)
    );

    // ---- build both formats over one file's rows -------------------------
    let built = Instant::now();
    let mut builder = IndexBuilder::new();
    for row in 0..rows_per_file {
        builder.push_row(&raw(row, rare_every));
    }
    let whole = builder.build();
    let v1_build = built.elapsed();
    let v1_bytes = whole.to_bytes();

    let built = Instant::now();
    let mut writer = SegmentedWriter::new_v2(block_bytes);
    let mut group_start = 0usize;
    while group_start < rows_per_file {
        let group_end = (group_start + group_rows).min(rows_per_file);
        let mut group = IndexBuilder::new();
        for row in group_start..group_end {
            group.push_row(&raw(row, rare_every));
        }
        writer.push_group_index(&group.build());
        group_start = group_end;
    }
    let seg_bytes: std::sync::Arc<[u8]> = writer.finish().into();
    let seg_build = built.elapsed();

    let parse = Instant::now();
    let reparsed = InvertedIndex::from_bytes(&v1_bytes).expect("v1 round-trips");
    let v1_parse = parse.elapsed();
    assert_eq!(reparsed.n_rows(), whole.n_rows());

    let open = Instant::now();
    let reader = SegmentedReader::open(SliceSource::shared(std::sync::Arc::clone(&seg_bytes)))
        .expect("segmented opens");
    let seg_open = open.elapsed();
    assert_eq!(reader.n_rows() as usize, rows_per_file);

    println!(
        "\n## one file ({} rows, {} terms, {} row groups)\n\n\
         | format | build | serialized | parse/open | resident |\n\
         |---|---:|---:|---:|---:|\n\
         | whole-file v1 | {:?} | {} | {:?} | {} |\n\
         | seg2 | {:?} | {} | {:?} | {} |\n\
         \nserialized ratio {:.2}x · resident ratio {:.0}x smaller · \
         dictionary {} · postings {} · directory {}",
        whole.n_rows(),
        whole.n_terms(),
        reader.n_groups(),
        v1_build,
        mib(v1_bytes.len() as u64),
        v1_parse,
        mib(whole.heap_size_bytes() as u64),
        seg_build,
        mib(seg_bytes.len() as u64),
        seg_open,
        mib(reader.resident_bytes() as u64),
        seg_bytes.len() as f64 / v1_bytes.len() as f64,
        whole.heap_size_bytes() as f64 / reader.resident_bytes() as f64,
        mib(reader.dictionary_bytes()),
        mib(reader.postings_bytes()),
        mib(seg_bytes.len() as u64 - reader.dictionary_bytes() - reader.postings_bytes()),
    );

    // ---- per-shape reads and warm timings, one file ----------------------
    println!(
        "\n## per shape, one file, index work only (warm: the reader is open, \
         the v1 index is parsed)\n\n\
         | shape | rows | v1 warm | seg warm | seg reads | seg fetched | seg fetched ÷ blob |\n\
         |---|---:|---:|---:|---:|---:|---:|"
    );
    let groups_for = |shape: &Shape| tail_groups(reader.n_groups(), shape.tail_fraction);
    for shape in SHAPES {
        let groups = groups_for(shape);
        let group_slice = groups.as_deref();
        // Exactness first: the segmented answer is the v1 answer, restricted
        // to the row groups the shape kept.
        let expected: Vec<u32> = if shape.substring {
            whole.rows_containing(shape.term).expect("answerable")
        } else {
            whole.postings(shape.term).unwrap_or(&[]).to_vec()
        };
        let expected = match group_slice {
            None => expected,
            Some(selected) => {
                let first = selected[0] as u32 * group_rows as u32;
                expected.into_iter().filter(|row| *row >= first).collect()
            }
        };
        let actual = if shape.substring {
            reader
                .rows_containing_in_groups(shape.term, group_slice)
                .expect("answerable")
        } else {
            match reader.postings_in_groups(shape.term, group_slice) {
                Lookup::Rows(rows) => rows,
                Lookup::Absent => Vec::new(),
                Lookup::Unanswerable => panic!("{}: unanswerable", shape.name),
            }
        };
        assert_eq!(actual, expected, "{}: rows differ from v1", shape.name);

        let mut v1_samples = Vec::with_capacity(runs);
        let mut seg_samples = Vec::with_capacity(runs);
        reader.source().reset_counters();
        for _ in 0..runs {
            let timed = Instant::now();
            let got = if shape.substring {
                whole.rows_containing(shape.term).unwrap()
            } else {
                whole.postings(shape.term).unwrap_or(&[]).to_vec()
            };
            v1_samples.push(timed.elapsed());
            std::hint::black_box(got);

            let timed = Instant::now();
            let got = if shape.substring {
                reader
                    .rows_containing_in_groups(shape.term, group_slice)
                    .unwrap()
            } else {
                reader
                    .postings_in_groups(shape.term, group_slice)
                    .rows()
                    .unwrap()
            };
            seg_samples.push(timed.elapsed());
            std::hint::black_box(got);
        }
        let fetched = reader.source().bytes_read() / runs as u64;
        println!(
            "| {} | {} | {:?} | {:?} | {} | {} | {:.4}% |",
            shape.name,
            actual.len(),
            median(v1_samples),
            median(seg_samples),
            reader.source().reads() / runs as u64,
            mib(fetched),
            100.0 * fetched as f64 / seg_bytes.len() as f64,
        );
    }

    // ---- the plan: `files` logical files behind one parsed-index budget ---
    // Every logical file carries the same blob bytes; only the cache key
    // differs. That is exactly the working-set-to-budget ratio a 14-file text
    // plan puts on the cache, without building 14 distinct corpora.
    // Cold and warm are reported apart, not medianed together: the first
    // execution is the one that opens every reader and decodes every index, and
    // for the segmented arm it is the only one that pays a directory read.
    println!(
        "\n## a {files}-file plan under a {} parsed-index budget\n\n\
         {plan_runs} executions per shape; `cold` is the first, `warm` the median of the rest.\n\n\
         | shape | v1 cold | v1 warm | v1 hits/misses/evictions | seg cold | seg warm | seg hits/misses/evictions | seg cold fetched | seg warm fetched |\n\
         |---|---:|---:|---:|---:|---:|---:|---:|---:|",
        mib(parsed_bytes as u64)
    );
    for shape in SHAPES {
        let groups = groups_for(shape);
        let group_slice = groups.as_deref();

        let mut v1_cache: ByteLru<InvertedIndex> = ByteLru::new(parsed_bytes);
        let mut v1_samples = Vec::with_capacity(plan_runs);
        for _ in 0..plan_runs {
            let timed = Instant::now();
            for file in 0..files {
                let index = match v1_cache.get(file) {
                    Some(index) => index,
                    None => {
                        let parsed =
                            std::sync::Arc::new(InvertedIndex::from_bytes(&v1_bytes).unwrap());
                        v1_cache.put(
                            file,
                            std::sync::Arc::clone(&parsed),
                            parsed.heap_size_bytes(),
                        );
                        parsed
                    }
                };
                let rows = if shape.substring {
                    index.rows_containing(shape.term).unwrap()
                } else {
                    index.postings(shape.term).unwrap_or(&[]).to_vec()
                };
                std::hint::black_box(rows);
            }
            v1_samples.push(timed.elapsed());
        }

        let mut seg_cache: ByteLru<SegmentedReader<SliceSource>> = ByteLru::new(parsed_bytes);
        let mut seg_samples = Vec::with_capacity(plan_runs);
        let mut seg_fetched_per_run = Vec::with_capacity(plan_runs);
        for _ in 0..plan_runs {
            let mut seg_fetched = 0u64;
            let timed = Instant::now();
            for file in 0..files {
                let reader = match seg_cache.get(file) {
                    Some(reader) => reader,
                    None => {
                        // A cold file pays the trailer + directory read; the
                        // blob bytes stand in for the object-store fetch, which
                        // this harness deliberately does not model.
                        let opened = std::sync::Arc::new(
                            SegmentedReader::open(SliceSource::shared(std::sync::Arc::clone(
                                &seg_bytes,
                            )))
                            .unwrap(),
                        );
                        let resident = opened.resident_bytes();
                        seg_cache.put(file, std::sync::Arc::clone(&opened), resident);
                        opened
                    }
                };
                let rows = if shape.substring {
                    reader
                        .rows_containing_in_groups(shape.term, group_slice)
                        .unwrap()
                } else {
                    reader
                        .postings_in_groups(shape.term, group_slice)
                        .rows()
                        .unwrap()
                };
                std::hint::black_box(rows);
                seg_fetched += reader.source().bytes_read();
                reader.source().reset_counters();
            }
            seg_samples.push(timed.elapsed());
            seg_fetched_per_run.push(seg_fetched);
        }

        // `cold` is the first execution; `warm` the median of what follows it,
        // or the same sample when only one execution was asked for.
        let split = |samples: &[Duration]| -> (Duration, Duration) {
            let cold = samples[0];
            let warm = if samples.len() > 1 {
                median(samples[1..].to_vec())
            } else {
                cold
            };
            (cold, warm)
        };
        let (v1_cold, v1_warm) = split(&v1_samples);
        let (seg_cold, seg_warm) = split(&seg_samples);
        let warm_fetched = if seg_fetched_per_run.len() > 1 {
            seg_fetched_per_run[1..].iter().sum::<u64>() / (seg_fetched_per_run.len() - 1) as u64
        } else {
            seg_fetched_per_run[0]
        };
        println!(
            "| {} | {:?} | {:?} | {}/{}/{} | {:?} | {:?} | {}/{}/{} | {} | {} |",
            shape.name,
            v1_cold,
            v1_warm,
            v1_cache.hits,
            v1_cache.misses,
            v1_cache.evictions,
            seg_cold,
            seg_warm,
            seg_cache.hits,
            seg_cache.misses,
            seg_cache.evictions,
            mib(seg_fetched_per_run[0]),
            mib(warm_fetched),
        );
    }
    println!(
        "\nv1 resident working set for {files} files: {} · seg2: {}",
        mib((whole.heap_size_bytes() * files) as u64),
        mib((reader.resident_bytes() * files) as u64),
    );
}

/// Exhaustive single-bit corruption sweep, both formats, as a run-on-request
/// report. The default-run assertion of the same property is
/// `a_corrupt_section_makes_the_lookup_unanswerable_not_empty` in the codec's
/// own tests; this one prints the rates the design document quotes.
#[test]
#[ignore]
fn report_single_bit_corruption_rates() {
    let rows: Vec<String> = (0..knob("SIGLAKE_SEG_CORRUPT_ROWS", 1_000))
        .map(|row| raw(row, 997))
        .collect();
    let group_rows = knob("SIGLAKE_SEG_GROUP_ROWS", 250).max(1) as u32;

    let whole = InvertedIndex::from_rows(rows.iter().map(String::as_str));
    let truth = whole.postings("queen").expect("present").to_vec();

    let v1 = whole.to_bytes();
    let mut v1_counts = [0usize; 5]; // refused, same, wrong, outside domain, absent
    for byte in 0..v1.len() {
        for bit in 0..8u32 {
            let mut corrupt = v1.clone();
            corrupt[byte] ^= 1 << bit;
            match InvertedIndex::from_bytes(&corrupt) {
                None => v1_counts[0] += 1,
                Some(index) => match index.postings("queen") {
                    Some(got) if got == truth.as_slice() => v1_counts[1] += 1,
                    Some(got) => {
                        v1_counts[2] += 1;
                        if got.iter().any(|row| *row >= index.n_rows()) {
                            v1_counts[3] += 1;
                        }
                    }
                    None => v1_counts[4] += 1,
                },
            }
        }
    }

    let seg =
        siglake_index::segmented::encode_from_rows(rows.iter().map(String::as_str), group_rows);
    let mut seg_counts = [0usize; 5];
    for byte in 0..seg.len() {
        for bit in 0..8u32 {
            let mut corrupt = seg.clone();
            corrupt[byte] ^= 1 << bit;
            match SegmentedReader::open(SliceSource::new(corrupt)) {
                None => seg_counts[0] += 1,
                Some(reader) => match reader.postings("queen") {
                    Lookup::Rows(got) if got == truth => seg_counts[1] += 1,
                    Lookup::Rows(got) => {
                        seg_counts[2] += 1;
                        if got.iter().any(|row| *row >= reader.n_rows()) {
                            seg_counts[3] += 1;
                        }
                    }
                    Lookup::Unanswerable => seg_counts[0] += 1,
                    Lookup::Absent => seg_counts[4] += 1,
                },
            }
        }
    }

    println!(
        "\n| format | blob | flips | refused | unchanged answer | wrong rows | outside row domain | reported absent |\n\
         |---|---:|---:|---:|---:|---:|---:|---:|\n\
         | whole-file v1 | {} | {} | {} | {} | {} | {} | {} |\n\
         | segmented | {} | {} | {} | {} | {} | {} | {} |",
        v1.len(),
        v1.len() * 8,
        v1_counts[0],
        v1_counts[1],
        v1_counts[2],
        v1_counts[3],
        v1_counts[4],
        seg.len(),
        seg.len() * 8,
        seg_counts[0],
        seg_counts[1],
        seg_counts[2],
        seg_counts[3],
        seg_counts[4],
    );
}
