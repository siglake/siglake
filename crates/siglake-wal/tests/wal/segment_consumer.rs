//! The external-consumer interface: the contract an out-of-tree detector,
//! router or mirror depends on.
//!
//! These are the guarantees the module documents, each asserted rather than
//! described. siglake's own detection pipeline used to consume the WAL through
//! the raw primitives; when it moved out of the tree, this interface is what
//! stayed behind for arbitrary consumers, so its promises have to hold.

use std::time::Duration;

use siglake_core::Event;
use siglake_wal::consumer::SegmentConsumer;
use siglake_wal::{
    claim_segment, finish_segment, list_sealed, sweep_committed_coordinated, WalWriter,
};

fn synth(n: usize) -> Vec<Event> {
    (0..n).map(|i| Event::now(format!("e{i}"))).collect()
}

/// Seal `n` segments, one event each, and return the WAL dir.
fn seal_segments(dir: &std::path::Path, n: usize) {
    // One event per segment: the threshold is 1, so every append rolls.
    let mut w = WalWriter::with_thresholds(dir, "ing-1", 1, Duration::from_secs(3600)).unwrap();
    for e in synth(n) {
        w.append_events(&[e]).unwrap();
    }
}

#[test]
fn a_consumer_reads_each_segment_once_and_resumes_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    seal_segments(tmp.path(), 3);
    let sealed = list_sealed(tmp.path()).unwrap();
    assert!(
        sealed.len() >= 3,
        "fixture sealed {} segments",
        sealed.len()
    );

    let mut c = SegmentConsumer::open("test-consumer", tmp.path(), state.path()).unwrap();
    let first = c.poll().unwrap();
    assert_eq!(
        first.len(),
        sealed.len(),
        "first poll must offer everything"
    );

    // Process and commit only the first.
    let batches = c.read(&first[0]).unwrap();
    assert!(!batches.is_empty(), "segment decoded to no batches");
    c.commit(&first[0]).unwrap();

    // The committed one is not offered again.
    let second = c.poll().unwrap();
    assert_eq!(second.len(), first.len() - 1);
    assert!(!second.iter().any(|s| s.name == first[0].name));

    // A restart resumes from the cursor rather than re-reading everything --
    // the whole point of a durable position.
    drop(c);
    let c2 = SegmentConsumer::open("test-consumer", tmp.path(), state.path()).unwrap();
    assert_eq!(c2.position(), Some(first[0].name.as_str()));
    assert_eq!(c2.poll().unwrap().len(), first.len() - 1);
}

/// A crash between processing and commit re-delivers. That is the documented
/// at-least-once contract, and a consumer builds idempotence on top of it.
#[test]
fn an_uncommitted_segment_is_redelivered() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    seal_segments(tmp.path(), 1);

    let c = SegmentConsumer::open("test-consumer", tmp.path(), state.path()).unwrap();
    let seg = c.poll().unwrap().first().cloned().expect("one segment");
    let _ = c.read(&seg).unwrap();
    // ... crash here, before commit ...
    drop(c);

    let c2 = SegmentConsumer::open("test-consumer", tmp.path(), state.path()).unwrap();
    assert_eq!(
        c2.poll().unwrap().first().map(|s| s.name.clone()),
        Some(seg.name),
        "an uncommitted segment was not re-delivered after restart"
    );
}

/// The compactor renames a segment out from under a consumer mid-cycle. It must
/// still be readable: this is why the listing spans sealed/, processing/ and
/// committed/ rather than just sealed/.
#[test]
fn a_segment_renamed_by_the_compactor_is_still_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    seal_segments(tmp.path(), 1);

    let c = SegmentConsumer::open("test-consumer", tmp.path(), state.path()).unwrap();
    let seg = c.poll().unwrap().first().cloned().expect("one segment");

    // The compactor claims and commits it while we hold a stale path.
    let claimed = claim_segment(&seg.path).unwrap();
    let committed = finish_segment(&claimed).unwrap();
    assert!(committed.exists());
    assert!(!seg.path.exists(), "fixture did not actually move the file");

    let batches = c
        .read(&seg)
        .expect("a segment renamed by the compactor must still be readable");
    assert!(!batches.is_empty());
}

/// Committing publishes the watermark that holds retention open, so a
/// consumer's un-processed segments are not swept out from under it.
#[test]
fn retention_waits_for_a_live_consumer_and_not_for_a_dead_one() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    seal_segments(tmp.path(), 2);

    let mut c = SegmentConsumer::open("test-consumer", tmp.path(), state.path()).unwrap();
    let segs = c.poll().unwrap();
    assert!(segs.len() >= 2);

    // Move both into committed/ so retention is even eligible to sweep them.
    for s in &segs {
        let claimed = claim_segment(&s.path).unwrap();
        finish_segment(&claimed).unwrap();
    }
    // Acknowledge only the FIRST: the consumer is live but behind.
    c.commit(&segs[0]).unwrap();

    // Zero soft retention, a generous ceiling, and a stale window this
    // just-published watermark is well inside: the sweep must respect it.
    let swept = sweep_committed_coordinated(
        tmp.path(),
        Duration::ZERO,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
    )
    .unwrap();
    let remaining = std::fs::read_dir(tmp.path().join("committed"))
        .unwrap()
        .count();
    assert!(
        remaining >= segs.len() - 1,
        "retention swept {swept} segments past a live consumer's watermark, leaving {remaining}"
    );

    // Now treat the consumer as dead: a zero stale window makes its watermark
    // count as absent, and the WAL stops waiting. Otherwise one stuck consumer
    // would grow an ingester's disk without bound.
    sweep_committed_coordinated(
        tmp.path(),
        Duration::ZERO,
        Duration::from_secs(3600),
        Duration::ZERO,
    )
    .unwrap();
    let after = std::fs::read_dir(tmp.path().join("committed"))
        .unwrap()
        .count();
    assert!(
        after < remaining,
        "a stale consumer's watermark still held retention open: {remaining} -> {after}"
    );
}

/// The cursor must never move backwards, or a consumer that acknowledged out of
/// order would re-open a window it had already closed.
#[test]
fn committing_an_older_segment_does_not_rewind_the_cursor() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    seal_segments(tmp.path(), 3);

    let mut c = SegmentConsumer::open("test-consumer", tmp.path(), state.path()).unwrap();
    let segs = c.poll().unwrap();
    c.commit(&segs[segs.len() - 1]).unwrap();
    let ahead = c.position().map(str::to_string);
    c.commit(&segs[0]).unwrap();
    assert_eq!(
        c.position().map(str::to_string),
        ahead,
        "committing an older segment rewound the cursor"
    );
    assert!(c.poll().unwrap().is_empty());
}

/// The consumer id becomes a file name in `consumers/`, so it is validated
/// rather than trusted -- a traversal there would write outside the WAL dir.
#[test]
fn a_bad_consumer_id_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    for bad in ["", "../escape", "with/slash", "with space"] {
        assert!(
            SegmentConsumer::open(bad, tmp.path(), state.path()).is_err(),
            "accepted a consumer id that becomes a path component: {bad:?}"
        );
    }
    assert!(SegmentConsumer::open("ok-id_1", tmp.path(), state.path()).is_ok());
}

/// A consumer pointed one directory too high must SAY SO.
///
/// THE DEFECT THIS GUARDS. siglake's layout is `<wal>/<tenant>/` and
/// `<wal>/<tenant>/<index>/`, and the root also carries empty
/// `active/ sealed/ processing/ committed/` stubs created at startup. So there
/// are several plausible paths to point a consumer at, only one is right for a
/// given stream, and choosing wrong is SILENT: `list_visible` on a directory
/// whose `sealed/` is empty returns no segments and no error.
///
/// Measured on the 2026-08-30 200G round: a detector pointed at the WAL root
/// ran 489 clean cycles reporting `segments_pending 0` while 9,376 segments
/// were sealed one directory below it. No metric distinguished that from an
/// idle system — which is the whole problem.
#[test]
fn a_consumer_pointed_above_the_segments_reports_it() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    // The real shape: empty layout stubs at the root, segments under
    // <root>/<tenant>/<index>/.
    for sub in ["active", "sealed", "processing", "committed"] {
        std::fs::create_dir_all(tmp.path().join(sub)).unwrap();
    }
    let index_dir = tmp.path().join("default").join("logs-bench");
    seal_segments(&index_dir, 2);

    let found = siglake_wal::consumer::consumable_dirs(tmp.path()).unwrap();
    assert_eq!(
        found,
        vec![index_dir.clone()],
        "the directory that actually holds segments was not found"
    );

    let root = SegmentConsumer::open("c", tmp.path(), state.path()).unwrap();
    assert!(
        root.looks_misdirected(),
        "a consumer at the WAL root saw no segments and did not report being misdirected"
    );
    assert!(
        root.poll().unwrap().is_empty(),
        "fixture: the root should genuinely hold nothing"
    );

    // Pointed correctly, it is not flagged and it sees the segments.
    let ok = SegmentConsumer::open("c2", &index_dir, state.path().join("c2")).unwrap();
    assert!(
        !ok.looks_misdirected(),
        "a correctly-pointed consumer was flagged as misdirected"
    );
    assert_eq!(ok.poll().unwrap().len(), 2);
}
