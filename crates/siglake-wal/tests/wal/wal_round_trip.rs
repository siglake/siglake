use std::time::Duration;

use siglake_core::Event;
use siglake_wal::{
    claim_segment, finish_segment, list_sealed, list_visible, read_segment,
    read_segment_from_bytes, recover_orphaned_partials, recover_orphaned_processing,
    release_segment, segment_owner, sweep_committed, WalWriter, ACTIVE_DIR, COMMITTED_DIR,
    ORPHANS_DIR, PROCESSING_DIR, SEALED_DIR,
};

fn synth(n: usize) -> Vec<Event> {
    (0..n).map(|i| Event::now(format!("e{i}"))).collect()
}

#[test]
fn rolls_at_size_threshold() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w =
        WalWriter::with_thresholds(tmp.path(), "ing-1", 100, Duration::from_secs(60)).unwrap();

    // 99 events: no roll.
    let sealed = w.append_events(&synth(99)).unwrap();
    assert!(sealed.is_none(), "should not roll at 99 events");
    assert_eq!(list_sealed(tmp.path()).unwrap().len(), 0);

    // 1 more: hits 100, rolls.
    let sealed = w.append_events(&synth(1)).unwrap();
    assert!(sealed.is_some());
    let seg = sealed.unwrap();
    assert_eq!(seg.rows, 100);
    assert_eq!(list_sealed(tmp.path()).unwrap().len(), 1);
}

#[test]
fn rolls_at_age_threshold_via_tick() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w =
        WalWriter::with_thresholds(tmp.path(), "ing-1", 1_000_000, Duration::from_millis(50))
            .unwrap();

    w.append_events(&synth(5)).unwrap();
    assert_eq!(list_sealed(tmp.path()).unwrap().len(), 0);

    std::thread::sleep(Duration::from_millis(80));
    let sealed = w.tick().unwrap();
    assert!(sealed.is_some(), "tick should age-roll");
    assert_eq!(list_sealed(tmp.path()).unwrap().len(), 1);
}

#[test]
fn empty_seal_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w = WalWriter::new(tmp.path(), "ing-1").unwrap();
    assert!(w.seal().unwrap().is_none());
    assert_eq!(list_sealed(tmp.path()).unwrap().len(), 0);
}

#[test]
fn round_trip_through_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w =
        WalWriter::with_thresholds(tmp.path(), "ing-1", 50, Duration::from_secs(60)).unwrap();

    let events = synth(50);
    let sealed = w.append_events(&events).unwrap().unwrap();

    let batches = read_segment(&sealed.path).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 50);
}

#[test]
fn claim_and_release_use_atomic_renames() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w =
        WalWriter::with_thresholds(tmp.path(), "ing-1", 10, Duration::from_secs(60)).unwrap();
    let seg = w.append_events(&synth(10)).unwrap().unwrap();

    let claimed = claim_segment(&seg.path).unwrap();
    assert!(claimed.starts_with(tmp.path().join(PROCESSING_DIR)));
    assert!(!seg.path.exists(), "original sealed path should be gone");
    assert!(claimed.exists(), "claimed path should exist");

    let released = release_segment(&claimed).unwrap();
    assert!(released.starts_with(tmp.path().join(SEALED_DIR)));
    assert!(!claimed.exists());
    assert!(released.exists());
}

#[test]
fn finish_segment_moves_to_committed() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w =
        WalWriter::with_thresholds(tmp.path(), "ing-1", 5, Duration::from_secs(60)).unwrap();
    let seg = w.append_events(&synth(5)).unwrap().unwrap();
    let claimed = claim_segment(&seg.path).unwrap();

    let finished = finish_segment(&claimed).unwrap();
    assert!(finished.starts_with(tmp.path().join(COMMITTED_DIR)));
    assert!(!claimed.exists());
    assert!(finished.exists());

    // The detector's list_visible should see the committed segment.
    let visible = list_visible(tmp.path()).unwrap();
    assert_eq!(visible.len(), 1);
    assert!(visible[0].starts_with(tmp.path().join(COMMITTED_DIR)));
}

#[test]
fn sweep_committed_respects_retention() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w =
        WalWriter::with_thresholds(tmp.path(), "ing-1", 5, Duration::from_secs(60)).unwrap();
    let seg = w.append_events(&synth(5)).unwrap().unwrap();
    let claimed = claim_segment(&seg.path).unwrap();
    let finished = finish_segment(&claimed).unwrap();

    // With a 1-hour retention, the just-committed segment should remain.
    let deleted = sweep_committed(tmp.path(), Duration::from_secs(3600)).unwrap();
    assert_eq!(deleted, 0);
    assert!(finished.exists());

    // With zero retention, it gets swept.
    let deleted = sweep_committed(tmp.path(), Duration::ZERO).unwrap();
    assert_eq!(deleted, 1);
    assert!(!finished.exists());
}

#[test]
fn list_visible_dedupes_across_subdirs() {
    let tmp = tempfile::tempdir().unwrap();
    let mut w =
        WalWriter::with_thresholds(tmp.path(), "ing-1", 5, Duration::from_secs(60)).unwrap();
    // Build three segments in three different states: sealed/, processing/, committed/.
    for _ in 0..3 {
        w.append_events(&synth(5)).unwrap().unwrap();
    }
    let sealed = list_sealed(tmp.path()).unwrap();
    assert_eq!(sealed.len(), 3);
    let claimed = claim_segment(&sealed[0]).unwrap();
    let claimed2 = claim_segment(&sealed[1]).unwrap();
    let _finished = finish_segment(&claimed2).unwrap();

    let visible = list_visible(tmp.path()).unwrap();
    assert_eq!(
        visible.len(),
        3,
        "expected 3 distinct segments across all 3 dirs"
    );
    let by_dir: Vec<_> = visible
        .iter()
        .map(|p| p.parent().unwrap().file_name().unwrap().to_str().unwrap())
        .collect();
    assert!(by_dir.contains(&SEALED_DIR));
    assert!(by_dir.contains(&PROCESSING_DIR));
    assert!(by_dir.contains(&COMMITTED_DIR));

    // Suppress unused-variable warning for claimed (kept claimed in processing/).
    drop(claimed);
}

#[test]
fn dropping_writer_seals_pending() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut w =
            WalWriter::with_thresholds(tmp.path(), "ing-1", 1_000_000, Duration::from_secs(60))
                .unwrap();
        w.append_events(&synth(7)).unwrap();
        // No explicit seal; let Drop handle it.
    }
    let sealed = list_sealed(tmp.path()).unwrap();
    assert_eq!(sealed.len(), 1, "drop should seal pending segment");
    let batches = read_segment(&sealed[0]).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 7);
}

// ---- Phase 4.13d: durability tests ---------------------------------------

/// `append_batch` must flush the BufWriter to the OS so a SIGKILL
/// between seals doesn't drop already-accepted events. We can't
/// SIGKILL a unit-test process, so the proxy is: after an append,
/// the file on disk must already have non-zero bytes (the BufWriter
/// would have buffered them in memory otherwise).
#[test]
fn append_batch_flushes_to_os_immediately() {
    use std::fs;
    let tmp = tempfile::tempdir().unwrap();
    let mut w = WalWriter::with_thresholds(
        tmp.path(),
        "ing-1",
        // High threshold so we DON'T trigger a seal.
        1_000_000,
        Duration::from_secs(60),
    )
    .unwrap();
    w.append_events(&synth(50)).unwrap();
    // The active partial file should exist with non-zero size.
    let active_dir = tmp.path().join(ACTIVE_DIR);
    let mut found = None;
    for entry in fs::read_dir(&active_dir).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|s| s.to_str()) == Some("partial") {
            found = Some(p);
            break;
        }
    }
    let partial = found.expect("active partial file present");
    let size = fs::metadata(&partial).unwrap().len();
    assert!(
        size > 0,
        "BufWriter must have flushed to OS after append; got 0 bytes on disk"
    );
}

/// `recover_orphaned_partials` promotes `.arrow.partial` files left
/// behind by a SIGKILLed WalWriter to `.arrow` in `sealed/`. The
/// recovered segments must read back as a valid Arrow IPC stream
/// even though they never received the EOS marker.
#[test]
fn recover_orphaned_partials_promotes_to_sealed() {
    use std::fs;
    let tmp = tempfile::tempdir().unwrap();
    // Simulate a SIGKILL-during-write: open a writer, append events,
    // then `std::mem::forget` so Drop's seal() doesn't run.
    {
        let mut w =
            WalWriter::with_thresholds(tmp.path(), "ing-1", 1_000_000, Duration::from_secs(60))
                .unwrap();
        w.append_events(&synth(17)).unwrap();
        // Skip Drop — partial file stays in active/.
        std::mem::forget(w);
    }
    assert_eq!(list_sealed(tmp.path()).unwrap().len(), 0);
    let active_dir = tmp.path().join(ACTIVE_DIR);
    let partials: Vec<_> = fs::read_dir(&active_dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension().and_then(|s| s.to_str()) == Some("partial")).then_some(p)
        })
        .collect();
    assert_eq!(partials.len(), 1, "exactly one orphaned partial");

    // Recovery runs.
    let recovered = recover_orphaned_partials(tmp.path(), "ing-1").unwrap();
    assert_eq!(recovered, 1);

    // Now in sealed/ with the right contents.
    let sealed = list_sealed(tmp.path()).unwrap();
    assert_eq!(sealed.len(), 1);
    let batches = read_segment(&sealed[0]).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 17,
        "recovered partial must contain its original events"
    );

    // active/ is empty after recovery.
    let leftover: Vec<_> = fs::read_dir(&active_dir).unwrap().collect();
    assert_eq!(leftover.len(), 0, "active/ cleaned up after recovery");
}

/// Recovery runs automatically when a fresh `WalWriter` opens
/// against a directory that already has orphaned partials. That's
/// the path SIGTERM → pod-restart actually exercises in
/// production.
#[test]
fn fresh_writer_recovers_partials_on_open() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut w =
            WalWriter::with_thresholds(tmp.path(), "ing-old", 1_000_000, Duration::from_secs(60))
                .unwrap();
        w.append_events(&synth(42)).unwrap();
        std::mem::forget(w);
    }
    // A DIFFERENT writer id opening the same directory cannot tell "abandoned
    // by a dead pod" from "open right now on another node" — every ingester
    // replica mounts the same RWX PVC. So it does not touch it yet.
    let _w = WalWriter::with_thresholds(tmp.path(), "ing-new", 1_000_000, Duration::from_secs(60))
        .unwrap();
    assert_eq!(
        list_sealed(tmp.path()).unwrap().len(),
        0,
        "another writer's fresh partial must be left alone — it may be live"
    );

    // Backdate it past the adoption threshold: now no live writer could have
    // left it this way (a live one age-rolls every few seconds), so the rows
    // are recovered rather than stranded.
    backdate_partials(tmp.path(), Duration::from_secs(3600));
    let recovered = recover_orphaned_partials(tmp.path(), "ing-new").unwrap();
    assert_eq!(
        recovered, 1,
        "an abandoned partial must eventually be adopted"
    );
    let sealed = list_sealed(tmp.path()).unwrap();
    assert_eq!(sealed.len(), 1);
    let batches = read_segment(&sealed[0]).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 42);
}

/// Set every `active/*.partial` mtime `age` into the past.
fn backdate_partials(dir: &std::path::Path, age: Duration) {
    let when = std::time::SystemTime::now() - age;
    for e in std::fs::read_dir(dir.join(ACTIVE_DIR)).unwrap() {
        let p = e.unwrap().path();
        if p.extension().and_then(|s| s.to_str()) == Some("partial") {
            let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
            f.set_times(std::fs::FileTimes::new().set_modified(when))
                .unwrap();
        }
    }
}

/// The defect this ownership test exists for: a starting replica must not
/// promote a live sibling's OPEN partial into `sealed/`.
///
/// Only the segment FILENAME is pod-scoped; the directory is shared, and the
/// chart mounts one RWX PVC across every ingester replica while actively
/// recommending replicas > 1. Promoting a live partial does not merely publish
/// early: the original writer keeps appending to the same inode, so the file
/// GROWS while the compactor lists, claims and commits it, and the writer then
/// seals its own FULL row set over the same name. If the promoted copy was
/// already claimed, the overlapping prefix commits twice.
///
/// Against the old unconditional promotion this test FAILS.
#[test]
fn a_starting_replica_does_not_steal_a_live_siblings_partial() {
    // Pod A is live and holding an open, non-empty partial.
    let tmp = tempfile::tempdir().unwrap();
    let mut a = WalWriter::with_thresholds(tmp.path(), "pod-a", 1_000_000, Duration::from_secs(60))
        .unwrap();
    a.append_events(&synth(10)).unwrap();

    // Pod B starts on the same shared WAL directory.
    let mut b = WalWriter::with_thresholds(tmp.path(), "pod-b", 1_000_000, Duration::from_secs(60))
        .unwrap();
    assert_eq!(
        list_sealed(tmp.path()).unwrap().len(),
        0,
        "pod B must not have promoted pod A's open partial"
    );

    // A goes on writing and seals its own segment, whole.
    a.append_events(&synth(7)).unwrap();
    let seg = a.seal().unwrap().expect("A seals its own segment");
    let rows: usize = read_segment(&seg.path)
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        rows, 17,
        "A's segment must hold every row A wrote, exactly once"
    );

    // And B's own writes are independent.
    b.append_events(&synth(3)).unwrap();
    let bseg = b.seal().unwrap().expect("B seals its own segment");
    let brows: usize = read_segment(&bseg.path)
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(brows, 3);
    assert_ne!(
        seg.path, bseg.path,
        "the two writers must not share a segment"
    );
}

/// A pod restarting IN PLACE keeps its identity (a StatefulSet ordinal, or any
/// stable hostname), so its own prior partial is unambiguously its own and is
/// recovered at once — no adoption wait.
#[test]
fn a_writer_recovers_its_own_prior_partial_immediately() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut w =
            WalWriter::with_thresholds(tmp.path(), "pod-0", 1_000_000, Duration::from_secs(60))
                .unwrap();
        w.append_events(&synth(5)).unwrap();
        std::mem::forget(w);
    }
    let _w = WalWriter::with_thresholds(tmp.path(), "pod-0", 1_000_000, Duration::from_secs(60))
        .unwrap();
    assert_eq!(
        list_sealed(tmp.path()).unwrap().len(),
        1,
        "a writer's own prior partial needs no adoption wait"
    );
}

/// Zero-byte partials owned by the recovering writer get cleaned up rather
/// than promoted — promoting them would create empty sealed segments that
/// confuse the compactor.
#[test]
fn recover_skips_zero_byte_partials() {
    use std::fs::File;
    let tmp = tempfile::tempdir().unwrap();
    let active_dir = tmp.path().join(ACTIVE_DIR);
    std::fs::create_dir_all(&active_dir).unwrap();
    let partial = active_dir.join("ing-1-empty.arrow.partial");
    File::create(&partial).unwrap();
    let recovered = recover_orphaned_partials(tmp.path(), "ing-1").unwrap();
    assert_eq!(recovered, 0);
    assert!(!partial.exists(), "empty partial removed");
    assert_eq!(list_sealed(tmp.path()).unwrap().len(), 0);
}

// ---- #3049: a fsynced batch followed by a torn append --------------------

/// Owner id for the torn-append fixtures: recovery must see them as its own.
const TORN_OWNER: &str = "ing-torn";

/// The table identity the fixture's frame header carries. Recovery renames the
/// partial without rewriting its header, so this must survive every case.
fn torn_table_uuid() -> uuid::Uuid {
    uuid::Uuid::parse_str("0198d3c4-0000-7000-8000-00000000abcd").unwrap()
}

/// Build one active partial with two appends, each flushed AND fsynced, and
/// return `(bytes, prefix_len)` — `prefix_len` is one past the first append's
/// record-batch message, i.e. exactly where a crash during the second append
/// leaves a torn message.
///
/// The writer is `forget`ten rather than sealed: a sealed segment is a
/// reframed, CRC-covered file, and what we want is the bytes a SIGKILL leaves
/// in `active/`.
fn fsynced_partial_with_second_append() -> (Vec<u8>, usize) {
    use std::fs;
    let tmp = tempfile::tempdir().unwrap();
    let mut w =
        WalWriter::with_thresholds(tmp.path(), TORN_OWNER, 1_000_000, Duration::from_secs(3600))
            .unwrap();
    w.bind_table_uuid(Some(torn_table_uuid())).unwrap();

    w.append_events(&synth(3)).unwrap();
    assert!(w.sync_active().unwrap(), "first append must fsync");
    let partial = sole_partial(tmp.path());
    let prefix = fs::read(&partial).unwrap();

    w.append_events(&synth(5)).unwrap();
    assert!(w.sync_active().unwrap(), "second append must fsync");
    let both = fs::read(&partial).unwrap();
    std::mem::forget(w);

    assert!(
        both.len() > prefix.len(),
        "the second append must have added bytes"
    );
    (both, prefix.len())
}

/// The sole `active/*.arrow.partial` under `dir`.
fn sole_partial(dir: &std::path::Path) -> std::path::PathBuf {
    let mut found: Vec<_> = std::fs::read_dir(dir.join(ACTIVE_DIR))
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension().and_then(|s| s.to_str()) == Some("partial")).then_some(p)
        })
        .collect();
    assert_eq!(found.len(), 1, "exactly one active partial");
    found.pop().unwrap()
}

/// Lay `bytes` down as this owner's active partial, run recovery, and return
/// the promoted `sealed/` path. The `TempDir` is returned so the caller keeps
/// it alive.
fn recover_torn_fixture(bytes: &[u8]) -> (std::path::PathBuf, tempfile::TempDir) {
    use std::fs;
    let tmp = tempfile::tempdir().unwrap();
    let active = tmp.path().join(ACTIVE_DIR);
    fs::create_dir_all(&active).unwrap();
    fs::write(
        active.join(format!("{TORN_OWNER}-0198d3c4.arrow.partial")),
        bytes,
    )
    .unwrap();

    let recovered = recover_orphaned_partials(tmp.path(), TORN_OWNER).unwrap();
    assert_eq!(recovered, 1, "recovery promotes the torn partial as-is");
    let sealed = list_sealed(tmp.path()).unwrap();
    assert_eq!(sealed.len(), 1);
    (sealed[0].clone(), tmp)
}

/// The second append's Arrow IPC metadata length, read out of the fixture.
fn second_message_meta_len(both: &[u8], prefix_len: usize) -> usize {
    let second = &both[prefix_len..];
    assert_eq!(
        &second[0..4],
        &[0xFF, 0xFF, 0xFF, 0xFF],
        "an IPC message starts with the continuation marker"
    );
    u32::from_le_bytes(second[4..8].try_into().unwrap()) as usize
}

/// Rows readable from a promoted segment, by path and by bytes — the two
/// production readers (`read_segment` for the local drain and the query-side
/// WAL buffer, `read_segment_from_bytes` for the catalog-claim mirror path).
/// They must agree, so the fixtures characterize one behaviour, not two.
fn rows_via_both_readers(sealed: &std::path::Path, bytes: &[u8]) -> Result<usize, String> {
    let via_path = read_segment(sealed)
        .map(|b| b.iter().map(|b| b.num_rows()).sum::<usize>())
        .map_err(|e| format!("{e:#}"));
    let via_bytes = read_segment_from_bytes(bytes)
        .map(|b| b.iter().map(|b| b.num_rows()).sum::<usize>())
        .map_err(|e| format!("{e:#}"));
    assert_eq!(
        via_path.is_ok(),
        via_bytes.is_ok(),
        "read_segment and read_segment_from_bytes must agree: {via_path:?} vs {via_bytes:?}"
    );
    if let (Ok(a), Ok(b)) = (&via_path, &via_bytes) {
        assert_eq!(a, b, "both readers must return the same row count");
    }
    via_path
}

/// A crash that lands on (or just inside the leading continuation marker of)
/// the next message boundary is the benign case: the Arrow `StreamReader`
/// treats EOF on the first four bytes of a message as end-of-stream, so the
/// fsynced first append reads back whole.
#[test]
fn a_torn_append_at_a_message_boundary_keeps_the_fsynced_prefix() {
    let (both, prefix_len) = fsynced_partial_with_second_append();
    // 0..=3 bytes of the 4-byte continuation marker written before the crash.
    for extra in 0..=3 {
        let bytes = &both[..prefix_len + extra];
        let (sealed, _tmp) = recover_torn_fixture(bytes);
        assert_eq!(
            rows_via_both_readers(&sealed, bytes),
            Ok(3),
            "{extra} byte(s) into the continuation marker must still drain the \
             first fsynced append"
        );
        assert_eq!(
            segment_owner(&sealed),
            Some(torn_table_uuid()),
            "recovery must not disturb the frame header's table UUID"
        );
    }
}

/// The second append complete on disk but with no EOS marker: both appends
/// drain. This is the pre-existing recovery case
/// (`recover_orphaned_partials_promotes_to_sealed`) with a second fsynced
/// message behind the first.
#[test]
fn a_complete_second_message_without_eos_drains_both_appends() {
    let (both, _prefix_len) = fsynced_partial_with_second_append();
    let (sealed, _tmp) = recover_torn_fixture(&both);
    assert_eq!(rows_via_both_readers(&sealed, &both), Ok(8));
    assert_eq!(segment_owner(&sealed), Some(torn_table_uuid()));
}

/// A crash anywhere past the second message's continuation marker — in its
/// metadata length, metadata, or body — still drains the complete first
/// append, which `sync_active` had already fsynced and ingest had acknowledged
/// under the default `commit=wait_for` contract. Recovery leaves the original
/// bytes and table identity intact; PARTIAL-frame decoding alone drops the
/// incomplete final IPC message.
#[test]
fn a_torn_second_message_keeps_the_fsynced_prefix() {
    let (both, prefix_len) = fsynced_partial_with_second_append();
    let meta_len = second_message_meta_len(&both, prefix_len);
    let tears = [
        ("torn metadata length", prefix_len + 6),
        ("torn metadata", prefix_len + 8 + 4),
        ("metadata whole, body absent", prefix_len + 8 + meta_len),
        ("torn body", prefix_len + 8 + meta_len + 16),
    ];
    for (label, len) in tears {
        assert!(len < both.len(), "{label}: tear point must be a truncation");
        let bytes = &both[..len];
        let (sealed, _tmp) = recover_torn_fixture(bytes);

        assert_eq!(
            rows_via_both_readers(&sealed, bytes),
            Ok(3),
            "{label}: the complete fsynced prefix must drain"
        );

        // The reader returns exactly the intact prefix, without reconstructing
        // any of the second append's five rows.
        assert_eq!(
            read_segment_from_bytes(&both[..prefix_len])
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            3,
            "{label}: the fsynced prefix is intact on disk"
        );
        assert_eq!(
            segment_owner(&sealed),
            Some(torn_table_uuid()),
            "{label}: the promoted segment keeps its table UUID"
        );
    }
}

// ---- Phase 4.13h: compactor orphan quarantine ----------------------------

/// `recover_orphaned_processing` moves stranded files from
/// `processing/` to `orphans/`. The compactor calls this at
/// startup so a previous-pod-died-mid-cycle scenario doesn't
/// leave segments in `processing/` forever (round-12 bug).
#[test]
fn recover_orphaned_processing_quarantines_to_orphans() {
    use std::fs::{self, File};
    let tmp = tempfile::tempdir().unwrap();
    let processing = tmp.path().join(PROCESSING_DIR);
    fs::create_dir_all(&processing).unwrap();
    // Two stranded segments + one non-matching directory entry.
    File::create(processing.join("a.arrow")).unwrap();
    File::create(processing.join("b.arrow")).unwrap();
    fs::create_dir(processing.join("subdir-not-a-segment")).unwrap();

    let moved = recover_orphaned_processing(tmp.path()).unwrap();
    assert_eq!(moved, 2);

    // orphans/ now has both segments.
    let orphans_dir = tmp.path().join(ORPHANS_DIR);
    let orphans: Vec<_> = fs::read_dir(&orphans_dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    assert_eq!(orphans.len(), 2);
    let names: Vec<_> = orphans
        .iter()
        .filter_map(|p| p.file_name().and_then(|s| s.to_str()).map(str::to_string))
        .collect();
    assert!(names.contains(&"a.arrow".to_string()));
    assert!(names.contains(&"b.arrow".to_string()));

    // processing/ no longer has the segments (subdir untouched).
    let still: Vec<_> = fs::read_dir(&processing)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    assert_eq!(still.len(), 1, "non-file entries untouched");
}

/// If `processing/` is empty (steady-state operation), recovery
/// is a no-op and doesn't even create the `orphans/` directory.
#[test]
fn recover_orphaned_processing_noop_when_processing_empty() {
    let tmp = tempfile::tempdir().unwrap();
    // No processing/ dir at all yet.
    let moved = recover_orphaned_processing(tmp.path()).unwrap();
    assert_eq!(moved, 0);
    assert!(
        !tmp.path().join(ORPHANS_DIR).exists(),
        "no need to create orphans/ when there's nothing to quarantine"
    );
}

/// Re-running recovery against an already-quarantined orphan is
/// idempotent: the duplicate file in `processing/` is just
/// removed (rather than overwriting the prior quarantine copy).
#[test]
fn recover_orphaned_processing_idempotent_on_collision() {
    use std::fs::{self, File};
    let tmp = tempfile::tempdir().unwrap();
    let processing = tmp.path().join(PROCESSING_DIR);
    let orphans = tmp.path().join(ORPHANS_DIR);
    fs::create_dir_all(&processing).unwrap();
    fs::create_dir_all(&orphans).unwrap();
    // Same name already quarantined.
    File::create(orphans.join("a.arrow")).unwrap();
    File::create(processing.join("a.arrow")).unwrap();

    let moved = recover_orphaned_processing(tmp.path()).unwrap();
    assert_eq!(moved, 0, "no new quarantine when name collides");
    // processing/ was cleaned out anyway.
    assert!(!processing.join("a.arrow").exists());
    // orphans/ unchanged — the prior file wins (it was quarantined first).
    assert!(orphans.join("a.arrow").exists());
}
