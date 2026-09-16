//! `siglake wal-requeue` run as a real process: the operator's deliberate way
//! out of the drain's `poison/` set-aside (#3143).
//!
//! The set-aside itself is terminal by design — nothing in the drain requeues
//! it — so this command is the whole documented procedure, and what it prints
//! is what an operator reads before deciding. The WAL-level moves are pinned
//! by `siglake-wal`'s lifecycle tests; this covers the walk over the
//! tenant/index layout, the dry run, the name filter and the report.

use std::path::{Path, PathBuf};
use std::process::Command;

use siglake_core::Event;
use siglake_wal::{list_poisoned, list_sealed, WalWriter, POISON_DIR};

/// Seal one segment under `dir` and set it aside as if the drain had spent its
/// attempts on it.
fn set_aside(dir: &Path, reason: &str) -> PathBuf {
    let mut w =
        WalWriter::with_thresholds(dir, "ing-1", 1, std::time::Duration::from_secs(60)).unwrap();
    w.append_events(&[Event::now("held".to_string())])
        .unwrap()
        .expect("one row seals the segment");
    let sealed = list_sealed(dir).unwrap();
    siglake_wal::quarantine_poison_segment(&sealed[0], reason, 3).unwrap()
}

fn run(args: &[&str]) -> (String, bool) {
    let out = Command::new(env!("CARGO_BIN_EXE_siglake"))
        .args(args)
        .output()
        .expect("spawning the siglake binary");
    (
        String::from_utf8_lossy(&out.stdout).to_string(),
        out.status.success(),
    )
}

#[test]
fn requeue_walks_the_layout_and_only_moves_what_it_is_asked_for() {
    let tmp = tempfile::tempdir().unwrap();
    let wal = tmp.path().join("wal");
    let tenant = wal.join("acme");
    let index = tenant.join("http_logs");
    std::fs::create_dir_all(&index).unwrap();

    // Nothing set aside anywhere: the report says so and succeeds, so this is
    // safe to run from a runbook before anything is known.
    std::fs::create_dir_all(wal.join("sealed")).unwrap();
    let (out, ok) = run(&["wal-requeue", "--wal", wal.to_str().unwrap()]);
    assert!(ok, "{out}");
    assert!(out.contains("nothing is set aside"), "{out}");

    let root_held = set_aside(&wal, "CRC mismatch");
    let tenant_held = set_aside(&tenant, "truncated IPC message");
    let index_held = set_aside(&index, "unknown frame version");

    // A dry run reads as a report and moves nothing.
    let (out, ok) = run(&["wal-requeue", "--wal", wal.to_str().unwrap(), "--dry-run"]);
    assert!(ok, "{out}");
    for (held, reason) in [
        (&root_held, "CRC mismatch"),
        (&tenant_held, "truncated IPC message"),
        (&index_held, "unknown frame version"),
    ] {
        assert!(
            out.contains(&format!("WOULD REQUEUE {}", held.display())),
            "{} missing from {out}",
            held.display()
        );
        assert!(out.contains(reason), "the recorded reason is shown: {out}");
    }
    assert!(out.contains("3 segment(s) would be requeued"), "{out}");
    assert!(out.contains("after 3 attempt(s)"), "{out}");
    for dir in [&wal, &tenant, &index] {
        assert_eq!(list_poisoned(dir).unwrap().len(), 1, "nothing moved");
    }

    // One name: exactly one segment moves, the other two stay held.
    let name = index_held.file_name().unwrap().to_str().unwrap();
    let (out, ok) = run(&[
        "wal-requeue",
        "--wal",
        wal.to_str().unwrap(),
        "--segment",
        name,
    ]);
    assert!(ok, "{out}");
    assert!(out.contains("requeued 1 of 1 segment(s)"), "{out}");
    assert!(list_poisoned(&index).unwrap().is_empty());
    assert_eq!(list_sealed(&index).unwrap().len(), 1, "back in sealed/");
    assert_eq!(list_poisoned(&wal).unwrap().len(), 1);
    assert_eq!(list_poisoned(&tenant).unwrap().len(), 1);

    // A name nobody holds is a report, not an error.
    let (out, ok) = run(&[
        "wal-requeue",
        "--wal",
        wal.to_str().unwrap(),
        "--segment",
        "01900000-0000-0000-0000-000000000000.arrow",
    ]);
    assert!(ok, "{out}");
    assert!(out.contains("no segment named"), "{out}");

    // The rest.
    let (out, ok) = run(&["wal-requeue", "--wal", wal.to_str().unwrap()]);
    assert!(ok, "{out}");
    assert!(out.contains("requeued 2 of 2 segment(s)"), "{out}");
    for dir in [&wal, &tenant, &index] {
        assert!(list_poisoned(dir).unwrap().is_empty());
        assert_eq!(list_sealed(dir).unwrap().len(), 1);
    }
}

/// Pointing the command at the quarantine directory instead of the WAL root
/// would silently visit nothing; it is refused with the path to use.
#[test]
fn requeue_refuses_a_poison_directory_as_the_root() {
    let tmp = tempfile::tempdir().unwrap();
    let wal = tmp.path().join("wal");
    std::fs::create_dir_all(&wal).unwrap();
    set_aside(&wal, "CRC mismatch");

    let out = Command::new(env!("CARGO_BIN_EXE_siglake"))
        .args([
            "wal-requeue",
            "--wal",
            wal.join(POISON_DIR).to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("must be the WAL ROOT"), "{err}");
    assert!(err.contains(wal.to_str().unwrap()), "{err}");
    assert_eq!(list_poisoned(&wal).unwrap().len(), 1, "nothing moved");
}
