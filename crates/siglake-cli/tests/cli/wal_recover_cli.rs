//! `siglake wal-recover` run as a real process against a `file://` mirror:
//! the documented disaster-recovery path, and the stated reason WAL mirroring
//! is on by default.
//!
//! WHAT THIS PINS (#4912). The command built its object store from the whole
//! `--from` URL — `build_opendal_operator` roots the store at the URL's path —
//! and then passed that same path again as the listing prefix, so the lister
//! walked `<path>/<path>/` and found nothing. Every `--from` carrying a path
//! printed `pulled 0 segments` and exited 0: a restore reported as clean that
//! had recovered nothing. The library's own tests pass an UNROOTED operator
//! plus a separate prefix, which is the working combination, so nothing
//! covered the CLI's pairing. This runs the binary the runbook runs.

use std::path::{Path, PathBuf};
use std::process::Command;

use siglake_core::Event;
use siglake_wal::{list_sealed, WalWriter, SEALED_DIR};

/// Seal one real segment under `dir` and return its path. The bytes matter:
/// what recovery writes has to be a segment the ordinary drain will read.
pub(crate) fn seal_one(dir: &Path, body: &str) -> PathBuf {
    let mut w =
        WalWriter::with_thresholds(dir, "ing-1", 1, std::time::Duration::from_secs(60)).unwrap();
    w.append_events(&[Event::now(body.to_string())])
        .unwrap()
        .expect("one row seals the segment");
    list_sealed(dir).unwrap().pop().expect("a sealed segment")
}

/// Copy `src` to `<mirror>/<key>`, creating the intermediate directories —
/// the mirror layout as the uploader writes it.
pub(crate) fn place(mirror: &Path, key: &str, src: &Path) -> String {
    let dest = mirror.join(key);
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    std::fs::copy(src, &dest).unwrap();
    key.to_string()
}

pub(crate) fn recover(from: &str, to: &Path) -> (String, String, bool) {
    let out = Command::new(env!("CARGO_BIN_EXE_siglake"))
        .args(["wal-recover", "--from", from, "--to", to.to_str().unwrap()])
        // `--from` also reads this env var; the caller's environment must not
        // be able to supply the source of a recovery test.
        .env_remove("SIGLAKE_WAL_MIRROR_URL")
        .output()
        .expect("spawning the siglake binary");
    (
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
        out.status.success(),
    )
}

/// Build a mirror holding three segments — a tenant's events segment, an
/// index segment, and an active-mirror prefix of a third — and return
/// `(mirror_dir, sealed_segment_names)`.
fn mirror_with_three_segments(tmp: &Path) -> (PathBuf, [String; 3]) {
    // A path deliberately several components deep: the warehouse prefix shape
    // an operator actually passes (`s3://bucket/warehouse/wal-mirror`).
    let mirror = tmp.join("store").join("warehouse").join("wal-mirror");
    let src = tmp.join("src");
    let events = seal_one(&src.join("acme"), "acme-events");
    let index = seal_one(&src.join("acme").join("orders"), "acme-orders");
    let active = seal_one(&src.join("widgets"), "widgets-active");

    let names = [
        events.file_name().unwrap().to_str().unwrap().to_string(),
        index.file_name().unwrap().to_str().unwrap().to_string(),
        active.file_name().unwrap().to_str().unwrap().to_string(),
    ];
    place(&mirror, &format!("acme/{}", names[0]), &events);
    place(&mirror, &format!("acme/orders/{}", names[1]), &index);
    // The active mirror uploads a flushed but unsealed prefix under
    // `_active/`, spelled `.arrow.partial`.
    place(
        &mirror,
        &format!("_active/widgets/{}.partial", names[2]),
        &active,
    );
    (mirror, names)
}

fn assert_restored(wal: &Path, names: &[String; 3]) {
    let acme = wal.join("acme");
    let orders = acme.join("orders");
    let widgets = wal.join("widgets");
    assert!(
        acme.join(SEALED_DIR).join(&names[0]).exists(),
        "acme's events segment must land in acme's WAL"
    );
    assert!(
        orders.join(SEALED_DIR).join(&names[1]).exists(),
        "an index segment must land in that index's WAL"
    );
    assert!(
        widgets.join(SEALED_DIR).join(&names[2]).exists(),
        "an active-mirror object must recover as a sealed segment"
    );
    // Nothing flattened into a tenant-less directory, and every restored
    // segment is visible to the ordinary FS drain.
    assert!(
        !wal.join(SEALED_DIR).exists(),
        "no segment may be written to a tenant-less sealed/ directory"
    );
    for dir in [&acme, &orders, &widgets] {
        assert_eq!(
            list_sealed(dir).unwrap().len(),
            1,
            "the drain sees nothing in {}",
            dir.display()
        );
    }
}

/// The acceptance: a `--from` URL with a path restores the segments under it,
/// with and without a trailing slash. Against the pre-#4912 binary both arms
/// print `pulled 0 segments` and exit 0.
#[test]
fn recover_restores_the_layout_from_a_url_with_a_path() {
    let tmp = tempfile::tempdir().unwrap();
    let (mirror, names) = mirror_with_three_segments(tmp.path());

    for (arm, from) in [
        ("no trailing slash", format!("file://{}", mirror.display())),
        ("trailing slash", format!("file://{}/", mirror.display())),
    ] {
        let wal = tmp.path().join(format!("wal-{}", arm.replace(' ', "-")));
        let (stdout, stderr, ok) = recover(&from, &wal);
        assert!(ok, "{arm}: {stdout}{stderr}");
        assert!(
            stdout.contains("pulled 3 segments"),
            "{arm}: {stdout}{stderr}"
        );
        assert_restored(&wal, &names);

        // Rerunning the runbook is safe and says so: everything is already
        // present, nothing is re-pulled, and nothing is disturbed. The count
        // of what is already there is what separates this line from a restore
        // that understood nothing (#4928).
        let (stdout, stderr, ok) = recover(&from, &wal);
        assert!(ok, "{arm} rerun: {stdout}{stderr}");
        assert!(
            stdout.contains("pulled 0 segments"),
            "{arm} rerun: {stdout}{stderr}"
        );
        assert!(
            stdout.contains("(3 already present)"),
            "{arm} rerun: {stdout}{stderr}"
        );
        assert_restored(&wal, &names);
    }
}

/// A sealed copy wins over the active prefix of the SAME segment: the active
/// object is a flushed prefix, so taking both would duplicate its rows. This
/// is the one place the CLI's own key handling could double-count.
#[test]
fn recover_prefers_a_sealed_copy_over_its_active_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let mirror = tmp.path().join("warehouse").join("wal-mirror");
    let sealed = seal_one(&tmp.path().join("src").join("acme"), "complete");
    let name = sealed.file_name().unwrap().to_str().unwrap().to_string();
    place(&mirror, &format!("acme/{name}"), &sealed);
    // The same segment, still under `_active/`, with a shorter body.
    let short = tmp.path().join("short.arrow");
    std::fs::write(&short, b"PREFIX").unwrap();
    place(&mirror, &format!("_active/acme/{name}.partial"), &short);

    let wal = tmp.path().join("wal");
    let (stdout, stderr, ok) = recover(&format!("file://{}", mirror.display()), &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(
        stdout.contains("pulled 1 segments"),
        "one segment, two objects: {stdout}{stderr}"
    );
    assert_eq!(
        std::fs::read(wal.join("acme").join(SEALED_DIR).join(&name)).unwrap(),
        std::fs::read(&sealed).unwrap(),
        "the sealed copy must win over its active prefix"
    );
}

/// #4928: `--from` one component too high understands not one key, and the
/// report has to say so. Before this, the run printed `pulled 0 segments` and
/// exited 0 — the same line and the same status as a re-run with nothing left
/// to do — so an operator read a restore that recovered nothing as a restore
/// that had nothing to recover.
#[test]
fn recovering_from_an_ancestor_of_the_mirror_root_fails_and_counts_the_skips() {
    let tmp = tempfile::tempdir().unwrap();
    let (mirror, _names) = mirror_with_three_segments(tmp.path());
    let wal = tmp.path().join("wal");

    // `…/store`, two components above `…/store/warehouse/wal-mirror`: every
    // key is deeper than the `<tenant>[/<index>]/<segment>` layout allows.
    let ancestor = tmp.path().join("store");
    let (stdout, stderr, ok) = recover(&format!("file://{}", ancestor.display()), &wal);
    assert!(!ok, "a restore that understood nothing must not exit 0");
    assert!(
        stdout.contains("pulled 0 segments") && stdout.contains("3 keys skipped"),
        "the skipped count belongs on stdout next to the pulled count: {stdout}{stderr}"
    );
    assert!(
        stderr.contains("--from must name the MIRROR ROOT"),
        "the diagnostic names the thing to fix: {stderr}"
    );
    assert!(
        stderr.contains("warehouse/wal-mirror"),
        "and shows a refused key: {stderr}"
    );
    assert_eq!(
        std::fs::read_dir(&wal).unwrap().count(),
        0,
        "nothing was restored under {}",
        wal.display()
    );

    // The control for the exit status: the same command pointed at the mirror
    // root restores, and its idempotent re-run — also `pulled 0 segments` —
    // succeeds.
    let from = format!("file://{}", mirror.display());
    let (stdout, stderr, ok) = recover(&from, &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("pulled 3 segments"), "{stdout}{stderr}");
    let (stdout, stderr, ok) = recover(&from, &wal);
    assert!(ok, "an idempotent re-run succeeds: {stdout}{stderr}");
    assert!(stdout.contains("pulled 0 segments"), "{stdout}{stderr}");
}

/// An empty mirror is a clean answer, not a failure: nothing was refused, so
/// there is nothing to warn about and nothing for the operator to fix.
#[test]
fn recovering_from_an_empty_mirror_succeeds_quietly() {
    let tmp = tempfile::tempdir().unwrap();
    let mirror = tmp.path().join("warehouse").join("wal-mirror");
    std::fs::create_dir_all(&mirror).unwrap();
    let wal = tmp.path().join("wal");

    let (stdout, stderr, ok) = recover(&format!("file://{}", mirror.display()), &wal);
    assert!(ok, "{stdout}{stderr}");
    assert!(stdout.contains("pulled 0 segments"), "{stdout}{stderr}");
    assert!(
        !stdout.contains("skipped"),
        "nothing was refused: {stdout}{stderr}"
    );
}

/// A mirror holding recognised segments AND foreign keys restores the
/// segments and succeeds — the operator is not blocked by a stray object —
/// but the skip count is on stdout both times, and the re-run separates the
/// three segments it already has from the two keys it still does not
/// understand.
#[test]
fn a_mirror_with_unknown_keys_alongside_segments_restores_and_reports_both() {
    let tmp = tempfile::tempdir().unwrap();
    let (mirror, names) = mirror_with_three_segments(tmp.path());
    // Neither of these is a segment: one has no `.arrow` suffix at all, the
    // other is deeper than tenant/index.
    std::fs::write(mirror.join("README.md"), b"operator notes").unwrap();
    place(
        &mirror,
        &format!("acme/orders/nested/{}", names[0]),
        &mirror.join("acme").join(&names[0]),
    );

    let from = format!("file://{}", mirror.display());
    let wal = tmp.path().join("wal");
    let (stdout, stderr, ok) = recover(&from, &wal);
    assert!(ok, "a mixed mirror still restores: {stdout}{stderr}");
    assert!(
        stdout.contains("pulled 3 segments") && stdout.contains("2 keys skipped"),
        "{stdout}{stderr}"
    );
    assert_restored(&wal, &names);

    let (stdout, stderr, ok) = recover(&from, &wal);
    assert!(ok, "rerun: {stdout}{stderr}");
    assert!(
        stdout.contains("pulled 0 segments")
            && stdout.contains("3 already present")
            && stdout.contains("2 keys skipped"),
        "a rerun with unrelated keys is still a success, and says why it pulled \
         nothing: {stdout}{stderr}"
    );
    assert_restored(&wal, &names);
}

/// `--to` is the WAL ROOT. Pointing it at a `sealed/` directory would build
/// `<wal>/sealed/<tenant>/sealed/...`, which nothing drains, so it is refused
/// with the path to use instead — before anything is downloaded.
#[test]
fn recover_refuses_a_sealed_directory_as_the_target() {
    let tmp = tempfile::tempdir().unwrap();
    let (mirror, _names) = mirror_with_three_segments(tmp.path());
    let wal = tmp.path().join("wal");

    let (stdout, stderr, ok) = recover(
        &format!("file://{}", mirror.display()),
        &wal.join(SEALED_DIR),
    );
    assert!(!ok, "{stdout}{stderr}");
    assert!(stderr.contains("must be the WAL ROOT"), "{stderr}");
    assert!(stderr.contains(wal.to_str().unwrap()), "{stderr}");
    assert!(!wal.exists(), "nothing was written: {stdout}{stderr}");
}
