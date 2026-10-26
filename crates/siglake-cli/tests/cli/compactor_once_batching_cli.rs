//! The shipped `compactor --once` commit-batch gate and its immediate-drain
//! opt-out, exercised against the filesystem WAL path an operator uses after
//! stopping an ingester.

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_wal::{list_sealed, WalWriter};

const ROWS: usize = 9_867;
const TARGET_BYTES: u64 = 32 * 1024 * 1024;
const MAX_AGE: Duration = Duration::from_secs(600);

fn run_once(data_dir: &Path, target_mb: &str, max_age_secs: &str) -> String {
    let mut command = Command::new(env!("CARGO_BIN_EXE_siglake"));
    command
        .arg("--data-dir")
        .arg(data_dir)
        .args(["compactor", "--once"]);
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("SIGLAKE_") {
            command.env_remove(key);
        }
    }
    let output = command
        .env("SIGLAKE_COMMIT_BATCH_TARGET_MB", target_mb)
        .env("SIGLAKE_COMMIT_BATCH_MAX_AGE_SECS", max_age_secs)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn siglake compactor --once");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(
        output.status.success(),
        "compactor --once exited {:?}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        output.status.code()
    );
    stdout
}

async fn count_events(warehouse: &Path) -> i64 {
    let ice = IcebergContext::open(warehouse).await.unwrap();
    let context = SessionContext::new();
    ice.register_with_datafusion(&context).await.unwrap();
    let batches = context
        .sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

fn pending_stats(wal: &Path) -> (Vec<std::path::PathBuf>, u64, Duration) {
    let sealed = list_sealed(wal).unwrap();
    let mut bytes = 0u64;
    let mut oldest_age = Duration::ZERO;
    for path in &sealed {
        let metadata = std::fs::metadata(path).unwrap();
        bytes += metadata.len();
        oldest_age = oldest_age.max(metadata.modified().unwrap().elapsed().unwrap());
    }
    (sealed, bytes, oldest_age)
}

#[tokio::test]
async fn once_defers_a_young_small_queue_and_zero_target_drains_it() {
    let tmp = tempfile::tempdir().unwrap();
    let wal = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    let events: Vec<_> = (0..ROWS)
        .map(|row| Event::now(format!("shutdown-batch-row-{row}")))
        .collect();
    let sealed_path = {
        let mut writer =
            WalWriter::with_thresholds(&wal, "stopped-ingester", ROWS + 1, Duration::from_secs(60))
                .unwrap();
        writer.append_events(&events).unwrap();
        writer.seal().unwrap().expect("shutdown seals the WAL").path
    };

    let (before_files, before_bytes, before_age) = pending_stats(&wal);
    assert_eq!(before_files, vec![sealed_path.clone()]);
    assert!(before_bytes > 0, "the sealed queue has physical bytes");
    assert!(
        before_bytes < TARGET_BYTES,
        "fixture must stay below the 32 MiB gate: {before_bytes} bytes"
    );
    assert!(
        before_age < MAX_AGE,
        "fixture must be younger than the configured hold: {before_age:?}"
    );

    let deferred = run_once(tmp.path(), "32", "600");
    assert_eq!(deferred, "committed 0 segment(s)\n");
    let (after_files, after_bytes, after_age) = pending_stats(&wal);
    assert_eq!(after_files, vec![sealed_path]);
    assert_eq!(
        after_bytes, before_bytes,
        "deferral preserves pending bytes"
    );
    assert!(
        after_age < MAX_AGE,
        "the pass deferred before the age trigger: {after_age:?}"
    );
    assert_eq!(count_events(&warehouse).await, 0);

    let drained = run_once(tmp.path(), "0", "600");
    assert_eq!(drained, "committed 1 segment(s)\n");
    assert!(list_sealed(&wal).unwrap().is_empty());
    assert_eq!(count_events(&warehouse).await, ROWS as i64);
}
