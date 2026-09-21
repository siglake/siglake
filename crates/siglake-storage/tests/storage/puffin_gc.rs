use siglake_core::Event;
use siglake_storage::iceberg::{GcOptions, IcebergContext};

use crate::fixture_clock::{assert_one_partition, fixture_base};

/// One second apart off [`fixture_base`], so the appends a fixture then
/// re-clusters carry one `day(timestamp)` partition value whatever time of day
/// the suite runs (#5678).
fn event_at(offset_secs: i64, raw: &str) -> Event {
    Event {
        timestamp: fixture_base() + chrono::Duration::seconds(offset_secs),
        ..Event::now(raw)
    }
}

async fn rewritten_sidecars() -> (
    tempfile::TempDir,
    IcebergContext,
    iceberg::TableIdent,
    Vec<String>,
    String,
) {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_footer_max_bytes: Some(1),
            ..Default::default()
        });
    ice.append_events(&[event_at(0, "database timeout one")])
        .await
        .unwrap();
    ice.append_events(&[event_at(1, "database timeout two")])
        .await
        .unwrap();
    let ident = ice.events_table_ident().clone();
    let before_rewrite = ice.catalog().load_table(&ident).await.unwrap();
    let retired_sidecars: Vec<String> = before_rewrite
        .metadata()
        .statistics_iter()
        .map(|statistics| statistics.statistics_path.clone())
        .collect();
    assert_eq!(retired_sidecars.len(), 2);

    let files = ice.live_data_files(&ident).await.unwrap();
    assert_one_partition(&files, "rewritten_sidecars");
    ice.recluster_files(
        &ident,
        files,
        siglake_storage::iceberg::BLOOM_FILTER_COLUMNS,
    )
    .await
    .unwrap();
    ice.append_events(&[event_at(2, "database timeout live")])
        .await
        .unwrap();

    let after_append = ice.catalog().load_table(&ident).await.unwrap();
    let live_sidecar = after_append
        .metadata()
        .statistics_iter()
        .map(|statistics| statistics.statistics_path.clone())
        .find(|path| !retired_sidecars.contains(path))
        .expect("the post-rewrite append registers a live sidecar");
    (tmp, ice, ident, retired_sidecars, live_sidecar)
}

#[tokio::test]
async fn registered_puffin_sidecars_are_reachable_and_unregistered_ones_are_collected() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_footer_max_bytes: Some(1),
            ..Default::default()
        });
    ice.append_events(&[Event::now("database timeout retry")])
        .await
        .unwrap();

    let ident = ice.events_table_ident().clone();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let stats = table
        .metadata()
        .statistics_iter()
        .next()
        .cloned()
        .expect("append should register a Puffin statistics file");
    let reachable = ice.reachable_files(&ident).await.unwrap();
    assert!(
        reachable.contains(&stats.statistics_path),
        "registered statistics files must be part of the GC reachable set"
    );

    let metadata_dir = table
        .metadata_location()
        .unwrap()
        .rsplit_once('/')
        .unwrap()
        .0;
    let orphan_path = format!("{metadata_dir}/orphan-test.puffin");
    table
        .file_io()
        .new_output(&orphan_path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"orphan"))
        .await
        .unwrap();

    let dry_run = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: std::time::Duration::ZERO,
                apply: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        dry_run.orphans, 1,
        "only the unregistered Puffin should be orphaned"
    );
    assert!(
        table
            .file_io()
            .exists(&stats.statistics_path)
            .await
            .unwrap(),
        "registered Puffin should not be listed for deletion"
    );

    let applied = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: std::time::Duration::ZERO,
                apply: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(applied.deleted, 1);
    assert!(
        !table.file_io().exists(&orphan_path).await.unwrap(),
        "unregistered Puffin should be reclaimed by GC"
    );
    assert!(
        table
            .file_io()
            .exists(&stats.statistics_path)
            .await
            .unwrap(),
        "registered Puffin must remain after GC"
    );
}

#[tokio::test]
async fn expiry_retires_only_fully_obsolete_owned_statistics_entries() {
    let (_tmp, ice, ident, retired_sidecars, live_sidecar) = rewritten_sidecars().await;

    assert!(ice.expire_snapshots(&ident, 1).await.unwrap() > 0);
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let retained: Vec<&str> = table
        .metadata()
        .statistics_iter()
        .map(|statistics| statistics.statistics_path.as_str())
        .collect();
    assert_eq!(retained, vec![live_sidecar.as_str()]);
    for path in &retired_sidecars {
        assert!(
            table.file_io().exists(path).await.unwrap(),
            "metadata retirement leaves {path} for age-gated orphan GC"
        );
    }
    assert!(table.file_io().exists(&live_sidecar).await.unwrap());
}

#[tokio::test]
async fn gc_removes_obsolete_entries_then_reclaims_only_their_puffin_objects() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let (_tmp, ice, ident, retired_sidecars, live_sidecar) = rewritten_sidecars().await;
    // Exercise gc-orphans' own retirement sequence by applying only the fork's
    // metadata-only snapshot action first. Production's elected expiry pass
    // chains the same retirement action in its commit (covered above).
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let expire = tx
        .expire_snapshots()
        .retain_last(1)
        .expire_older_than_ms(i64::MAX)
        .retain_statistics_files();
    let tx = expire.apply(tx).unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();
    ice.invalidate_cached_table(&ident).await;

    let dry_run = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: std::time::Duration::ZERO,
                apply: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(dry_run.statistics_entries_eligible, 2);
    assert_eq!(dry_run.statistics_entries_removed, 0);
    assert!(dry_run.orphans >= retired_sidecars.len());
    let table = ice.catalog().load_table(&ident).await.unwrap();
    for path in &retired_sidecars {
        assert!(table.file_io().exists(path).await.unwrap());
    }

    let report = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: std::time::Duration::ZERO,
                apply: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(report.statistics_entries_eligible, 2);
    assert_eq!(report.statistics_entries_removed, 2);
    assert_eq!(report.statistics_entries_kept_live, 1);

    let table = ice.catalog().load_table(&ident).await.unwrap();
    for path in &retired_sidecars {
        assert!(
            !table.file_io().exists(path).await.unwrap(),
            "obsolete Puffin object survived GC: {path}"
        );
    }
    assert!(
        table.file_io().exists(&live_sidecar).await.unwrap(),
        "a sidecar with a live data-file reference must survive"
    );
}
