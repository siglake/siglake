use siglake_core::Event;
use siglake_storage::iceberg::{GcOptions, IcebergContext};

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
