use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, BooleanArray, Int64Array, StringArray};
use datafusion::prelude::SessionContext;

use siglake_compactor::Compactor;
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, MappingMode};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::index_manager::IndexTemplate;
use siglake_wal::{list_sealed, WalWriter};

fn typed_template(template_id: &str, pattern: &str) -> IndexTemplate {
    IndexTemplate {
        template_id: template_id.to_string(),
        index_id_patterns: vec![pattern.to_string()],
        priority: 0,
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                FieldMapping {
                    name: "event_time".into(),
                    field_type: FieldType::Datetime,
                    required: true,
                },
                FieldMapping {
                    name: "host".into(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".into()),
                    },
                    required: true,
                },
                FieldMapping {
                    name: "raw".into(),
                    field_type: FieldType::Text {
                        tokenizer: Some("default".into()),
                    },
                    required: true,
                },
                FieldMapping {
                    name: "service".into(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".into()),
                    },
                    required: false,
                },
                FieldMapping {
                    name: "status".into(),
                    field_type: FieldType::Long,
                    required: false,
                },
                FieldMapping {
                    name: "sampled".into(),
                    field_type: FieldType::Bool,
                    required: false,
                },
            ],
            timestamp_field: "event_time".into(),
            tag_fields: vec!["host".into(), "service".into()],
            default_search_fields: vec!["raw".into()],
        },
        retention: None,
    }
}

fn carrier_event(raw: &str, attrs: &str) -> Event {
    let mut event = Event::now(raw).with_attributes(Some(attrs.to_string()));
    event.host = "host-a".into();
    event.source = "src".into();
    event.sourcetype = "app".into();
    event.index = "main".into();
    event
}

async fn register_index_table(
    ice: &IcebergContext,
    index_id: &str,
    df_name: &str,
) -> SessionContext {
    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(index_id), df_name)
        .await
        .unwrap();
    ctx
}

#[tokio::test]
async fn compactor_commits_per_index_segments_to_typed_tables() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    let tenant_dir = wal_root.join("acme").join("app1");
    std::fs::create_dir_all(wal_root.join("acme").join("sealed")).unwrap();

    {
        let mut writer =
            WalWriter::with_thresholds(&tenant_dir, "ing", 2, Duration::from_secs(60)).unwrap();
        writer
            .append_events(&[
                carrier_event(
                    "row-1",
                    r#"{"service":"api","status":"200","sampled":"true","left":"keep"}"#,
                ),
                carrier_event(
                    "row-2",
                    r#"{"service":"worker","status":"oops","left":"stay"}"#,
                ),
            ])
            .unwrap()
            .expect("segment should seal at threshold");
    }

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let tenant_ice = ice.for_namespace("tenant_acme").await.unwrap();
    tenant_ice
        .put_index_template(&typed_template("app1-template", "app1"))
        .await
        .unwrap();

    let compactor = Compactor::new(&wal_root, ice.clone());
    assert_eq!(compactor.run_once().await.unwrap(), 1);
    assert_eq!(
        compactor.run_once().await.unwrap(),
        0,
        "second cycle should be empty"
    );

    let config = tenant_ice.get_index("app1").await.unwrap().unwrap();
    assert_eq!(config.index_id, "app1");

    let ctx = register_index_table(&tenant_ice, "app1", "app1").await;
    let batches = ctx
        .sql("SELECT event_time, host, raw, service, status, sampled, attributes FROM app1 ORDER BY raw")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    let host = batch
        .column_by_name("host")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(host.value(0), "host-a");

    let service = batch
        .column_by_name("service")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(service.value(0), "api");
    assert_eq!(service.value(1), "worker");

    let status = batch
        .column_by_name("status")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(status.value(0), 200);
    assert!(status.is_null(1));

    let sampled = batch
        .column_by_name("sampled")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(sampled.value(0));
    assert!(sampled.is_null(1));

    let residual = batch
        .column_by_name("attributes")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(residual.value(0), r#"{"left":"keep"}"#);
    assert_eq!(residual.value(1), r#"{"left":"stay","status":"oops"}"#);
}

#[tokio::test]
async fn unresolved_index_segments_retry_after_template_is_added() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    let tenant_dir = wal_root.join("acme").join("delayed");
    std::fs::create_dir_all(wal_root.join("acme").join("sealed")).unwrap();

    {
        let mut writer =
            WalWriter::with_thresholds(&tenant_dir, "ing", 1, Duration::from_secs(60)).unwrap();
        writer
            .append_events(&[carrier_event("row-1", r#"{"service":"api"}"#)])
            .unwrap()
            .expect("segment should seal at threshold");
    }

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let tenant_ice = ice.for_namespace("tenant_acme").await.unwrap();
    let compactor = Compactor::new(&wal_root, ice.clone());

    assert_eq!(compactor.run_once().await.unwrap(), 0);
    assert_eq!(
        list_sealed(&tenant_dir).unwrap().len(),
        1,
        "sealed segment must stay pending"
    );
    assert_eq!(tenant_ice.get_index("delayed").await.unwrap(), None);

    tenant_ice
        .put_index_template(&typed_template("delayed-template", "delayed"))
        .await
        .unwrap();

    assert_eq!(compactor.run_once().await.unwrap(), 1);
    assert!(list_sealed(&tenant_dir).unwrap().is_empty());

    let ctx = register_index_table(&tenant_ice, "delayed", "delayed").await;
    let batches = ctx
        .sql("SELECT count(*) AS n FROM delayed")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);
}

/// #2836, the filesystem half of
/// `catalog_claim::a_recreation_before_the_mirrored_append_loads_its_table_commits_nothing`.
///
/// The per-index WAL directory carries the identity the drain verified
/// (#2661), and each segment's frame header carries its own (#2693). Both are
/// facts about the moment they were read. The append then resolved the index
/// NAME again, so a `DELETE` + `POST` of the same id in between committed the
/// dropped incarnation's rows into the replacement — and the transaction's uuid
/// fence cannot object, because the replacement was loaded before
/// `Transaction::new` and is that transaction's own base.
///
/// The verified identity now travels with the commit target. On a mismatch the
/// batch is refused before a file is written: the replacement receives no rows,
/// no snapshot and no consumed proof, the segments go back to `sealed/` rather
/// than `committed/`, and the next cycle's ownership check quarantines them
/// under `stale/<dropped-uuid>/` the way it always has.
#[tokio::test]
async fn a_recreation_before_the_append_loads_its_table_commits_nothing() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    let index_dir = wal_root.join("default").join("fs_race");
    // `list_tenant_dirs` recognizes a tenant by its own `sealed/`.
    std::fs::create_dir_all(wal_root.join("default").join("sealed")).unwrap();

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let config = siglake_core::index_config::IndexConfig {
        index_id: "fs_race".to_string(),
        doc_mapping: siglake_core::index_config::IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    let verified = ice.index_table_uuid("fs_race").await.unwrap().unwrap();

    let seal_owned = |ingester: &str, owner: &str, raw: &str| {
        let mut writer =
            WalWriter::with_thresholds(&index_dir, ingester, 1, Duration::from_secs(60)).unwrap();
        writer
            .bind_table_uuid(Some(uuid::Uuid::parse_str(owner).unwrap()))
            .unwrap();
        writer
            .append_events(&[Event::now(raw)])
            .unwrap()
            .expect("seals at threshold")
            .path
    };

    // The control: the name still resolves to the table the check verified, so
    // the cycle commits. This is the behaviour the fence must not cost, and it
    // is what stamps the directory's owner marker.
    let control = seal_owned("ing-control", &verified, "control row");
    assert_eq!(
        Compactor::new(&wal_root, ice.clone())
            .run_once()
            .await
            .unwrap(),
        1
    );
    assert!(index_dir
        .join("committed")
        .join(control.file_name().unwrap())
        .exists());
    assert_eq!(
        siglake_wal::read_wal_owner(&index_dir).as_deref(),
        Some(verified.as_str())
    );

    // Same id, different table.
    assert!(ice.delete_index("fs_race").await.unwrap());
    ice.create_index(&config).await.unwrap();
    let ice_new = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let live = ice_new.index_table_uuid("fs_race").await.unwrap().unwrap();
    assert_ne!(live, verified, "recreation is a new table");

    // A writer that has not re-resolved the index acknowledges more rows for
    // the dropped incarnation into the same directory.
    let stale = seal_owned("ing-stale", &verified, "dropped incarnation row");

    // The window: the cycle's directory check ran while the verified table was
    // still live, and the recreation lands before the append resolves the name.
    // Everything after the verdict is the shipped path — the per-segment gate
    // admits the segment, it is claimed into `processing/`, read, and the
    // append is the first step that can see the recreation.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let started = Instant::now();
    let err = {
        let _guard = metrics::set_default_local_recorder(&recorder);
        Compactor::new(&wal_root, ice_new.clone())
            .with_verified_owner_for_test(&verified)
            .run_once()
            .await
            .expect_err("the cycle must fail rather than commit into the replacement")
    };
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the terminal refusal must return well inside the default 30 second cycle budget"
    );
    let commit_errors: u64 = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "siglake_compactor_cycles_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == "commit_error")
        })
        .map(|(key, _, _, value)| match value {
            DebugValue::Counter(n) => n,
            other => panic!("{} must be a counter, got {other:?}", key.key().name()),
        })
        .sum();
    assert_eq!(
        commit_errors, 1,
        "the incarnation refusal must be attempted exactly once"
    );
    let text = format!("{err:#}");
    assert!(
        text.contains(&verified) && text.contains("dropped and recreated"),
        "the error must name the incarnation boundary: {text}"
    );

    // The replacement received nothing at all.
    let ctx = register_index_table(&ice_new, "fs_race", "fs_race").await;
    let batches = ctx
        .sql("SELECT count(*) AS n FROM fs_race")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0,
        "the replacement must expose no rows"
    );
    let table = ice_new
        .catalog()
        .load_table(&ice_new.index_table_ident("fs_race"))
        .await
        .unwrap();
    assert_eq!(
        table.metadata().snapshots().count(),
        0,
        "the replacement must have no snapshot of its own"
    );
    assert!(
        table
            .metadata()
            .properties()
            .get(siglake_storage::consumed_proof::CONSUMED_PROOF_PROP)
            .is_none(),
        "and no consumed proof: a proof here retires the ingester's copy of rows \
         this table does not have"
    );

    // The segment is not marked committed: released back to `sealed/`, still
    // there for the check that decides its fate, and nothing is quarantined yet.
    assert!(stale.exists(), "the refused segment is back in sealed/");
    assert!(!index_dir
        .join("committed")
        .join(stale.file_name().unwrap())
        .exists());
    assert!(
        !index_dir.join("stale").exists(),
        "the append refusal is not the quarantine decision"
    );
    assert!(list_sealed(&index_dir).unwrap().len() == 1);

    // And the ordinary path finishes the job: the next cycle's real check finds
    // the directory naming a dropped table and quarantines what it holds.
    assert_eq!(
        Compactor::new(&wal_root, ice_new.clone())
            .run_once()
            .await
            .unwrap(),
        0
    );
    assert!(
        index_dir
            .join("stale")
            .join(&verified)
            .join(stale.file_name().unwrap())
            .exists(),
        "the dropped incarnation's segment is held under stale/<dropped-uuid>/"
    );
    assert_eq!(
        siglake_wal::read_wal_owner(&index_dir).as_deref(),
        Some(live.as_str()),
        "and the directory is re-stamped for the replacement"
    );
}

/// Terminal discovery stops refills, but batches admitted before the first
/// refusal still reach completion and release their own claims.
#[tokio::test]
async fn an_incarnation_refusal_drains_already_admitted_sibling_batches() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    let index_dir = wal_root.join("default").join("fs_siblings");
    std::fs::create_dir_all(wal_root.join("default").join("sealed")).unwrap();

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let config = siglake_core::index_config::IndexConfig {
        index_id: "fs_siblings".to_string(),
        doc_mapping: siglake_core::index_config::IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    let verified = ice.index_table_uuid("fs_siblings").await.unwrap().unwrap();
    for ingester in ["ing-a", "ing-b"] {
        let mut writer =
            WalWriter::with_thresholds(&index_dir, ingester, 1, Duration::from_secs(60)).unwrap();
        writer
            .bind_table_uuid(Some(uuid::Uuid::parse_str(&verified).unwrap()))
            .unwrap();
        writer
            .append_events(&[Event::now(ingester)])
            .unwrap()
            .expect("one row seals each sibling");
    }
    assert!(ice.delete_index("fs_siblings").await.unwrap());
    ice.create_index(&config).await.unwrap();
    let ice_new = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    {
        let _guard = metrics::set_default_local_recorder(&recorder);
        Compactor::new(&wal_root, ice_new)
            .with_verified_owner_for_test(&verified)
            .with_fs_batch_limits(1, 0)
            .with_drain_concurrency(2)
            .run_once()
            .await
            .expect_err("both admitted batches cross the same incarnation boundary");
    }
    let commit_errors: u64 = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "siglake_compactor_cycles_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == "commit_error")
        })
        .map(|(key, _, _, value)| match value {
            DebugValue::Counter(n) => n,
            other => panic!("{} must be a counter, got {other:?}", key.key().name()),
        })
        .sum();
    assert_eq!(commit_errors, 2, "both admitted sibling batches completed");
    assert_eq!(
        list_sealed(&index_dir).unwrap().len(),
        2,
        "each sibling released its own segment"
    );
}
