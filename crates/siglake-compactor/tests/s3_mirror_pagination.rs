//! Native S3 mirror-pagination regression test against the compose object store.
//!
//! This stays ignored because it requires the compose stack's S3 service:
//!
//!     scripts/up.sh
//!     cargo test -p siglake-compactor --test s3_mirror_pagination -- --ignored --nocapture
//!     scripts/down.sh
//!
//! `SIGLAKE_TEST_S3_ENDPOINT` overrides the default `http://localhost:9000`, and
//! `SIGLAKE_TEST_S3_ACCESS_KEY`/`SIGLAKE_TEST_S3_SECRET_KEY` override the
//! `minioadmin` defaults — the three together point the test at the Garage arm
//! of the 0.2.0 comparison (`SIGLAKE_OBJECT_STORE=garage`, task #2958), where a
//! `ListObjectsV2` that ignores `start-after` is the failure this test sees.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::{stream, StreamExt as _};
use opendal::services::S3;
use opendal::Operator;
use siglake_compactor::{CatalogClaimConfig, Compactor};
use siglake_storage::catalog_claim::SqlSegmentClaim;
use siglake_storage::iceberg::IcebergContext;

const BUCKET: &str = "siglake-warehouse";
const PAGE_OBJECTS: u64 = 1_024;
const INITIAL_OBJECTS: u64 = PAGE_OBJECTS * 2 + 1;
const MIRROR_ELECTION_LEASE: &str = "__maintenance__mirror_sync";

fn minio_operator() -> Operator {
    let endpoint = std::env::var("SIGLAKE_TEST_S3_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());
    let access_key =
        std::env::var("SIGLAKE_TEST_S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key =
        std::env::var("SIGLAKE_TEST_S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let builder = S3::default()
        .bucket(BUCKET)
        .region("us-east-1")
        .endpoint(&endpoint)
        .access_key_id(&access_key)
        .secret_access_key(&secret_key)
        .disable_config_load();
    Operator::new(builder).unwrap().finish()
}

fn unique_root() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("mirror-pagination-test-{}-{nanos}", std::process::id())
}

fn cursor_id(store: &Operator, prefix: &str) -> String {
    let info = store.info();
    format!(
        "{}:{}:{}:{}",
        info.scheme(),
        info.name(),
        info.root(),
        prefix.trim_matches('/')
    )
}

async fn write_objects(store: &Operator, keys: Vec<String>) {
    let writes = stream::iter(keys.into_iter().map(|key| {
        let store = store.clone();
        async move { store.write(&key, Bytes::from_static(b"x")).await }
    }))
    .buffer_unordered(64)
    .collect::<Vec<_>>()
    .await;
    for result in writes {
        result.unwrap();
    }
}

async fn run_sync_pass(
    pass: usize,
    owner: &SqlSegmentClaim,
    store: &Operator,
    prefix: &str,
    ice: &Arc<IcebergContext>,
    tmp: &std::path::Path,
) {
    let compactor = Compactor::new(tmp.join(format!("wal-{pass}")), ice.clone())
        .with_catalog_claim(CatalogClaimConfig {
            claim: owner.clone(),
            store: store.clone(),
            prefix: prefix.to_string(),
            // Exercise reconciliation without trying to decode the one-byte
            // marker objects as Arrow WAL segments.
            batch_size: 0,
            last_mirror_sync: Default::default(),
            last_reclaim: Default::default(),
        });
    assert_eq!(compactor.run_once().await.unwrap(), 0);

    // The election lease is intentionally long-lived in production. Release
    // it between passes so alternating claim owners model a pod handoff without
    // waiting for the five-minute TTL. The correctness-critical reconciliation
    // lease is released by `run_once` itself.
    owner
        .release_table_lease(MIRROR_ELECTION_LEASE)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires the compose object store from scripts/up.sh"]
async fn native_s3_pages_resume_wrap_and_repair_behind_cursor() {
    let store = minio_operator();
    assert!(
        store.info().full_capability().list_with_start_after,
        "the S3 backend must select native start_after pagination"
    );

    let test_root = unique_root();
    let prefix = format!("{test_root}/wal-mirror");
    let sealed = (0..INITIAL_OBJECTS).map(|i| format!("{prefix}/sealed-{i:05}.arrow"));
    let active = (0..17).map(|i| format!("{prefix}/_active/partial-{i:02}.arrow"));
    write_objects(&store, sealed.chain(active).collect()).await;

    let tmp = tempfile::tempdir().unwrap();
    let claim_uri = format!(
        "sqlite://{}?mode=rwc",
        tmp.path().join("claim.db").display()
    );
    let owner_a = SqlSegmentClaim::connect(&claim_uri, "owner-a")
        .await
        .unwrap();
    let owner_b = SqlSegmentClaim::connect(&claim_uri, "owner-b")
        .await
        .unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let cursor_id = cursor_id(&store, &prefix);

    run_sync_pass(1, &owner_a, &store, &prefix, &ice, tmp.path()).await;
    let first = owner_b.mirror_sync_cursor(&cursor_id).await.unwrap();
    assert_eq!(first.rotation, 0);
    assert_eq!(first.rotation_objects_examined, PAGE_OBJECTS);
    assert!(first.last_key.is_some());
    assert_eq!(owner_b.peek_pending().await.unwrap().segments, PAGE_OBJECTS);

    // This key sorts behind the durable cursor and must wait for the next
    // rotation. Owner B resumes owner A's cursor in the meantime.
    let behind_id = "a-behind-cursor";
    store
        .write(
            &format!("{prefix}/{behind_id}.arrow"),
            Bytes::from_static(b"x"),
        )
        .await
        .unwrap();

    run_sync_pass(2, &owner_b, &store, &prefix, &ice, tmp.path()).await;
    let second = owner_a.mirror_sync_cursor(&cursor_id).await.unwrap();
    assert_eq!(second.rotation, 0);
    assert_eq!(second.rotation_objects_examined, PAGE_OBJECTS * 2);
    assert!(second.last_key > first.last_key);
    assert_eq!(
        owner_a.peek_pending().await.unwrap().segments,
        PAGE_OBJECTS * 2
    );

    // The third page contains one original sealed object. `_active` objects
    // never consume the sealed-object budget or enter the catalog, and the
    // behind-cursor insert is still excluded by native S3 start_after.
    run_sync_pass(3, &owner_a, &store, &prefix, &ice, tmp.path()).await;
    let wrapped = owner_b.mirror_sync_cursor(&cursor_id).await.unwrap();
    assert_eq!(wrapped.last_key, None);
    assert_eq!(wrapped.rotation, 1);
    assert_eq!(wrapped.rotation_objects_examined, INITIAL_OBJECTS);
    assert_eq!(
        owner_b.peek_pending().await.unwrap().segments,
        INITIAL_OBJECTS
    );

    run_sync_pass(4, &owner_b, &store, &prefix, &ice, tmp.path()).await;
    let next_rotation = owner_a.mirror_sync_cursor(&cursor_id).await.unwrap();
    assert_eq!(next_rotation.rotation, 1);
    assert_eq!(next_rotation.rotation_objects_examined, PAGE_OBJECTS);
    assert!(next_rotation.last_key.is_some());
    assert_eq!(
        owner_a.peek_pending().await.unwrap().segments,
        INITIAL_OBJECTS + 1
    );

    let claimed = owner_a
        .try_claim(usize::try_from(INITIAL_OBJECTS + 1).unwrap())
        .await
        .unwrap();
    assert!(
        claimed.iter().any(|segment| segment.id == behind_id),
        "the next rotation did not repair the key inserted behind its cursor"
    );
    assert!(
        claimed
            .iter()
            .all(|segment| !segment.id.starts_with("partial-")),
        "an _active object was registered as a sealed segment"
    );

    store
        .delete_with(&format!("{test_root}/"))
        .recursive(true)
        .await
        .unwrap();
}
