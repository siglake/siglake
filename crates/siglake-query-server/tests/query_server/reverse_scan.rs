use crate::support;

use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use datafusion::physical_plan::displayable;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, QueryScanConfig, ServerLimits};
use siglake_storage::{iceberg::IcebergContext, PreferredScanOrder};

macro_rules! require_loopback {
    () => {
        if !support::loopback_available() {
            return;
        }
    };
}

struct Server {
    base: String,
    handle: tokio::task::JoinHandle<()>,
    _tmp: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn event_at(base: chrono::DateTime<Utc>, seconds: i64) -> Event {
    let mut event = Event::now(format!("row-{seconds}"));
    event.timestamp = base + Duration::seconds(seconds);
    event.raw = format!("row-{seconds}");
    event
}

async fn spawn_with_event_files(files: Vec<Vec<Event>>) -> (Server, AppState) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    for file in files {
        ice.append_events(&file).await.unwrap();
    }
    let state = AppState::new(Arc::new(ice), AuthConfig::open())
        .with_limits(ServerLimits::default())
        .with_query_scan(QueryScanConfig {
            target_partitions: Some(2),
            ..Default::default()
        });
    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        Server {
            base: format!("http://{addr}"),
            handle,
            _tmp: tmp,
        },
        state,
    )
}

#[tokio::test]
async fn explicit_desc_order_uses_reverse_scan_without_sortexec() {
    require_loopback!();

    let base = Utc.with_ymd_and_hms(2026, 6, 4, 12, 0, 0).unwrap();
    let files = vec![
        vec![event_at(base, 0), event_at(base, 1), event_at(base, 2)],
        vec![event_at(base, 10), event_at(base, 11), event_at(base, 12)],
        vec![event_at(base, 20), event_at(base, 21), event_at(base, 22)],
        vec![event_at(base, 30), event_at(base, 31), event_at(base, 32)],
    ];
    let (srv, state) = spawn_with_event_files(files).await;

    let query = "SELECT timestamp FROM events ORDER BY timestamp DESC LIMIT 100";
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query": query,
            "default_order": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let rows = body["rows"].as_array().unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|row| {
            row["timestamp"]
                .as_str()
                .unwrap()
                .parse::<chrono::DateTime<Utc>>()
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();
    let want: Vec<i64> = [32, 31, 30, 22, 21, 20, 12, 11, 10, 2, 1, 0]
        .into_iter()
        .map(|s| (base + Duration::seconds(s)).timestamp_nanos_opt().unwrap())
        .collect();
    assert_eq!(got, want);

    let ctx = state
        .query_scan
        .session_context_with_order(Some(PreferredScanOrder { descending: true }));
    state.ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx.sql(query).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_str = format!("{}", displayable(plan.as_ref()).indent(true));
    assert!(
        plan_str.contains("SortPreservingMergeExec"),
        "reverse scan should feed a sort-preserving merge:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("SortExec"),
        "explicit DESC order should not require SortExec/TopK:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("TopK"),
        "reverse scan should avoid a TopK sort node:\n{plan_str}"
    );
}

/// WS-3 cluster segmentation: a LIMIT plan packs every file into ONE
/// partition, and the old all-or-nothing disjoint check refused ordering for
/// any layout with a single overlap once the file count passed the merge
/// fan-in cap (1TB round: 761 files → `fan_in` refusal → full 2B-row scan →
/// mid-flight breaker). With clustering, only the transitively-overlapping
/// files merge (fan-in = the largest local cluster) and the disjoint rest
/// concatenates — so a 20-file partition with one overlapping pair must
/// advertise (no SortExec) and return exactly ordered rows through the
/// mixed concat + merge path, in both directions.
#[tokio::test]
async fn clustered_overlap_advertises_and_orders_exactly() {
    require_loopback!();

    let base = Utc.with_ymd_and_hms(2026, 6, 4, 12, 0, 0).unwrap();
    // 18 time-disjoint files (3 rows each at 100i, 100i+2, 100i+4 seconds)…
    let mut files: Vec<Vec<Event>> = (0..18)
        .map(|i| {
            vec![
                event_at(base, i * 100),
                event_at(base, i * 100 + 2),
                event_at(base, i * 100 + 4),
            ]
        })
        .collect();
    // …plus one OVERLAPPING pair above them (cluster of 2).
    files.push(vec![
        event_at(base, 2000),
        event_at(base, 2004),
        event_at(base, 2008),
    ]);
    files.push(vec![
        event_at(base, 2001),
        event_at(base, 2005),
        event_at(base, 2009),
    ]);
    let (srv, state) = spawn_with_event_files(files).await;

    let fetch = |query: &'static str| {
        let base_url = srv.base.clone();
        async move {
            let resp = reqwest::Client::new()
                .post(format!("{base_url}/api/v1/sql"))
                .json(&serde_json::json!({ "query": query, "default_order": false }))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();
            body["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    row["timestamp"]
                        .as_str()
                        .unwrap()
                        .parse::<chrono::DateTime<Utc>>()
                        .unwrap()
                        .timestamp_nanos_opt()
                        .unwrap()
                })
                .collect::<Vec<i64>>()
        }
    };
    let ns = |s: i64| (base + Duration::seconds(s)).timestamp_nanos_opt().unwrap();

    // DESC: top-8 interleaves the overlapping pair, then the disjoint tail.
    let got = fetch("SELECT timestamp FROM events ORDER BY timestamp DESC LIMIT 8").await;
    let want: Vec<i64> = [2009, 2008, 2005, 2004, 2001, 2000, 1704, 1702]
        .into_iter()
        .map(ns)
        .collect();
    assert_eq!(got, want, "DESC through mixed concat + cluster merge");

    // ASC: bottom-5 from the disjoint head.
    let got = fetch("SELECT timestamp FROM events ORDER BY timestamp ASC LIMIT 5").await;
    let want: Vec<i64> = [0, 2, 4, 100, 102].into_iter().map(ns).collect();
    assert_eq!(got, want, "ASC through the disjoint head");

    // The gate must ADVERTISE through this layout (the old gate refused it as
    // fan_in and fell back to a blocking sort).
    for descending in [true, false] {
        let ctx = state
            .query_scan
            .session_context_with_order(Some(PreferredScanOrder { descending }));
        state.ice.register_with_datafusion(&ctx).await.unwrap();
        let dir = if descending { "DESC" } else { "ASC" };
        let df = ctx
            .sql(&format!(
                "SELECT timestamp FROM events ORDER BY timestamp {dir} LIMIT 8"
            ))
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let plan_str = format!("{}", displayable(plan.as_ref()).indent(true));
        assert!(
            !plan_str.contains("SortExec"),
            "clustered layout must advertise {dir} ordering (no SortExec):\n{plan_str}"
        );
    }
}

/// The 1TB wide-file shape: one file spanning the whole range transitively
/// chains 20+ narrow disjoint files into a single cluster — far past the
/// 16-way merge cap as a flat merge, but only DEPTH 2 as a layered merge
/// (narrow run ∥ wide file). The gate must advertise, and the layered merge
/// must interleave the wide file's rows exactly.
#[tokio::test]
async fn wide_file_chain_advertises_via_layering() {
    require_loopback!();

    let base = Utc.with_ymd_and_hms(2026, 6, 4, 12, 0, 0).unwrap();
    // One WIDE file with rows across the whole span (at 55, 1055, 2155 s)…
    let mut files: Vec<Vec<Event>> = vec![vec![
        event_at(base, 55),
        event_at(base, 1055),
        event_at(base, 2155),
    ]];
    // …and 22 narrow disjoint files (2 rows each at 100i, 100i+2).
    files.extend((0..22).map(|i| vec![event_at(base, i * 100), event_at(base, i * 100 + 2)]));
    let (srv, state) = spawn_with_event_files(files).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query": "SELECT timestamp FROM events ORDER BY timestamp DESC LIMIT 6",
            "default_order": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let got: Vec<i64> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            row["timestamp"]
                .as_str()
                .unwrap()
                .parse::<chrono::DateTime<Utc>>()
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();
    // Newest-first: the wide file's newest row (2155) leads, then the narrow
    // files' rows in order.
    let want: Vec<i64> = [2155, 2102, 2100, 2002, 2000, 1902]
        .into_iter()
        .map(|s| (base + Duration::seconds(s)).timestamp_nanos_opt().unwrap())
        .collect();
    assert_eq!(got, want, "layered merge interleaves the wide file exactly");

    // And the plan advertises (no blocking sort) despite the 23-file chain.
    let ctx = state
        .query_scan
        .session_context_with_order(Some(PreferredScanOrder { descending: true }));
    state.ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx
        .sql("SELECT timestamp FROM events ORDER BY timestamp DESC LIMIT 6")
        .await
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_str = format!("{}", displayable(plan.as_ref()).indent(true));
    assert!(
        !plan_str.contains("SortExec"),
        "wide-file chain must advertise via layering (no SortExec):\n{plan_str}"
    );
}
