//! Stage 1b/2 of the incremental group-count aggregate
//! (`docs/DESIGN_incremental_group_count_aggregate.md`): the commit path writes
//! a per-commit delta, and the read path folds the deltas the compactor has not
//! absorbed yet.
//!
//! The point of this file is to pin the ROUTING, not the answer. A test that
//! only checks `GROUP BY host` comes out exact passes just as happily when the
//! column is served by the inline object, by a footer sum, or by a full scan —
//! which is how a column can be "covered" and still cost three seconds. So this
//! asserts where each column's counts physically live: `host` must be absent
//! from the object the commit path rewrites (that object's size is a per-commit
//! cost, and holding 1.1M keys in it is what collapsed ingest on 2026-07-29)
//! and present in the delta stream.
//!
//! The mirror case — the default cap, where none of this machinery exists — is
//! `agg_delta_disabled.rs`.

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::{
    group_count_delta_sequence_number, GroupCountDelta, IcebergContext, SnapshotAggregates,
};

/// Comfortably over the inline ceiling (4,096) so `host` can only be served by
/// the incremental path, and over it in EVERY commit so the routing is not an
/// accident of how the rows happened to be split.
const DISTINCT_HOSTS: usize = 9_000;
const ROWS: usize = 27_000;
const COMMITS: usize = 3;

fn ev(secs: i64, host: &str) -> Event {
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: host.to_string(),
        source: "src".into(),
        // Low cardinality on purpose: the control column. It must keep taking
        // the inline path while `host` takes the new one.
        sourcetype: if secs % 2 == 0 {
            "app:json".into()
        } else {
            "syslog".into()
        },
        index: "main".into(),
        raw: format!("request from {host}"),
        attributes: None,
    }
}

/// The events table's metadata dir — found rather than hardcoded, so the test
/// keeps working if the layout moves.
/// The one incarnation directory under the events table's `metadata/` — every
/// aggregate artifact of that incarnation lives directly under it (#2919).
/// Found rather than hardcoded, so the test keeps working if the layout moves.
fn aggregate_dir(root: &std::path::Path) -> std::path::PathBuf {
    fn find(dir: &std::path::Path) -> Option<std::path::PathBuf> {
        for e in std::fs::read_dir(dir).ok()? {
            let p = e.ok()?.path();
            if !p.is_dir() {
                continue;
            }
            if p.join("siglake-aggregates.json").exists() {
                return Some(p);
            }
            if let Some(found) = find(&p) {
                return Some(found);
            }
        }
        None
    }
    find(root).expect("events table aggregate dir")
}

fn deltas(root: &std::path::Path) -> Vec<GroupCountDelta> {
    let dir = aggregate_dir(root).join("siglake-agg-deltas");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out: Vec<(i64, GroupCountDelta)> = entries
        .filter_map(|e| {
            let path = e.ok()?.path();
            // Parse through the production path parser, so the test also covers
            // the name the writer produced being a name the reader accepts.
            let rel = format!("siglake-agg-deltas/{}", path.file_name()?.to_str()?);
            let seq = group_count_delta_sequence_number(&rel)?;
            let d: GroupCountDelta = serde_json::from_slice(&std::fs::read(&path).ok()?).ok()?;
            Some((seq, d))
        })
        .collect();
    out.sort_by_key(|(seq, _)| *seq);
    out.into_iter().map(|(_, d)| d).collect()
}

#[tokio::test]
async fn a_wide_column_is_routed_to_deltas_and_kept_out_of_the_commit_path_object() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&root).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(2_000_000),
            ..Default::default()
        },
    );
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    let mut expected: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let mut evs = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        let h = format!("host-{:06}", i % DISTINCT_HOSTS);
        *expected.entry(h.clone()).or_default() += 1;
        evs.push(ev(base + i as i64, &h));
    }
    for chunk in evs.chunks(ROWS.div_ceil(COMMITS)) {
        ice.append_events(chunk).await.unwrap();
    }

    // 1. The object the commit path rewrites must NOT have grown a wide column.
    //    This is the assertion the 2026-07-29 collapse turns on: that object is
    //    read, decoded, re-encoded and written on every single commit, so a
    //    million keys in it is a per-commit cost.
    let bytes = std::fs::read(aggregate_dir(&root).join("siglake-aggregates.json")).unwrap();
    let inline: SnapshotAggregates = serde_json::from_slice(&bytes).unwrap();
    let inline_gc = inline.group_counts.expect("inline aggregate exists");
    assert!(
        inline_gc.column_total("host").is_none(),
        "`host` must stay out of the per-commit object, found {:?} keys",
        inline_gc.columns.get("host").map(|c| c.values.len())
    );
    assert_eq!(
        inline_gc.column_total("sourcetype"),
        Some(ROWS as u64),
        "a low-cardinality column keeps taking the inline path unchanged"
    );

    // 2. One delta per commit, each keyed by a distinct, increasing sequence
    //    number, together accounting for every row of the wide column.
    let deltas = deltas(&root);
    assert_eq!(deltas.len(), COMMITS, "one delta object per commit");
    let seqs: Vec<i64> = deltas.iter().map(|d| d.sequence_number).collect();
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "sequence numbers are distinct and increase with commit order: {seqs:?}"
    );
    let delta_rows: u64 = deltas
        .iter()
        .filter_map(|d| d.group_counts.as_ref()?.column_total("host"))
        .sum();
    assert_eq!(
        delta_rows, ROWS as u64,
        "the delta stream accounts for every row of the wide column"
    );

    // 3. And the served answer is exact — via the fold, since (1) proved the
    //    inline object cannot be the source.
    let rows = ice
        .grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .expect("grouped counts served");
    let got: std::collections::HashMap<String, u64> = rows
        .iter()
        .map(|(v, c)| (v.unwrap_or_default().to_string(), c))
        .collect();
    assert_eq!(got, expected, "counts must be exact for every host");
}
