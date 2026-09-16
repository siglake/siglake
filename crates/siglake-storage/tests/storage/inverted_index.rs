//! WS-5 slice B differential test: with the per-file inverted index enabled, a
//! `raw LIKE '%substr%'` query must return exactly the ground-truth matches —
//! the index produces a row-selection *superset* and the query engine re-checks
//! the exact `LIKE`, so the index must never drop a matching row. Exercises the
//! cross-row-group ordinal mapping (tiny row groups → many groups per file) and
//! the interplay with the trigram-bloom row-group skip.

use datafusion::prelude::SessionContext;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn inverted_index_query_matches_ground_truth() {
    // Index on; trigram bloom on (so row-group skipping is active alongside the
    // index selection); 1-byte row-group target ⇒ many row groups per file, to
    // exercise build_deletes_row_selection's cross-row-group walk.
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            target_row_group_bytes: Some(1),
            ..Default::default()
        });

    // A corpus with known substring membership. Several appends ⇒ several files,
    // each (with the 1-byte target) split into many row groups.
    let raws: Vec<String> = (0..200)
        .map(|i| match i % 5 {
            0 => format!("error connecting database row{i}"),
            1 => format!("user login okay session{i}"),
            2 => format!("database error timeout req{i}"),
            3 => format!("warning slow query detected{i}"),
            _ => format!("info heartbeat ping{i}"),
        })
        .collect();
    for chunk in raws.chunks(40) {
        let evs: Vec<Event> = chunk.iter().map(|r| Event::now(r.clone())).collect();
        ice.append_events(&evs).await.unwrap();
    }

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();

    // Index-answerable substrings (delimiter-free, >= MIN_TOKEN_LEN), absent
    // terms, fragments-of-tokens, and a not-answerable (delimiter-bearing) one
    // that must fall back — every case must equal the ground-truth count.
    for sub in [
        "database",
        "error",
        "login",
        "timeout",
        "warning",
        "heartbeat",
        "err",           // fragment of "error"
        "data",          // fragment of "database"
        "zzznope",       // absent
        "error timeout", // delimiter-bearing ⇒ index not used, must still be correct
    ] {
        let want = raws.iter().filter(|r| r.contains(sub)).count() as i64;
        let sql = format!("SELECT count(*) AS n FROM events WHERE raw LIKE '%{sub}%'");
        let got = count(&ctx, &sql).await;
        assert_eq!(got, want, "substring {sub:?}: index dropped or added rows");
    }

    // Combined with a dimensional predicate (host filter ANDs with the index
    // selection) — still exact. All events share host "localhost" (Event::now).
    let want_db: i64 = raws.iter().filter(|r| r.contains("database")).count() as i64;
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%database%' AND host = 'localhost'"
        )
        .await,
        want_db,
        "index selection must intersect correctly with a dimensional predicate"
    );
}
