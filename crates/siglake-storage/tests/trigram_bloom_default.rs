//! Trigram blooms over `raw` are written BY DEFAULT (flipped 2026-07-28).
//!
//! They power `LIKE '%substr%'` pruning. The read path is ungated and falls
//! through harmlessly when a file has no bloom, so the write-side default is
//! what decides whether the feature is live at all — and until now it was
//! opt-in, which meant every default deployment (and every benchmark round)
//! silently ran without it.
//!
//! Two things are asserted here, because the default only matters if the bloom
//! is both PRESENT and USEFUL: files carry the bloom keys without any env set,
//! and the bloom actually rejects a substring that appears nowhere while
//! accepting one that does. A bloom that says "maybe" to everything would
//! satisfy the first assertion and be worthless.
//!
//! Own test binary: `SIGLAKE_PARQUET_RAW_TOKEN_BLOOM` is process-global and
//! latched once.

use chrono::{TimeZone, Utc};
use siglake_bloom::{RAW_TRIGRAM_BLOOM_KV_KEY, RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

fn footer_kv(path: &std::path::Path, key: &str) -> Option<String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let bytes = std::fs::read(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|kv| {
            kv.iter()
                .find(|e| e.key == key)
                .and_then(|e| e.value.clone())
        })
}

fn find_parquet(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(find_parquet(&p));
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(p);
            }
        }
    }
    out
}

fn ev(secs: i64, raw: &str) -> Event {
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: "h1".into(),
        source: "src".into(),
        sourcetype: "app:json".into(),
        index: "main".into(),
        raw: raw.to_string(),
        attributes: None,
    }
}

#[tokio::test]
async fn trigram_blooms_are_written_by_default_and_prune() {
    // Deliberately set NOTHING: this asserts the default, which is the point.
    assert!(
        std::env::var("SIGLAKE_PARQUET_RAW_TOKEN_BLOOM").is_err(),
        "this test must run with the env unset so it exercises the default"
    );

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    let evs: Vec<Event> = (0..400)
        .map(|i| {
            ev(
                base + i,
                &format!("GET /english/nav_inet_{i}.html HTTP/1.0"),
            )
        })
        .collect();
    ice.append_events(&evs).await.unwrap();

    let files = find_parquet(tmp.path());
    assert!(!files.is_empty(), "ingest must produce a data file");

    let mut with_file_bloom = 0;
    let mut with_rowgroup_bloom = 0;
    for f in &files {
        if let Some(hex) = footer_kv(f, RAW_TRIGRAM_BLOOM_KV_KEY) {
            with_file_bloom += 1;

            // Present is not enough — it has to discriminate.
            let bloom = siglake_bloom::TokenBloom::from_hex(&hex).expect("bloom decodes");
            let probe = |s: &str| {
                siglake_bloom::query_trigrams(s)
                    .map(|grams| grams.iter().all(|g| bloom.maybe_contains(g)))
                    .unwrap_or(true)
            };
            assert!(
                probe("nav_inet"),
                "a substring that IS in the corpus must not be pruned (false negatives \
                 would drop real rows)"
            );
            assert!(
                !probe("zzqxjvwq"),
                "a substring present nowhere must be pruned — a bloom that says \
                 'maybe' to everything is worthless"
            );
        }
        if footer_kv(f, RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY).is_some() {
            with_rowgroup_bloom += 1;
        }
    }

    assert!(
        with_file_bloom > 0,
        "no file carried {RAW_TRIGRAM_BLOOM_KV_KEY} — the default did not take effect"
    );
    assert!(
        with_rowgroup_bloom > 0,
        "no file carried {RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY}"
    );
}
