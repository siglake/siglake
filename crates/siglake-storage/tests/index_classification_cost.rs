//! #4074 measurement: what the distributed gate's classification costs, before
//! (`get_index`, an uncached `load_table`) and after (`is_managed_index`, the
//! bounded-staleness table cache), against metadata.json of growing size.
//!
//! `#[ignore]`d — it is a measurement, not an assertion. Run it with
//! `cargo test --release -p siglake-storage --test index_classification_cost -- --ignored --nocapture`.
//!
//! The warehouse here is a local directory, so the numbers are the FLOOR: the
//! only cost separating the arms is parsing a metadata.json off page cache. On
//! object storage the same read adds a GET round trip and the transfer of a
//! file whose snapshot history grows with every commit, which is the axis this
//! table sweeps.

use siglake_core::index_config::IndexConfig;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

const INDEX: &str = "cost-idx";

fn index_config() -> IndexConfig {
    IndexConfig {
        index_id: INDEX.to_string(),
        doc_mapping: IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    }
}

/// Median of `iterations` calls, in microseconds.
fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

#[tokio::test]
#[ignore = "measurement"]
async fn classification_cost_by_metadata_size() {
    const ITERATIONS: usize = 40;

    println!("snapshots  metadata.json   get_index (old)   is_managed_index (new)   ratio");
    for snapshots in [1usize, 32, 256] {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = IcebergContext::open(&warehouse).await.unwrap();
        let config = index_config();
        ice.create_index(&config).await.unwrap();
        let ident = ice.index_table_ident(INDEX);
        for commit in 0..snapshots {
            let events = vec![Event::now(format!("cost row {commit}"))];
            let batch = siglake_core::events_to_record_batch(&events).unwrap();
            let mapped = siglake_core::map_carrier_batch(&batch, &config).unwrap();
            ice.append_to_table(&ident, mapped, &[]).await.unwrap();
        }

        let metadata_bytes = metadata_size(&warehouse);

        // Warm both arms once: the new one populates the table cache, the old
        // one warms the page cache for the file it re-reads every time.
        ice.is_managed_index(INDEX).await.unwrap();
        ice.get_index(INDEX).await.unwrap().unwrap();

        // Interleaved, so a drifting machine moves both arms together.
        let mut old = Vec::with_capacity(ITERATIONS);
        let mut new = Vec::with_capacity(ITERATIONS);
        for _ in 0..ITERATIONS {
            let started = std::time::Instant::now();
            ice.get_index(INDEX).await.unwrap().unwrap();
            old.push(started.elapsed().as_secs_f64() * 1e6);

            let started = std::time::Instant::now();
            ice.is_managed_index(INDEX).await.unwrap();
            new.push(started.elapsed().as_secs_f64() * 1e6);
        }
        let (old, new) = (median(old), median(new));
        println!(
            "{snapshots:>9}  {:>10} KiB   {old:>11.0} us   {new:>18.0} us   {:>5.1}x",
            metadata_bytes / 1024,
            old / new
        );
    }
}

/// Bytes of the index table's current metadata.json (the largest one present).
fn metadata_size(warehouse: &std::path::Path) -> u64 {
    let mut largest = 0;
    for namespace in std::fs::read_dir(warehouse).unwrap() {
        let table_metadata = namespace.unwrap().path().join(INDEX).join("metadata");
        if !table_metadata.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(&table_metadata).unwrap() {
            let entry = entry.unwrap();
            if entry.path().extension().is_some_and(|ext| ext == "json") {
                largest = largest.max(entry.metadata().unwrap().len());
            }
        }
    }
    largest
}
