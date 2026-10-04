//! Task #6482: the auto-promotion cost series exist at zero before the first
//! pass and the first Backfill bin.
//!
//! #5065 prices a promotion wave from the DELTA between two `/metrics`
//! scrapes, and its reader refuses a series that is absent at the before
//! boundary rather than reading absence as zero. Until the registration this
//! test covers, `siglake_auto_promotion_pass_duration_seconds` appeared only
//! once a pass had been sampled and the four
//! `siglake_compactor_promotion_backfill_*` series only once a Backfill bin
//! had committed, which made the first wave on a fresh namespace — the only
//! wave a young table ever has — the one wave that could not be measured.
//!
//! The recorder here is the one the binaries install
//! (`siglake_core::metrics::builder`), rendered as the reader scrapes it, so
//! "starts at zero" is asserted on the exposition and not only on the
//! registry. Installing it globally is what makes the Iceberg writes'
//! observations, taken on runtime worker threads, land in the same recorder.

use iceberg::TableIdent;
use siglake_core::Event;
use siglake_storage::iceberg::{
    preregister_auto_promotion_cost_series, IcebergContext, LevelPolicy, LeveledPassOptions,
    ReclusterPolicy,
};

const SAMPLE_FILES: usize = 4;
const SAMPLE_ROWS: usize = 4096;
const BACKFILL_FILE_COUNT: usize = 2;

const PASS_DURATION: &str = "siglake_auto_promotion_pass_duration_seconds";
const BACKFILL_FILES: &str = "siglake_compactor_promotion_backfill_files_total";
const BACKFILL_BYTES_IN: &str = "siglake_compactor_promotion_backfill_bytes_in_total";
const BACKFILL_BYTES_OUT: &str = "siglake_compactor_promotion_backfill_bytes_out_total";
const BACKFILL_DURATION: &str = "siglake_compactor_promotion_backfill_duration_seconds";

/// Every series name the wave reader takes a delta over.
const COST_SERIES: &[&str] = &[
    PASS_DURATION,
    BACKFILL_FILES,
    BACKFILL_BYTES_IN,
    BACKFILL_BYTES_OUT,
    BACKFILL_DURATION,
];

/// The value of one exposition line, addressed by its full `name{labels}`
/// prefix. `None` is an absent series — the state the reader refuses.
fn sample(exposition: &str, series: &str) -> Option<f64> {
    exposition.lines().find_map(|line| {
        let (head, value) = line.rsplit_once(' ')?;
        if head != series {
            return None;
        }
        value.parse().ok()
    })
}

/// `{table="..."}`, the backfill family's whole label set. `suffix` is the
/// histogram part (`_sum`, `_count`) and goes on the NAME, before the labels.
fn backfill_series(name: &str, suffix: &str, ident: &TableIdent) -> String {
    format!("{name}{suffix}{{table=\"{}\"}}", ident.name())
}

/// `{iceberg_namespace="...",table="..."}`, the sampling family's.
fn pass_series(name: &str, suffix: &str, ident: &TableIdent) -> String {
    format!(
        "{name}{suffix}{{iceberg_namespace=\"{}\",table=\"{}\"}}",
        ident.namespace(),
        ident.name()
    )
}

fn events(n: usize, attributes: impl Fn(usize) -> String) -> Vec<Event> {
    (0..n)
        .map(|i| Event::now(format!("row {i}")).with_attributes(Some(attributes(i))))
        .collect()
}

/// One test, because the recorder it renders is process-global.
#[tokio::test(flavor = "multi_thread")]
async fn cost_series_start_at_zero_and_still_report_the_first_wave() {
    let handle = siglake_core::metrics::builder()
        .expect("metrics builder")
        .install_recorder()
        .expect("install recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    for _ in 0..BACKFILL_FILE_COUNT {
        ice.append_events(&events(100, |i| {
            format!(r#"{{"k8s.namespace":"ns-{}"}}"#, i % 4)
        }))
        .await
        .unwrap();
    }

    // Before boundary as it stands without registration: the five series do
    // not exist at all, which is what #6478's reader refuses.
    let unregistered = handle.render();
    for name in COST_SERIES {
        assert!(
            !unregistered.contains(name),
            "{name} exists before anything registered it:\n{unregistered}"
        );
    }

    let idents = ice.auto_promotion_table_idents().await;
    let ident = ice.events_table_ident().clone();
    assert!(idents.contains(&ident), "{idents:?}");
    for ident in &idents {
        preregister_auto_promotion_cost_series(&ident.namespace().to_string(), ident.name());
    }

    // Registration observes nothing: the counters read 0 and the histograms
    // render an empty distribution, so the scrape carries a before boundary
    // that is a real zero rather than a first sample.
    let registered = handle.render();
    for name in [BACKFILL_FILES, BACKFILL_BYTES_IN, BACKFILL_BYTES_OUT] {
        assert_eq!(
            sample(&registered, &backfill_series(name, "", &ident)),
            Some(0.0),
            "{name} is not exported at zero:\n{registered}"
        );
    }
    for series in [
        backfill_series(BACKFILL_DURATION, "_sum", &ident),
        backfill_series(BACKFILL_DURATION, "_count", &ident),
        pass_series(PASS_DURATION, "_sum", &ident),
        pass_series(PASS_DURATION, "_count", &ident),
    ] {
        assert_eq!(
            sample(&registered, &series),
            Some(0.0),
            "{series} is not exported at zero:\n{registered}"
        );
    }

    // The measured wave: one sampling pass, then one Backfill bin rewriting
    // the two files that predate the declaration.
    let pre_pass = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(pre_pass.len(), BACKFILL_FILE_COUNT);
    let expected_bytes_in = pre_pass
        .iter()
        .map(|file| file.file_size_in_bytes())
        .sum::<u64>();
    let promoted = ice
        .auto_promote_hot_keys(0.5, 16, SAMPLE_FILES, SAMPLE_ROWS)
        .await
        .unwrap();
    assert_eq!(promoted.len(), 1, "{promoted:?}");
    let bins = ice
        .recluster_pass_leveled(
            &ident,
            &["host"],
            &LevelPolicy {
                trigger_files: 100,
                max_merge_gen: 0,
                ..Default::default()
            },
            ReclusterPolicy::default(),
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(bins.len(), 1, "one backfill bin should cover the fixture");

    // Re-registration is what a second compactor cycle does, and it must not
    // reset or re-create what the wave recorded.
    for ident in &idents {
        preregister_auto_promotion_cost_series(&ident.namespace().to_string(), ident.name());
    }

    let after = handle.render();
    assert_eq!(
        sample(&after, &backfill_series(BACKFILL_FILES, "", &ident)),
        Some(BACKFILL_FILE_COUNT as f64),
        "{after}"
    );
    assert_eq!(
        sample(&after, &backfill_series(BACKFILL_BYTES_IN, "", &ident)),
        Some(expected_bytes_in as f64),
        "{after}"
    );
    assert!(
        sample(&after, &backfill_series(BACKFILL_BYTES_OUT, "", &ident))
            .is_some_and(|bytes| bytes > 0.0),
        "{after}"
    );
    assert_eq!(
        sample(
            &after,
            &backfill_series(BACKFILL_DURATION, "_count", &ident)
        ),
        Some(1.0),
        "{after}"
    );
    assert_eq!(
        sample(&after, &pass_series(PASS_DURATION, "_count", &ident)),
        Some(1.0),
        "one sampling pass ran on the events table:\n{after}"
    );
    for series in [
        backfill_series(BACKFILL_DURATION, "_sum", &ident),
        pass_series(PASS_DURATION, "_sum", &ident),
    ] {
        assert!(
            sample(&after, &series).is_some_and(|secs| secs > 0.0),
            "{series} recorded no time:\n{after}"
        );
    }
}
