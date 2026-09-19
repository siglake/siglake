//! Process-wide Prometheus metrics setup. Each binary calls
//! [`init`] once at startup with a `--metrics-bind` address; that spawns
//! a tiny axum server on `/metrics` that serves the global Prometheus
//! recorder.
//!
//! Metric naming convention: `siglake_<subsystem>_<name>{labels}`.
//! Histograms get the `_seconds` / `_bytes` suffix per Prometheus convention.
//!
//! Exposition form. `metrics-exporter-prometheus` renders a
//! `metrics::histogram!` as a Prometheus *summary* (`{quantile="0.99"}`
//! lines, no `_bucket` series) unless the recorder is handed buckets for that
//! name. A summary cannot be aggregated across pods and `histogram_quantile`
//! reads nothing from it, so every latency histogram (name ending in
//! `_seconds`) and the per-call count histograms in [`COUNT_HISTOGRAMS`] are
//! configured as true histograms by [`builder`]. Everything else (`_bytes`,
//! `_rows`, ratios, ...) deliberately remains a summary: no shipped consumer
//! needs aggregatable quantiles for those families, and their scales differ
//! too much for one useful generic bucket layout. Add per-family buckets here
//! alongside any future `_bucket` consumer.
//!
//! Pre-registration. The `metrics` crate creates a series on its first write,
//! so a counter that has never been incremented is absent from `/metrics`,
//! and a Prometheus `increase()` over a series whose first sample IS the
//! first increment reads nothing: `increase()` needs two samples, and the
//! first one carries no history. Every `increase(...) > 0` alert in the chart
//! therefore missed the first event on a fresh pod. Each binary calls
//! [`preregister`] with its catalog ([`INGESTER_ALERTED_COUNTERS`],
//! [`COMPACTOR_ALERTED_COUNTERS`], [`QUERY_SERVER_ALERTED_COUNTERS`]) right
//! after [`init`], so those series exist at 0 from the first scrape and the
//! first increment is a visible delta. `scripts/check-chart.py` holds the
//! PrometheusRule to these lists: every counter it reads through `increase()`
//! must be in one of them or in [`UNREGISTERABLE_ALERTED_COUNTERS`].

use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

/// Bucket upper bounds, in seconds, for every histogram whose name ends in
/// `_seconds`: sub-millisecond ingest requests up through multi-minute
/// compaction cycles and mirror-sync passes. Whole mirror rotations use the
/// wider [`MIRROR_ROTATION_DURATION_BUCKETS_SECONDS`] override below.
pub const LATENCY_BUCKETS_SECONDS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0,
    120.0, 300.0, 600.0, 1800.0,
];

/// Whole mirror rotations include the one-minute gaps between bounded pages,
/// so a 24-hour retained prefix needs hour-to-day buckets without adding those
/// series to every request-latency histogram.
pub const MIRROR_ROTATION_DURATION_BUCKETS_SECONDS: &[f64] = &[
    60.0, 300.0, 600.0, 1800.0, 3600.0, 10_800.0, 21_600.0, 43_200.0, 86_400.0,
];

/// Bucket upper bounds for the per-call count histograms in
/// [`COUNT_HISTOGRAMS`]. The `0` bucket is deliberate: a healthy fleet folds
/// zero deltas per read, and `histogram_quantile` can only report exactly
/// zero when a bucket ends there.
pub const COUNT_BUCKETS: &[f64] = &[
    0.0, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10_000.0,
    20_000.0, 50_000.0, 100_000.0,
];

/// A completed rotation covers the whole retained mirror population. The
/// design's 50K-EPS/24-hour estimate is about 1.05 million objects, well above
/// the per-call count families' useful range.
pub const MIRROR_ROTATION_OBJECT_BUCKETS: &[f64] = &[
    0.0,
    1_000.0,
    5_000.0,
    10_000.0,
    20_000.0,
    50_000.0,
    100_000.0,
    200_000.0,
    500_000.0,
    1_000_000.0,
    2_000_000.0,
];

/// Rows decoded by one decoded-file-cache population (#4890). The edges that
/// matter are 131,071 and 131,072: the write path's `MIN_ROW_GROUP_ROWS` is
/// 131,072, so a population closed at least one row group at the shipped floor
/// exactly when it was handed 131,072 rows or more. `le` is inclusive, so the
/// fraction BELOW the floor is `le="131071"` and the fraction at or above it is
/// `+Inf` minus that; the 131,072 edge next to it isolates the population that
/// stopped precisely on the boundary. A row group may be far larger than the
/// floor (`MAX_ROW_GROUP_ROWS` is 4 Mi), so crossing 131,072 rows does not by
/// itself prove a group was completed — that needs the file's footer geometry,
/// which is why the reader takes it as a separate input.
pub const POPULATE_ROW_BUCKETS: &[f64] = &[
    0.0,
    1.0,
    100.0,
    1_000.0,
    8_192.0,
    32_768.0,
    65_536.0,
    131_071.0,
    131_072.0,
    262_144.0,
    524_288.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
];

/// Count histograms exported in histogram form, matched by full name.
pub const COUNT_HISTOGRAMS: &[&str] = &[
    "siglake_group_count_deltas_folded",
    "siglake_group_count_tier2_files_per_call",
    "siglake_compactor_mirror_sync_objects",
    "siglake_compactor_mirror_sync_rotation_objects",
];

/// The recorder configuration every binary installs: a [`PrometheusBuilder`]
/// carrying the bucket layout above. Pure, so tests can build a local
/// recorder from it without installing the global one.
pub fn builder() -> Result<PrometheusBuilder> {
    let mut builder = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Suffix("_seconds".to_string()),
            LATENCY_BUCKETS_SECONDS,
        )
        .context("latency buckets")?;
    for name in COUNT_HISTOGRAMS {
        builder = builder
            .set_buckets_for_metric(Matcher::Full((*name).to_string()), COUNT_BUCKETS)
            .with_context(|| format!("count buckets for {name}"))?;
    }
    builder
        .set_buckets_for_metric(
            Matcher::Full("siglake_compactor_mirror_sync_rotation_duration_seconds".to_string()),
            MIRROR_ROTATION_DURATION_BUCKETS_SECONDS,
        )
        .context("mirror rotation duration buckets")?
        .set_buckets_for_metric(
            Matcher::Full("siglake_compactor_mirror_sync_rotation_objects".to_string()),
            MIRROR_ROTATION_OBJECT_BUCKETS,
        )
        .context("mirror rotation object buckets")?
        .set_buckets_for_metric(
            Matcher::Full("siglake_query_scan_file_cache_populate_rows".to_string()),
            POPULATE_ROW_BUCKETS,
        )
        .context("file cache population row buckets")
}

/// A counter a shipped alert reads through `increase()`, with every label set
/// the code emits it under that is known before the first increment.
///
/// `series` holds one entry per series to create at 0; [`UNLABELLED`] for a
/// counter recorded without labels. A label value only known at the moment of
/// the increment (a column name, an index table) cannot be listed: such a
/// counter goes in [`UNREGISTERABLE_ALERTED_COUNTERS`] with the reason, or,
/// when one value is known ahead (the `events` table), lists just that one and
/// says so in its comment. `scripts/check-chart.py` reads these literals and
/// holds every static label set recorded under a listed name to its entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlertedCounter {
    pub name: &'static str,
    pub series: &'static [&'static [(&'static str, &'static str)]],
}

/// The `series` of an [`AlertedCounter`] recorded without labels.
pub const UNLABELLED: &[&[(&str, &str)]] = &[&[]];

/// #1561's table-metadata cache fences, pre-registered by every binary that
/// reads a table through that cache while something invalidates it — the query
/// server and the compactor (`siglake-storage` invalidates only on its own
/// commits, and the ingester writes the WAL, not the catalog).
///
/// All three actions are listed although only `unpublished` is alerted on:
/// `superseded` and `reload` are healthy publication contention, and
/// `SiglakeTableCacheUnpublished` is read against them — a table whose reloads
/// are ALL being fenced out shows a sustained `unpublished` rate while the
/// healthy arms sit still, which an operator can only see if those arms exist
/// at 0. `siglake_storage`'s
/// `every_fence_action_recorded_here_is_preregistered` holds this vocabulary to
/// the call sites.
pub const TABLE_CACHE_FENCED: AlertedCounter = AlertedCounter {
    name: "siglake_iceberg_table_cache_fenced_total",
    series: &[
        &[("action", "superseded")],
        &[("action", "reload")],
        &[("action", "unpublished")],
    ],
};

/// Counters the ingester (`siglake ingest-server`) pre-registers: the WAL
/// mirror's permanent failures, the WAL's integrity and recovery counters, and
/// the two refusal counters on the request path.
pub const INGESTER_ALERTED_COUNTERS: &[AlertedCounter] = &[
    AlertedCounter {
        name: "siglake_ingest_lane_refused_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_ingest_tenant_denied_total",
        series: &[
            &[("reason", "not_allowed")],
            &[("reason", "claim_missing")],
            &[("reason", "claim_invalid")],
            &[("reason", "header_mismatch")],
            // The single-tenant default's refusal. It exists at 0 on a fresh
            // install for the same reason the others do: an upgrade that
            // silently stops routing a client's `X-Scope-OrgID` has to be
            // visible on the FIRST refused request, not on the second.
            &[("reason", "header_not_trusted")],
            // `--max-tenants`. The operator who set the cap is the one who
            // needs to see it bite, and on an ingester below its cap this arm
            // would otherwise not exist at all.
            &[("reason", "at_capacity")],
        ],
    },
    AlertedCounter {
        name: "siglake_wal_mirror_register_abandoned_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_wal_mirror_upload_abandoned_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_wal_crc_mismatch_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_wal_partials_adopted_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_wal_partial_tail_dropped_total",
        series: UNLABELLED,
    },
];

/// Counters the compactor (`siglake compactor`, not `--once`, which serves no
/// metrics) pre-registers. `siglake_wal_crc_mismatch_total` is here too: the
/// drain reads sealed segments through the same CRC check as ingest replay.
/// The two group-count counters are labelled by Iceberg namespace and table
/// (and rebuild outcome) and are listed for the default namespace's events
/// table only (`siglake_storage::iceberg::NAMESPACE` and `TABLE_NAME`, held
/// equal by tests there); an event on an index table, in a `tenant_*`
/// namespace, or under a base namespace moved off the default by
/// `SIGLAKE_TENANT_NAMESPACE` is a series this cannot know ahead, and its
/// first increment stays invisible to `increase()`.
/// `siglake_compactor_mirror_sync_total` is the activity arm of
/// `SiglakeMirrorReconciliationStalled`: without the series at 0, a compactor
/// whose very first reconciliation pass never wraps around looks idle to
/// `increase()` rather than stalled, which is exactly the case the alert is for.
/// The examined-to-date gauge is dashboard progress, not an activity arm: a
/// live pod retains its last gauge value when reconciliation is switched off or
/// it loses the lease, and a listing failure happens before that gauge exists.
/// `siglake_compactor_cycles_total{outcome="catalog_sync_error"}` reports that
/// failure directly; all literal outcome series are listed so the catalog stays
/// exhaustive as required by `check-chart.py`.
/// `siglake_compactor_delete_tasks_stalled_total` is the alerting arm of
/// `SiglakeDeleteTaskStalled`. Both `state` values are static (the emitter
/// picks between two `&'static str`s, so check-chart.py sees a dynamic site and
/// cannot hold the catalog to it — `delete_task_series_are_preregistered` in
/// siglake-compactor does that instead), and without them the FIRST stranded
/// GDPR deletion on a fresh pod is invisible to `increase()`, which is the one
/// case the alert exists for. Its `_nonterminal` gauge sibling is deliberately
/// absent: gauges are not pre-registered, and that one is dashboard-only.
pub const COMPACTOR_ALERTED_COUNTERS: &[AlertedCounter] = &[
    // A snapshot can carry only one Iceberg statistics file. A v1 rebuild that
    // reaches a rewrite snapshot which already carries seg2 blobs is deferred
    // instead of replacing those blobs. The dashboard reads the first refusal
    // through `increase()`, so the bounded reason series must start at zero.
    AlertedCounter {
        name: "siglake_index_registration_deferred_total",
        series: &[&[("reason", "snapshot_has_statistics")]],
    },
    // Statistics retirement runs inside the elected snapshot-expiry pass.
    // Register every outcome before that first pass so a clean table reads 0
    // rather than absent; the reclaimed-byte sibling is emitted by the
    // age-gated orphan sweep that consumes the retired Puffin objects.
    AlertedCounter {
        name: "siglake_iceberg_statistics_removed_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_iceberg_statistics_retirement_skipped_total",
        series: &[
            &[("reason", "unowned_blob_type")],
            &[("reason", "missing_data_file")],
        ],
    },
    AlertedCounter {
        name: "siglake_gc_bytes_reclaimed_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_consumed_proof_cap_refusals_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_compactor_reclaim_unprovable_total",
        series: UNLABELLED,
    },
    // #4913: a mirror object the ledger mark never reached, swept locally at
    // the 3600-second ceiling so the WAL volume stays bounded. Both series
    // exist at 0 on every compactor, whether or not mirror reclamation is on:
    // the dashboard panel that reads them has to distinguish "off" and "on and
    // leaking nothing" from "no data".
    AlertedCounter {
        name: "siglake_compactor_mirror_unreclaimed_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_compactor_mirror_mark_errors_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_compactor_watchdog_trips_total",
        series: &[
            &[("stage", "expire")],
            &[("stage", "drain")],
            &[("stage", "agg_fold")],
            &[("stage", "agg_short_repair")],
            &[("stage", "inline_coverage_census")],
            &[("stage", "recluster")],
            &[("stage", "delete_tasks")],
        ],
    },
    AlertedCounter {
        name: "siglake_group_count_delta_write_failures_total",
        series: &[&[("iceberg_namespace", "siglake"), ("table", "events")]],
    },
    // #3799: an append's contribution to the inline aggregate object exists
    // nowhere else, so a publication that spends its four attempts leaves the
    // object short of `total-records` for good. Listed with the default
    // namespace's `events` alone for the same reason as the delta counter
    // above: an index table's name, and a tenant namespace's, are known only
    // at the increment.
    AlertedCounter {
        name: "siglake_side_aggregate_publish_failures_total",
        series: &[&[("iceberg_namespace", "siglake"), ("table", "events")]],
    },
    AlertedCounter {
        name: "siglake_group_count_auto_rebuilds_total",
        series: &[
            &[
                ("iceberg_namespace", "siglake"),
                ("table", "events"),
                ("outcome", "success"),
            ],
            &[
                ("iceberg_namespace", "siglake"),
                ("table", "events"),
                ("outcome", "incomplete"),
            ],
            &[
                ("iceberg_namespace", "siglake"),
                ("table", "events"),
                ("outcome", "failed"),
            ],
        ],
    },
    // #3000: the maintenance census's verdict on an aggregate that is short of
    // `record_count` with every contribution accounted for. `detected` is the
    // default-install series — automatic repair is opt-in — so it has to exist
    // at 0 from the compactor's first scrape or the first find is invisible to
    // `increase()`. Listed with the default namespace's `events` alone for the
    // same reason as the counters above: an index table's name, and a tenant
    // namespace's, are known only at the increment.
    AlertedCounter {
        name: "siglake_group_count_short_aggregates_total",
        series: &[
            &[
                ("iceberg_namespace", "siglake"),
                ("table", "events"),
                ("outcome", "detected"),
            ],
            &[
                ("iceberg_namespace", "siglake"),
                ("table", "events"),
                ("outcome", "repaired"),
            ],
            &[
                ("iceberg_namespace", "siglake"),
                ("table", "events"),
                ("outcome", "incomplete"),
            ],
            &[
                ("iceberg_namespace", "siglake"),
                ("table", "events"),
                ("outcome", "failed"),
            ],
        ],
    },
    // #4674: one increment per completed inline-coverage census pass. It is the
    // liveness arm of `SiglakeInlineCoverageUnproven`, whose other arm is a
    // last-observation gauge — a pod that stops censusing keeps serving its last
    // reading, and for a `> 0` alert that is stale-BAD. A fresh compactor that
    // finds an unproven table on its FIRST pass has to pass the liveness arm on
    // that same pass, so the series must exist at 0 before it: without this
    // entry the very first census would raise the gauge and the `increase()`
    // beside it would still read nothing.
    AlertedCounter {
        name: "siglake_inline_coverage_census_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_compactor_mirror_sync_total",
        series: UNLABELLED,
    },
    // #3143: the local drain setting a segment aside under `poison/`. The
    // level an operator alerts on is the gauge beside it (gauges are not
    // pre-registered), and this is the event arm a dashboard reads to tell one
    // long-held segment apart from a directory that keeps producing them. Its
    // only label is the tenant, which is known at the increment and cannot be
    // listed here — the unlabelled series exists so the counter is on a fresh
    // compactor's first scrape at 0, as `siglake_compactor_cycles_total` is.
    AlertedCounter {
        name: "siglake_compactor_segments_poisoned_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_compactor_delete_tasks_stalled_total",
        series: &[&[("state", "running")], &[("state", "pending_claimed")]],
    },
    AlertedCounter {
        name: "siglake_compactor_cycles_total",
        series: &[
            &[("outcome", "empty")],
            &[("outcome", "deferred")],
            &[("outcome", "claim_error")],
            &[("outcome", "commit_error")],
            &[("outcome", "ok")],
            &[("outcome", "catalog_sync_error")],
        ],
    },
    AlertedCounter {
        name: "siglake_wal_crc_mismatch_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_wal_partial_tail_dropped_total",
        series: UNLABELLED,
    },
    TABLE_CACHE_FENCED,
];

/// Counters the query server pre-registers. The WAL CRC counter is here as
/// well: the real-time buffer and the hot caches read sealed segments.
pub const QUERY_SERVER_ALERTED_COUNTERS: &[AlertedCounter] = &[
    // Query responses stay non-blocking when the best-effort audit path is
    // saturated or stopped. Pre-register every bounded reason so the first
    // whole-row refusal is visible to the dashboard's increase() reader.
    // `append_deadline` is the one that is charged per row of an abandoned
    // batch rather than per refused submit: the storage append outlived its
    // service deadline and those rows are gone (#3438).
    AlertedCounter {
        name: "siglake_query_audit_dropped_total",
        series: &[
            &[("reason", "oversized")],
            &[("reason", "row_limit")],
            &[("reason", "byte_limit")],
            &[("reason", "channel_full")],
            &[("reason", "worker_shutdown")],
            &[("reason", "append_deadline")],
        ],
    },
    // Recovery refusing a start means no query was executed; recovery rejecting
    // a late completion means computed output was discarded. `write_abandoned`
    // means the row stays non-terminal until this replica exits. Cancellation,
    // TTL expiry, oversize bodies, `write_deferred` (the executor is still
    // retrying) and store errors share the counter but do not page; only these
    // bounded series need a zero baseline for their alert's first increment.
    // `actual` is fixed by the cause — recovery installs `failed`, an abandoned
    // write installs nothing and reports `unknown` — so recovery has the start
    // attempt plus the three verdicts a batch run can compute.
    AlertedCounter {
        name: "siglake_query_job_terminal_conflict_total",
        series: &[
            &[
                ("attempted", "succeeded"),
                ("actual", "failed"),
                ("cause", "recovery"),
            ],
            &[
                ("attempted", "failed"),
                ("actual", "failed"),
                ("cause", "recovery"),
            ],
            &[
                ("attempted", "timeout"),
                ("actual", "failed"),
                ("cause", "recovery"),
            ],
            &[
                ("attempted", "running"),
                ("actual", "failed"),
                ("cause", "recovery"),
            ],
            &[
                ("attempted", "succeeded"),
                ("actual", "unknown"),
                ("cause", "write_abandoned"),
            ],
            &[
                ("attempted", "failed"),
                ("actual", "unknown"),
                ("cause", "write_abandoned"),
            ],
            &[
                ("attempted", "timeout"),
                ("actual", "unknown"),
                ("cause", "write_abandoned"),
            ],
        ],
    },
    // The other half of SiglakeBatchRowStrandedNonTerminal, and the one a
    // fresh pod is most likely to increment exactly once: reconciliation
    // bookkeeping refusing an id it has no room for.
    AlertedCounter {
        name: "siglake_query_jobs_unreconciled_dropped_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_query_exec_pool_abandoned_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_query_scan_attribution_incomplete_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_query_breaker_trips_total",
        series: &[
            &[("breaker", "admission"), ("priority", "interactive")],
            &[("breaker", "admission"), ("priority", "batch")],
            &[("breaker", "timeout"), ("priority", "interactive")],
            &[("breaker", "timeout"), ("priority", "batch")],
            &[("breaker", "shard_timeout"), ("priority", "interactive")],
            &[("breaker", "shard_timeout"), ("priority", "batch")],
            &[("breaker", "preflight_bytes"), ("priority", "interactive")],
            &[("breaker", "preflight_bytes"), ("priority", "batch")],
            &[("breaker", "pool_exhausted"), ("priority", "interactive")],
            &[("breaker", "pool_exhausted"), ("priority", "batch")],
            &[
                ("breaker", "midflight_rows_shard"),
                ("priority", "interactive"),
            ],
            &[("breaker", "midflight_rows_shard"), ("priority", "batch")],
            &[
                ("breaker", "midflight_rows_scanned"),
                ("priority", "interactive"),
            ],
            &[("breaker", "midflight_rows_scanned"), ("priority", "batch")],
            &[
                ("breaker", "midflight_rows_scanned_ndjson"),
                ("priority", "interactive"),
            ],
            &[
                ("breaker", "midflight_rows_scanned_ndjson"),
                ("priority", "batch"),
            ],
            // #2184: the four Jaeger read ceilings, so an operator can see
            // WHICH one refused a trace read (the statuses alone cannot say).
            // Interactive only, and no batch twin: the Jaeger routes have no
            // batch tier to run on. A refusal here is policy, not capacity —
            // reading them as `pool_exhausted` would be wrong.
            &[
                ("breaker", "jaeger_trace_limit"),
                ("priority", "interactive"),
            ],
            &[("breaker", "jaeger_span_rows"), ("priority", "interactive")],
            &[
                ("breaker", "jaeger_render_bytes"),
                ("priority", "interactive"),
            ],
            &[("breaker", "jaeger_name_rows"), ("priority", "interactive")],
        ],
    },
    AlertedCounter {
        name: "siglake_query_shard_pin_total",
        series: &[&[("outcome", "pinned")], &[("outcome", "miss")]],
    },
    TABLE_CACHE_FENCED,
    // #967 peer discovery. The failure outcomes all RETAIN the last known good
    // membership, so a pod whose resolver has been broken since boot looks
    // exactly like a healthy one-member cluster: it answers, locally, forever.
    // The alert on it compares the failing arms against the succeeding ones, so
    // both have to exist at 0 from startup — on a pod that never resolves, the
    // succeeding arms are precisely the series that would otherwise be absent.
    AlertedCounter {
        name: "siglake_query_peer_discovery_refresh_total",
        series: &[
            &[("outcome", "changed")],
            &[("outcome", "unchanged")],
            &[("outcome", "empty")],
            &[("outcome", "unmatched")],
            &[("outcome", "error")],
        ],
    },
    AlertedCounter {
        name: "siglake_query_warm_cycles_abandoned_total",
        series: UNLABELLED,
    },
    // The query analogue of `siglake_ingest_tenant_denied_total`, and worth
    // seeing on the FIRST denial: a deployment that has just turned on
    // `query.oidc.tenantClaim` against an IDP that does not mint it denies
    // every request, and a counter that only appears on the second increment
    // hides that behind an empty graph.
    AlertedCounter {
        name: "siglake_query_tenant_denied_total",
        series: &[
            &[("reason", "claim_missing")],
            &[("reason", "claim_invalid")],
        ],
    },
    AlertedCounter {
        name: "siglake_wal_crc_mismatch_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_wal_partial_tail_dropped_total",
        series: UNLABELLED,
    },
    // A footer checksum refusal is query-correctness protection taking the
    // exact-scan fallback. Both reasons feed the dashboard panel; create them
    // at zero so its first observed refusal is a delta rather than No data.
    AlertedCounter {
        name: "siglake_index_footer_checksum_refused_total",
        series: &[&[("reason", "malformed")], &[("reason", "mismatch")]],
    },
    // #3969's "Text-index startup" panels. Neither counter is alerted on; they
    // are here for the other half of the pre-registration argument — a panel
    // over a series that does not exist yet renders "No data", which reads the
    // same as a healthy pod. A query tier serving no text query yet has to
    // chart a flat zero, because the reading these panels exist for is a
    // CHANGE: run #78's `keyword_and_label` plan held 4.1 GB of parsed index
    // against the 1 GiB default and 91 of 156 partitions started cold, which
    // shows up here as an eviction rate that climbs away from zero.
    //
    // Both are recorded from the Iceberg fork with variable label values
    // (`iceberg::arrow`'s `PARSED_INDEX_CACHE_OUTCOMES`,
    // `TEXT_INDEX_STORAGE_FORMS` and `PARSED_INDEX_CACHE_DROP_REASONS`), so
    // check-chart.py sees dynamic sites and cannot hold this catalog to them;
    // `text_index_startup_series_are_preregistered` in siglake-storage does,
    // against those exported vocabularies.
    AlertedCounter {
        name: "siglake_iceberg_parsed_index_cache_lookups_total",
        series: &[
            &[("outcome", "hit"), ("storage", "puffin")],
            &[("outcome", "miss"), ("storage", "puffin")],
            &[("outcome", "hit"), ("storage", "footer_kv")],
            &[("outcome", "miss"), ("storage", "footer_kv")],
        ],
    },
    AlertedCounter {
        name: "siglake_iceberg_parsed_index_cache_evictions_total",
        series: &[
            &[("reason", "byte_bound")],
            &[("reason", "entry_bound")],
            &[("reason", "oversized")],
        ],
    },
    // #4718's blob-cache half of the same panel, and here for the same reason:
    // a pod whose blob cache is re-fetching every blob per execution charts a
    // rising `fetches` beside a flat `hit`, and both readings need the arms to
    // exist from startup — the one #4182 hid for a round was a zero hit rate,
    // which is indistinguishable from "no text query yet" while the series is
    // absent. The eviction reasons say which rule is running: `redundant` and
    // `stale` are the #4182 rule following the working set, a `fifo` rate is
    // the pre-#4182 fallback, which is what re-fetched a blob one step before
    // the query that wanted it.
    //
    // Recorded from the Iceberg fork with variable label values
    // (`PUFFIN_BLOB_CACHE_OUTCOMES`, `PUFFIN_BLOB_CACHE_DROP_REASONS`), so
    // `puffin_blob_cache_series_are_preregistered` in siglake-storage holds
    // this catalog to them where check-chart.py cannot.
    AlertedCounter {
        name: "siglake_iceberg_puffin_blob_fetches_total",
        series: UNLABELLED,
    },
    AlertedCounter {
        name: "siglake_iceberg_puffin_blob_cache_lookups_total",
        series: &[&[("outcome", "hit")], &[("outcome", "miss")]],
    },
    AlertedCounter {
        name: "siglake_iceberg_puffin_blob_cache_evictions_total",
        series: &[
            &[("reason", "stale")],
            &[("reason", "redundant")],
            &[("reason", "fifo")],
        ],
    },
    // #4846's "Decoded-file cache populations" panel. No alert reads these
    // either; they are here for the same reason as the two above, and with one
    // more edge — the cache is OFF by default, so on most deployments every
    // series stays at 0 and that is the correct reading. The one an operator who
    // turned it on is looking for is `abandoned`: a population that decoded
    // batches and was dropped before its insert. Charting it against `insert`
    // needs both arms to exist, and a query tier serving nothing but clipped
    // scans emits `insert` never.
    //
    // Every outcome the code records is listed; `scripts/check-chart.py` holds
    // this entry to the call sites in `siglake-storage`'s query provider, which
    // all pass a literal.
    AlertedCounter {
        name: "siglake_query_scan_file_cache_requests_total",
        series: &[
            &[("outcome", "hit")],
            &[("outcome", "miss")],
            &[("outcome", "bypass")],
            &[("outcome", "insert")],
            &[("outcome", "insert_skipped_contended")],
            &[("outcome", "skip_oversized")],
            &[("outcome", "abandoned")],
            &[("outcome", "evict")],
        ],
    },
];

/// Counters an `increase()` alert reads that no binary can pre-register,
/// with why. Every series they emit carries a label whose value is only
/// known at the increment, so there is nothing to create at 0 ahead of it;
/// their alerts see the SECOND increment. Listed so `check-chart.py` can tell
/// a deliberate gap from a forgotten one.
pub const UNREGISTERABLE_ALERTED_COUNTERS: &[(&str, &str)] = &[(
    "siglake_storage_schema_drift_total",
    "every series carries the offending `column`, known only when the drift is found; \
     the drain refuses the same segment every cycle, so the counter moves again within \
     one cycle and the alert fires on the second refusal",
)];

/// Create every series in `counters` at 0 on the global recorder, so the
/// first increment is a delta `increase()` can see and the dashboards show 0
/// rather than no data. Call once per process, right after [`init`]. Safe to
/// call again: a series that already exists is incremented by 0.
pub fn preregister(counters: &[AlertedCounter]) {
    for counter in counters {
        for labels in counter.series {
            metrics::counter!(counter.name, *labels).increment(0);
        }
    }
}

/// Initialize the global Prometheus recorder and spawn a metrics HTTP
/// server on `bind`.
///
/// Returns once the server is bound (errors otherwise). The server task
/// runs forever in the background; abort the returned [`tokio::task::JoinHandle`]
/// to stop it.
pub async fn init(bind: SocketAddr) -> Result<tokio::task::JoinHandle<()>> {
    let handle = builder()?.install_recorder().context("install_recorder")?;

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind metrics server {bind}"))?;
    let bound = listener.local_addr()?;
    tracing::info!(addr = %bound, "siglake metrics server listening");

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/", get(root_handler))
        .with_state(handle);

    // Profiling rides this server rather than the public API port because this
    // is the one HTTP surface every role shares, so a single mount covers the
    // ingester, the compactor and the query tier. Under a default build the
    // module does not exist; under a profiling build it still returns an empty
    // router unless `SIGLAKE_PPROF_ENABLED=1`. See `crate::profiling`.
    #[cfg(feature = "profiling")]
    let app = app.merge(crate::profiling::routes());

    Ok(tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "metrics server crashed");
        }
    }))
}

async fn metrics_handler(State(handle): State<PrometheusHandle>) -> String {
    handle.render()
}

async fn root_handler() -> &'static str {
    "siglake metrics — see /metrics"
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Render what `/metrics` would serve after `record` ran against a
    /// recorder built exactly the way `init` builds the global one.
    fn render_with(record: impl FnOnce()) -> String {
        let recorder = builder().expect("builder").build_recorder();
        metrics::with_local_recorder(&recorder, record);
        recorder.handle().render()
    }

    #[test]
    fn seconds_histograms_render_buckets_not_quantiles() {
        let out = render_with(|| {
            metrics::histogram!("siglake_test_request_duration_seconds", "endpoint" => "otlp")
                .record(0.2);
        });
        assert!(
            out.contains("# TYPE siglake_test_request_duration_seconds histogram"),
            "{out}"
        );
        assert!(
            out.contains(
                "siglake_test_request_duration_seconds_bucket{endpoint=\"otlp\",le=\"0.25\"} 1"
            ),
            "{out}"
        );
        assert!(
            out.contains(
                "siglake_test_request_duration_seconds_bucket{endpoint=\"otlp\",le=\"0.1\"} 0"
            ),
            "{out}"
        );
        assert!(
            out.contains(
                "siglake_test_request_duration_seconds_bucket{endpoint=\"otlp\",le=\"+Inf\"} 1"
            ),
            "{out}"
        );
        assert!(
            out.contains("siglake_test_request_duration_seconds_count{endpoint=\"otlp\"} 1"),
            "{out}"
        );
        assert!(!out.contains("quantile="), "{out}");
    }

    #[test]
    fn count_histograms_render_buckets_including_zero() {
        let out = render_with(|| {
            for name in COUNT_HISTOGRAMS {
                metrics::histogram!(*name).record(0.0);
            }
        });
        for name in COUNT_HISTOGRAMS {
            assert!(out.contains(&format!("# TYPE {name} histogram")), "{out}");
            assert!(
                out.contains(&format!("{name}_bucket{{le=\"0\"}} 1")),
                "{out}"
            );
            assert!(
                out.contains(&format!("{name}_bucket{{le=\"+Inf\"}} 1")),
                "{out}"
            );
        }
        assert!(!out.contains("quantile="), "{out}");
    }

    #[test]
    fn mirror_rotation_histograms_use_their_wide_overrides() {
        let out = render_with(|| {
            metrics::histogram!("siglake_compactor_mirror_sync_rotation_duration_seconds")
                .record(4_000.0);
            metrics::histogram!("siglake_compactor_mirror_sync_rotation_objects").record(600_000.0);
            metrics::histogram!("siglake_test_request_duration_seconds").record(4_000.0);
        });
        assert!(
            out.contains(
                "siglake_compactor_mirror_sync_rotation_duration_seconds_bucket{le=\"3600\"} 0"
            ),
            "{out}"
        );
        assert!(
            out.contains(
                "siglake_compactor_mirror_sync_rotation_duration_seconds_bucket{le=\"10800\"} 1"
            ),
            "{out}"
        );
        assert!(
            out.contains("siglake_compactor_mirror_sync_rotation_objects_bucket{le=\"500000\"} 0"),
            "{out}"
        );
        assert!(
            out.contains("siglake_compactor_mirror_sync_rotation_objects_bucket{le=\"1000000\"} 1"),
            "{out}"
        );
        assert!(
            !out.contains("siglake_test_request_duration_seconds_bucket{le=\"10800\"}"),
            "{out}"
        );
    }

    #[test]
    fn unbucketed_histograms_deliberately_render_as_summaries() {
        let summary_names = [
            "siglake_query_scan_partition_decoded_bytes",
            "siglake_query_runtime_leaf_rows_scanned",
            "siglake_catalog_metadata_snapshots",
            "siglake_wal_seal_bytes",
            "siglake_wal_seal_rows",
        ];
        let out = render_with(|| {
            for name in summary_names {
                metrics::histogram!(name).record(10.0);
            }
            // Ends in `_seconds_max`, not `_seconds`: the suffix matcher must
            // not catch it.
            metrics::histogram!("siglake_test_gap_seconds_max").record(0.1);
        });
        for name in summary_names {
            assert!(out.contains(&format!("# TYPE {name} summary")), "{out}");
            assert!(
                out.contains(&format!("{name}{{quantile=\"0.99\"}}")),
                "{out}"
            );
            assert!(!out.contains(&format!("{name}_bucket")), "{out}");
        }
        assert!(
            out.contains("# TYPE siglake_test_gap_seconds_max summary"),
            "{out}"
        );
        assert!(
            !out.contains("siglake_test_gap_seconds_max_bucket"),
            "{out}"
        );
    }

    #[test]
    fn bucket_layouts_are_sorted_and_distinct() {
        for buckets in [
            LATENCY_BUCKETS_SECONDS,
            COUNT_BUCKETS,
            MIRROR_ROTATION_DURATION_BUCKETS_SECONDS,
            MIRROR_ROTATION_OBJECT_BUCKETS,
            POPULATE_ROW_BUCKETS,
        ] {
            assert!(buckets.windows(2).all(|w| w[0] < w[1]), "{buckets:?}");
        }
    }

    /// #4890's reader divides at the shipped row-group floor, and can only do
    /// that if the exposition carries an edge there. `le="131071"` is the
    /// fraction strictly below the floor, so `+Inf - le("131071")` is the
    /// population that was handed at least `MIN_ROW_GROUP_ROWS` rows.
    #[test]
    fn population_row_depth_renders_buckets_at_the_row_group_floor() {
        let out = render_with(|| {
            let depth = |rows: f64, outcome: &'static str| {
                metrics::histogram!(
                    "siglake_query_scan_file_cache_populate_rows",
                    "outcome" => outcome
                )
                .record(rows)
            };
            depth(256.0, "clipped");
            depth(131_072.0, "clipped");
            depth(2_000_000.0, "completed");
        });
        assert!(
            out.contains("# TYPE siglake_query_scan_file_cache_populate_rows histogram"),
            "{out}"
        );
        for line in [
            "siglake_query_scan_file_cache_populate_rows_bucket{outcome=\"clipped\",le=\"131071\"} 1",
            "siglake_query_scan_file_cache_populate_rows_bucket{outcome=\"clipped\",le=\"131072\"} 2",
            "siglake_query_scan_file_cache_populate_rows_bucket{outcome=\"completed\",le=\"131071\"} 0",
            "siglake_query_scan_file_cache_populate_rows_bucket{outcome=\"completed\",le=\"+Inf\"} 1",
        ] {
            assert!(out.contains(line), "missing {line} in {out}");
        }
    }

    /// Every catalog a binary hands to `preregister`, by binary.
    fn catalogs() -> [(&'static str, &'static [AlertedCounter]); 3] {
        [
            ("ingester", INGESTER_ALERTED_COUNTERS),
            ("compactor", COMPACTOR_ALERTED_COUNTERS),
            ("query-server", QUERY_SERVER_ALERTED_COUNTERS),
        ]
    }

    /// The exposition line for one pre-registered series at `value`.
    fn series_line(counter: &AlertedCounter, labels: &[(&str, &str)], value: u64) -> String {
        if labels.is_empty() {
            return format!("{} {value}\n", counter.name);
        }
        let pairs: Vec<String> = labels.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
        format!("{}{{{}}} {value}\n", counter.name, pairs.join(","))
    }

    #[test]
    fn preregistered_counters_render_at_zero_before_any_increment() {
        for (which, catalog) in catalogs() {
            let out = render_with(|| preregister(catalog));
            for counter in catalog {
                assert!(
                    out.contains(&format!("# TYPE {} counter\n", counter.name)),
                    "{which}: {} not typed as a counter in:\n{out}",
                    counter.name
                );
                for labels in counter.series {
                    let line = series_line(counter, labels, 0);
                    assert!(out.contains(&line), "{which}: missing {line:?} in:\n{out}");
                }
            }
        }
    }

    #[test]
    fn first_increment_after_preregistration_is_a_visible_delta() {
        let recorder = builder().expect("builder").build_recorder();
        metrics::with_local_recorder(&recorder, || preregister(COMPACTOR_ALERTED_COUNTERS));
        let cycles = COMPACTOR_ALERTED_COUNTERS
            .iter()
            .find(|c| c.name == "siglake_compactor_cycles_total")
            .expect("compactor cycles are in the compactor catalog");
        let catalog_sync_error = &[("outcome", "catalog_sync_error")][..];
        let before = recorder.handle().render();
        assert!(
            before.contains(&series_line(cycles, catalog_sync_error, 0)),
            "{before}"
        );
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!(cycles.name, catalog_sync_error).increment(1);
        });
        let after = recorder.handle().render();
        assert!(
            after.contains(&series_line(cycles, catalog_sync_error, 1)),
            "{after}"
        );
        // The other outcomes are untouched by the increment and still there.
        assert!(
            after.contains(&series_line(cycles, &[("outcome", "ok")], 0)),
            "{after}"
        );
    }

    /// #1625: the table-metadata cache fences are read through
    /// `SiglakeTableCacheUnpublished`, which weighs a sustained `unpublished`
    /// rate against the healthy arms — so all three actions have to exist at 0
    /// on both binaries that read a table through that cache while commits
    /// invalidate it. The ingester writes the WAL, not the catalog, and never
    /// reaches the fence.
    #[test]
    fn table_cache_fences_are_preregistered_where_the_cache_is_read() {
        for (which, catalog) in catalogs() {
            let listed = catalog.contains(&TABLE_CACHE_FENCED);
            assert_eq!(
                listed,
                which != "ingester",
                "{which}: unexpected TABLE_CACHE_FENCED membership"
            );
        }
        let out = render_with(|| preregister(&[TABLE_CACHE_FENCED]));
        for action in ["superseded", "reload", "unpublished"] {
            let line = series_line(&TABLE_CACHE_FENCED, &[("action", action)], 0);
            assert!(out.contains(&line), "missing {line:?} in:\n{out}");
        }
        assert_eq!(TABLE_CACHE_FENCED.series.len(), 3, "exactly three actions");
    }

    #[test]
    fn preregister_is_idempotent() {
        let out = render_with(|| {
            preregister(INGESTER_ALERTED_COUNTERS);
            preregister(INGESTER_ALERTED_COUNTERS);
        });
        for counter in INGESTER_ALERTED_COUNTERS {
            for labels in counter.series {
                let line = series_line(counter, labels, 0);
                assert_eq!(out.matches(&line).count(), 1, "{line:?} in:\n{out}");
            }
        }
    }

    #[test]
    fn alerted_counter_catalogs_are_well_formed() {
        let unregisterable: Vec<&str> = UNREGISTERABLE_ALERTED_COUNTERS
            .iter()
            .map(|(name, _)| *name)
            .collect();
        for (which, catalog) in catalogs() {
            let mut seen = std::collections::HashSet::new();
            for counter in catalog {
                assert!(
                    counter.name.starts_with("siglake_") && counter.name.ends_with("_total"),
                    "{which}: {} is not a siglake_*_total counter name",
                    counter.name
                );
                assert!(
                    !counter.series.is_empty(),
                    "{which}: {} lists no series; use UNLABELLED",
                    counter.name
                );
                for labels in counter.series {
                    assert!(
                        seen.insert((counter.name, *labels)),
                        "{which}: {} {labels:?} is listed twice",
                        counter.name
                    );
                }
                assert!(
                    !unregisterable.contains(&counter.name),
                    "{which}: {} is both pre-registered and listed as unregisterable",
                    counter.name
                );
            }
        }
        for (name, why) in UNREGISTERABLE_ALERTED_COUNTERS {
            assert!(
                name.starts_with("siglake_") && name.ends_with("_total"),
                "{name}"
            );
            assert!(!why.trim().is_empty(), "{name} has no reason");
        }
    }
}
