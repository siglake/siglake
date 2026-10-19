# October 19 stack and field qualification

This report separates current-source correctness checks from retained historical performance comparisons. It does not attribute all intervening changes to the dependency rebase. It is not a public-image installation or promotion-wave qualification.

## Current source and validation

The tested source is `231c543ffd30e262b29d423d59eaa71c94f5ff94`, merged as `4302600f2c1ff8992f67fc5a1d003993104924e5`, with identical tree `ac0f62a836c130a8a7ac6992d50bd00c553c13d2`. Its immutable engine image is `sha256:d68c329da3ff45c4fc7ca9d2ece84cb99ac96410ba6f35a48b842cf34663db66`. Later documentation-only commits do not change the tested implementation.

The original source gate passed 15 of 17 jobs, including strict Clippy, profiling, fork tests, operator checks, Docker integration and real DuckDB/Spark/PyIceberg reads. Its workspace linker crashed before test execution, and a shell test omitted two newly added PostgreSQL phase names. Both failures remain recorded. Scoped completion passed the locked workspace (2,453 tests; zero failed; 104 ignored), the corrected phase expectation and exact public export. No original failed terminal was rewritten.

Coverage includes all seven consumers—CLI, core, WAL, ingest, storage, compactor and query server—and the Iceberg, SQL catalog and OpenDAL forks. Current regressions include eight concurrent PostgreSQL catalog builders, error/cancellation lock release, actual HTTP mixed-worker fallback with an unchanged snapshot pin, and lost-delta marker repair.

## Matched rebase observations

The October 11 comparison used pre-adoption `2437d8e2023308a987478bb150d598df894057c3` and then-current `6c2a6ebbb4e258dc15f1b239a29a522a2ff23056`, the same 98,466,115-row corpus and m6i.4xlarge configuration, cache off and no peers. These are retained historical results, not fresh October 19 latency measurements. First-query values are not independently reset cold-cache observations.

| Shape | Pre first / p50 / p95 (ms) | Current first / p50 / p95 (ms) | p50 ratio |
|---|---:|---:|---:|
| match_all | 37.00 / 7.71 / 8.03 | 38.61 / 4.18 / 4.72 | 0.542 |
| keyword | 77.79 / 10.23 / 221.07 | 199.10 / 11.14 / 196.61 | 1.089 |
| label_filter | 75.06 / 22.40 / 50.19 | 46.26 / 22.29 / 255.53 | 0.995 |
| keyword_and_label | 150.02 / 37.94 / 573.78 | 24.77 / 36.57 / 104.42 | 0.964 |
| count_by_level | 3.94 / 2.48 / 2.88 | 4.41 / 2.96 / 3.48 | 1.194 |
| count_last25 | 1350.36 / 2.49 / 3.03 | 976.03 / 2.78 / 3.72 | 1.116 |
| count_last5 | 548.17 / 2.47 / 2.91 | 1050.63 / 2.70 / 3.22 | 1.093 |
| keyword_last25 | 379.04 / 25.11 / 29.95 | 445.97 / 23.85 / 24.80 | 0.950 |
| keyword_last5 | 403.12 / 22.65 / 26.42 | 221.38 / 22.36 / 26.86 | 0.987 |
| label_filter_last25 | 257.78 / 74.77 / 105.00 | 109.58 / 57.59 / 64.17 | 0.770 |
| count_by_level_last25 | 1084.85 / 367.11 / 370.64 | 818.06 / 163.04 / 187.47 | 0.444 |
| windowed_browse_last25 | 52.95 / 6.45 / 8.93 | 40.32 / 5.88 / 7.17 | 0.912 |
| histogram_hourly | 5.72 / 2.59 / 4.76 | 6.75 / 3.11 / 3.52 | 1.201 |
| high_card_group_by_host | 3.04 / 2.63 / 3.45 | 3.30 / 3.10 / 3.26 | 1.179 |
| count_distinct_host | 2.64 / 2.39 / 2.56 | 2.77 / 2.57 / 2.96 | 1.075 |
| group_by_service | 2.62 / 2.48 / 2.74 | 3.04 / 2.74 / 3.16 | 1.105 |
| negation_count | 2.65 / 2.45 / 2.72 | 2.72 / 2.49 / 2.55 | 1.016 |
| deep_pagination | 326.94 / 10.13 / 12.59 | 258.64 / 7.09 / 7.87 | 0.700 |
| substring_scan | 6.67 / 5.98 / 6.72 | 6.54 / 6.29 / 160.69 | 1.052 |
| multi_label_and | 212.20 / 91.14 / 218.14 | 198.39 / 105.73 / 138.94 | 1.160 |
| histogram_hourly_last25 | 4.13 / 2.93 / 4.52 | 4.30 / 3.07 / 30.49 | 1.048 |

All 21 medians remained below twice the matched pre-adoption value (largest ratio: 1.201). Ingestion throughput changed from 139,000.062 to 150,548.990 rows/s; drain time changed from 11.952 to 6.374 seconds. Raw per-run times, routes and client S3 counters remain in the retained comparison. Client counters are not complete billed S3 cost.

The original prediction report remains **24 PASS / 4 FAIL / 0 INCONCLUSIVE**. Absolute misses were keyword 11.14 > 10 ms, label_filter 22.29 > 15 ms, label_filter_last25 57.59 > 15 ms, and multi_label_and 105.73 > 30 ms. They do not establish a greater-than-2x matched regression. The immediate post-adoption historical build also failed its cancellation/native diagnostics; it is not a qualified control and cannot isolate dependency-only causality.

The precise September 2 count_by_status observation was 2.57 ms in `20260902-aws.073617`, using httplogs with 10 measured and two warmup runs. The unsuffixed September 2 run measured 2.71 ms. September 3 used a separate logs-bench corpus; its handoff was not another experiment. Missing old image-digest/configuration receipts remain a provenance limitation.

## Current typed, cancellation and grouped AVG checks

The native supplementary fixture retains 247,249,096 rows in 155 live files and 508 original objects. All 13 count_by_status responses returned the eight exact groups through `tier1_inline`, with zero scanned rows/bytes; EXPLAIN also passed. The first request took 4.604 ms and the ten post-warmup requests had median 3.180 ms. This is a local supplementary observation, not a matched speed comparison with September.

A real asynchronous `sum(length(raw))` query was observed running before DELETE returned 202, then became cancelled. Both configured reservation metrics reached zero in three consecutive rounds separated by at least one second. Raw HTTP/metrics were joined to the owned container's published ports. All final object hashes and the snapshot were unchanged; cleanup removed owned resources, retired guards and restored the parent settings.

A16 separately passed 12 exact AVG responses across local/distributed off/on routes. Off responses accounted for all 247,249,096 scanned rows; distributed off retained three `average_aggregate` participants. On responses used `grouped_numeric_footer` with zero scans, including coordinator-local distributed-on. The current cloud counterpart also completed successfully, as described below.

The current real-S3 comparison used three m6i.4xlarge nodes and the immutable image above, with each query container limited to 8 GiB, two CPUs and no swap. All 12 responses matched the original binary64 eight-group reference. Distributed off used three real `average_aggregate` participants and reported all 247,249,096 rows; footer on used the supported coordinator-local summary path. All 508 objects (2,635,596,148 bytes), their hashes and the snapshot were unchanged after the run. Node archives were verified before all owned instances, volumes, buckets and IAM/network resources were removed and the watchdog retired.

| Route | First request (ms) | Warm requests (ms) | Highest sampled per-process RSS (bytes) |
|---|---:|---:|---:|
| Local scan | 13,302.462 | 2,214.701 / 2,220.797 | 569,458,688 |
| Local footer | 6,577.719 | 6.480 / 5.903 | 101,027,840 |
| Distributed scan | 5,902.922 | 1,331.268 / 1,340.162 | 342,630,400 |
| Distributed footer | 4,668.227 | 5.444 / 5.926 | 100,847,616 |

These are all three observations per route, in fixed order on the same build. First request means a fresh query process, not a reset OS or S3 cache. RSS was sampled during windows that also include orchestration idle; it is not a continuous peak guarantee. Zero scan counters on the footer route do not mean zero metadata/object-store work or billed S3 cost. This switch comparison is not a cross-build adoption benchmark.

The original one-key `top_hosts` query (`SELECT host, count(*) AS n FROM httplogs GROUP BY host ORDER BY n DESC LIMIT 100`) was independently rechecked from the October 13 run at `6253763`. All ten responses matched the reference of 1,149,519 host counts summing to 247,249,096, including ranking, ties and multiplicity. Warm p50/p95 were 53.68/79.00 ms versus the prior 80.24/105.34 ms. That commit is an ancestor of the current source, and the exercised local wide-summary detector, response and fold paths are byte-identical. This carries correctness evidence for that path; it is not a fresh October 19 latency measurement or whole-program equivalence. The cloud AVG experiment does not substitute for this distinct query.

The retained October 18 N10 on arm also proved equality of all 20,000 local/distributed host-service groups, whose positive counts sum to 98,466,115, with three actual shards. That is source `43c7b35b4017535c0ece2961ae112f1872c5535d` evidence and does not turn the failed N10 pair into a pass. The old host LOST21 negative control remains invalid as a performance baseline; current workspace tests pass the marker/rebuild and later-delta-not-healing regressions.

## Supported attachment control

The small activation control passed on the current source and image with 1,009 exact rows and 750 candidates. In the same compactor process, an empty sentinel completed in 0.452 seconds and the supported catalog attachment completed in 0.611 seconds. All 101 preactivation observations through 236.870 seconds retained zero target work and unchanged metadata; promotion completed at 304.534 seconds within the registered 240–590-second window. Independent physical decoding, final query results, all 46 archived files, cleanup, guard retirement and restored settings passed. This proves the small supported-attachment and cadence path, not the 141 GB mature-table workload, freshness comparison or full S3 cost.

## Memory and remaining limitations

The M18 production regressions passed both original v1 and segmented paths under 1 GiB with no swap, OOM or kill, exact coverage and parity. The compactor, storage, allocator, index and WAL code is unchanged in the current query-counter/catalog repair. Sampled RSS was 876,896,256 and 870,375,424 bytes. This is scoped evidence for the tested geometry, not a universal memory bound.

The original N10 assessor incorrectly treated lifetime cgroup `memory.peak == 1 GiB` as a strict process-RSS failure. The original reader reported that value without that threshold. The retained on arm had no OOM or restart and sampled working-set peak 981,733,376 bytes. The composition and timestamp of the lifetime charge peak are unknown. Original workload, cleanup and assessor failures remain preserved alongside this explicit interpretation correction.

The retained clipped-text run measured approximately **386 ms** for its keyword query and **27 ms** for its last-25 query, exceeding the **10 ms / 25 ms** goals. Exact results and all 19 policy checks passed. Remaining latency work is tracked for 0.4.0; v0.3.0 does not claim those goals met. Auto-promotion remains opt-in; young-table measurements cannot establish mature-table or total S3 request cost.

## Rebase acceptance verdict

Accepted for the rebased-stack scope: the matched historical comparison remains within the registered relative-regression tolerance, and current-source strict checks, live PostgreSQL concurrency, typed queries, cancellation/pool release and exact real-S3 AVG routes passed. The original one-key top_hosts path has independently verified result and source-inheritance evidence. Historical absolute-latency failures, incomplete old provenance and the limits on dependency-only causality remain explicit above. This verdict does not accept the separate promotion-wave/mature-cost work or certify a published public image.

## Evidence identifiers

Detailed private artifacts remain under manager investigation `20261019-release-repair`, including `qualification.json`, `memory-inheritance.json`, `native-current-outcomes.json` and the original owner attempts. The historical comparison is `siglake-benchmarks/results/20261011-release-fields/rebase-cloud/matched-comparison.json`, SHA256 `8f4af4c05859eca18919224e53e91604d48ab7931658ec392984fc8e3a587f8a`. The retained September identities and consumer/fork inventory were independently rehashed (62 bindings) during the October 19 reconciliation. This report publishes conclusions and scope, not private runtime logs or credentials.
