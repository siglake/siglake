# Exact `avg_size_by_status` candidate measurement — 2026-10-09

The candidate was measured from base commit
`7a518d38bd8aa2b323af8b6255411e52786de821`. The sandbox refused the Git
index lock, so the candidate remained in the working tree; its implementation
diff SHA-256 was
`47c4609cb6474832b56b28c2e2eecc7333f7bcd3276d66d804f8a94518c4b1f5` before
this report was added. Host: Linux 7.0.0-31-generic x86-64; Rust 1.95.0;
100 process CPU ticks per second.

Both arms used the byte-identical published SQL, one deterministic 250,000-row
local Parquet file, six requests, and `SIGLAKE_QUERY_RESULT_CACHE_CAP=0`. The
first request is cold at the query/file caches; the other five are warm. The
only changed setting was `SIGLAKE_GROUPED_NUMERIC_FAST_PATH=off|on`.

Commands:

```sh
SIGLAKE_QUERY_RESULT_CACHE_CAP=0 SIGLAKE_GROUPED_NUMERIC_FAST_PATH=off \
  cargo test -p siglake-query-server --test query_server \
  indexes::measure_avg_size_by_status_scan_and_footer -- --ignored --exact --nocapture
SIGLAKE_QUERY_RESULT_CACHE_CAP=0 SIGLAKE_GROUPED_NUMERIC_FAST_PATH=on \
  cargo test -p siglake-query-server --test query_server \
  indexes::measure_avg_size_by_status_scan_and_footer -- --ignored --exact --nocapture
```

Raw scan arm:

```json
{"cpu_ticks":24,"peak_rss_kib":349672,"query":"SELECT status, avg(size) AS avg_size, count(*) AS n FROM \"httplogs\" GROUP BY status ORDER BY n DESC LIMIT 20","result_cache":"disabled by command","rows":250000,"rss_before_kib":297208,"runs":6,"served_by":"scan","state":"first request cold; remaining requests warm","wall_ms":[101.657565,2.103296,1.47072,1.361381,1.076193,1.027473]}
```

Raw footer arm:

```json
{"cpu_ticks":2,"peak_rss_kib":315888,"query":"SELECT status, avg(size) AS avg_size, count(*) AS n FROM \"httplogs\" GROUP BY status ORDER BY n DESC LIMIT 20","result_cache":"disabled by command","rows":250000,"rss_before_kib":297424,"runs":6,"served_by":"grouped_numeric_footer","state":"first request cold; remaining requests warm","wall_ms":[19.228203,1.541899,1.058753,0.988204,0.968803,0.963454]}
```

Cold wall time fell 101.66 → 19.23 ms (5.29×). Aggregate CPU fell 24 → 2
ticks, and peak resident growth above the pre-query sample fell 52,464 →
18,464 KiB. Warm medians were 1.36 ms scan and 0.99 ms footer. This is a
bounded local candidate result, not the required 247,249,096-row S3 launch
qualification; the manager should run that unchanged harness arm during launch
week.

## 2026-10-10 AWS candidate: negative result

The combined-main candidate at
`362463cc9e2dc47183960570b8b14139ec78ca08` did not qualify against reference
`7a518d38bd8aa2b323af8b6255411e52786de821`. Reference AVG measured 4,403.14 ms
cold, 505.03 ms p50 and 522.14 ms p95. The candidate measured 1,885.31 ms cold,
2,154.29 ms p50 and 2,406.69 ms p95. All ten measured candidate responses used
`grouped_numeric_footer` with `rows_scanned=0`, but warm p50 regressed 4.27x.
Whole-process resources were 18 s elapsed / 54.6 s CPU / 5.86 GiB peak for the
reference and 33 s / 7.6 s / 1.96 GiB for the candidate. Footer routing did not
qualify the candidate.

The candidate path called `raw_page_load_metadata` for every one of the 173
live files on every request. A local counting storage test now puts 40 ms on
each metadata call. The first request performs one metadata call plus the
Parquet range reads; the second performs neither. The test also requires the
warm elapsed time to be less than one quarter of cold elapsed time. The retained
local run measured 41,484 µs cold and 18 µs warm, with one metadata call and
two range reads after both requests. Run it with:

```sh
cargo test -p siglake-storage \
  iceberg::grouped_numeric_footer_cache_tests::warm_hit_removes_footer_io_and_its_delay \
  -- --exact --nocapture
```

The cache stores a parsed summary or stable miss under
`(full file path, group column, value column)`. It does not store read errors.
Its entry cap, byte budget, eviction and new-path behavior are exercised in the
same test module. This attributes the injected local delay to repeated footer
I/O; it does not qualify S3 latency. An unchanged full HTTP candidate must run
after integration and the strict gate.

A separate 250,000-row HTTP run with
`SIGLAKE_QUERY_RESULT_CACHE_CAP=0` kept `served_by=grouped_numeric_footer` and
measured `[16.304, 1.306, 0.993, 0.941, 0.923, 0.902]` ms. The per-file
metadata cache remains active when the completed-answer cache is disabled; it
does not store a completed aggregate answer.

## Exact-answer differential

For status 304 the retained reference answer was `31.45880053399644`; the
candidate returned `31.458800533996442`. Their binary64 encodings are adjacent.
The candidate's count was 37,137,326 and its exact integer sum was
1,168,295,731, whose correctly rounded binary64 quotient is the candidate
value.

DataFusion 53.1 coerces integer AVG input to binary64. Its `AvgAccumulator`
sums each Arrow batch, sums those partials, then divides the binary64 sum by the
count. Binary64 addition is not associative: `[2^53, 1, -2^53]` produces sum
zero in that order, while `[2^53, -2^53, 1]` produces one. Both layouts have the
same exact integer sum and count, so no function of the footer's exact
`(sum, count)` can reproduce every scan layout's bit pattern. The unit test
retains both this proof and the observed adjacent encodings. No arithmetic
change was made: results are not rounded or status-specific, and qualification
was not weakened.

Retained external evidence:

- `results/20261010-httpavg-execution/candidate/qualification-failure.json`
- `results/20261010-httpavg-execution/candidate/top-hosts-answer-evidence.json`
- `results/20261009-avg-size/candidate/20261010-aws/siglake-core.json`
- `results/20261009-top-hosts/baseline/20261010-aws/siglake-core.json`
