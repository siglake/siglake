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
