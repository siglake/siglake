# Durable backoff for short-aggregate repair

Status: decision for 0.2.0; implementation is siglake task #5576. The 0.1.1
behavior and operator workaround remain unchanged.

## The failure to retain

The short-aggregate pass runs every 15 minutes and may rebuild one table by
default. The rebuild reads every live file for each maintained exact column,
then publishes `siglake-agg-wide.json` in one CAS. The compactor currently puts
one `tokio::time::timeout` around the whole cross-namespace census and repair
pass. A timeout drops the rebuild future before its final CAS, so the aggregate
usually stays byte-for-byte unchanged. No durable object says the scan ran.
`agg_short_scan_due` is an in-process `Instant`, so a restart tries again at
once.

That behavior preserves exact results and wastes reads. It also stops the pass
before it reports tables after the slow one. The 600-second ceiling is
cooperative: `tokio::time::timeout` can observe expiry only when the repair
future yields.

## Local reproduction and cost

The ignored probes live beside the repair integration tests:

```text
cargo test --release -p siglake-storage --test storage \
  agg_short_repair::measure_census_and_repair_cost -- --ignored --nocapture
cargo test --release -p siglake-storage --test storage \
  agg_short_repair::reproduce_watchdog_cancelled_repair_restart \
  -- --ignored --nocapture
```

Measured 2026-09-20 on the development host with a local filesystem warehouse,
eight files, one exact dimension and release code:

| Rows | Census | Durable marker path | Repair |
|---:|---:|---:|---:|
| 40,000 | 14.6 ms | 1.1 ms / 60 bytes | 65.1 ms |
| 400,000 | 145.4 ms | 1.3 ms / 60 bytes | 876.5 ms |

The marker reading uses the existing lost-delta marker test helper. It includes
a catalog load, the incarnation-scoped OpenDAL PUT and its retry policy, so
1.3 ms is a conservative local bound for that path rather than a raw filesystem
write. It says nothing about S3 PUT latency or price. The proposed body will be
larger than 60 bytes, but still one small object and one PUT per attempt.

The restart probe measured a 143.2 ms census, requested a 286.3 ms timeout and
received the timeout after 830.5 ms, when the Tier-2 scan next yielded. The wide
object was unchanged, and a newly opened `IcebergContext` returned `Detected`
immediately. This is the retry-on-every-boot failure. The approximately
nine-minute value previously cited for 250M rows is a linear extrapolation from
the 40k/400k fixtures, not a measured large-table timeout.

## Existing durable state

`WideGroupCounts.short_repair` is the successful rebuild's suppression record.
It belongs in the same CAS as the counts because its `unrestored` columns are a
result of that rebuild. Writing an `attempted` value into the wide object before
the scan would require a second CAS, rewrite a potentially large base object,
and mix an incomplete attempt with the aggregate's successful state. A timeout
cannot add it after the scan because the future has already been dropped.

The lost-delta record is a closer fit. A `<sequence>.rebuild.json` object is a
small retried PUT outside the aggregate CAS. It is discovered by the fold's
existing LIST of `siglake-agg-deltas/`, under
`metadata/siglake-agg/<table-uuid>/`, and is deleted after `rebuilt_through`
covers it. It survives process loss and cannot cross a table recreation. The
backoff record should reuse those storage and retry semantics, while using a
distinct filename and body so a repair attempt cannot be consumed as a lost
delta.

Catalog leases are not the record. The `agg_fold` maintenance lease defaults to
300 seconds, shorter than the 600-second watchdog, and filesystem mode has no
lease. Another compactor may therefore start while the first still scans.

## Decision

Write a unique pre-attempt object in the incarnation's existing delta directory:

```text
siglake-agg-deltas/<sequence>.short-repair.<attempt-uuid>.json
```

Version 1 records the attempt UUID, table UUID, target snapshot id and sequence,
maintained columns, UTC start time, fixed reason (`started`, `watchdog`,
`failed`) and the next eligible UTC time. The table UUID in the body is checked
against the prefix. The snapshot fields explain what was scanned; retry history
is incarnation-scoped and therefore continues across later snapshots. A normal
append must not erase evidence that this table repeatedly exceeds the repair
budget.

Use the lost-delta writer's bounded retries. If the pre-write fails, do not
start the Tier-2 scan: log and count `marker_failed`, leave the aggregate short,
and let the next scheduled pass try the marker again. A watchdog or returned
repair error overwrites that attempt's unique object with the fixed reason and
next retry time. A fresh `started` marker means another compactor may still be
running and blocks a duplicate attempt. Once its watchdog interval has elapsed,
it is read as `interrupted`, rather than permission to scan immediately. No raw
error string is part of the format.

The default retry schedule is an initial attempt, then delays of 15 minutes,
one hour and four hours. After the fourth unsuccessful attempt in one table
incarnation, automatic repair is suppressed until a successful operator rebuild
or table recreation clears it. A future UTC deadline is honored conservatively;
an invalid timestamp suppresses automatic work and names the malformed marker
for the operator instead of creating a retry loop. This bounds one persistently
trippable incarnation at four automatic scans.

The fold already lists the directory for each maintained table. Extend that
listing to return short-repair attempt summaries to the immediately following
census; do not add a second healthy-path LIST. Run the metadata census for all
tables before spending the repair budget, then apply the watchdog to each
selected repair rather than the cross-namespace loop. This preserves outcomes
for later tables. `max_repairs` continues to bound the number of Tier-2 scans in
one pass.

## Concurrency, CAS and cleanup

Attempt UUIDs prevent two compactors that lost the lease from overwriting each
other. A compactor that sees a fresh `started` marker backs off. If two writers
race before either marker is visible, both may scan; the aggregate CAS remains
the correctness fence. One can publish, and the other must reload and report a
conflict or discover that the deficit is gone. Attempt state is never merged
into the aggregate.

`Repaired` remains possible only after `store_wide_group_counts` confirms the
final CAS. Cancellation records `watchdog`, never success. A remote CAS request
may land just as its future is cancelled; on the next pass, coverage and
`rebuilt_through` take precedence, prove the repair landed, and permit marker
cleanup. Thus a transient watchdog report may coexist with a repaired object,
but no marker certifies counts that were not published.

After a successful automatic or CLI rebuild, delete every attempt marker whose
target sequence is at or below the new `rebuilt_through`. Deletion happens only
after the CAS and is retryable. A marker written concurrently after the cleanup
listing is removed by the next census when the same watermark proves it stale.
Dropping and recreating a table changes its UUID and starts with an empty
history. The old prefix is unreachable from the new incarnation and retains its
existing object-lifecycle behavior.

`siglake rebuild-group-counts` clears the applicable attempt history only after
its own successful CAS and reports how many records it cleared. A failed manual
rebuild leaves the suppression in place.

## Operator reading

Add bounded `ShortAggregateOutcome` cases for `BackedOff`, `Suppressed` and
`MarkerFailed`. The WARN line includes namespace, table, attempt count, fixed
last reason, next eligible time and the `rebuild-group-counts` command. Extend
`siglake_group_count_short_aggregates_total{outcome}` with fixed values
`backed_off_watchdog`, `backed_off_failed`, `backed_off_interrupted`,
`suppressed` and `marker_failed`; update `SiglakeGroupCountAggregateShort` to
include them. These labels let an operator identify the reason without listing
warehouse objects and avoid unbounded error-text labels.

## Rejected alternatives

- Pre-writing `short_repair` into `siglake-agg-wide.json` adds a large-object
  CAS before every scan and gives an incomplete attempt the same authority as a
  rebuild result.
- A new catalog table or lease protocol duplicates incarnation and cleanup
  rules already present in the aggregate prefix. The maintenance lease is also
  too short to retain the outcome.
- Process memory cannot survive restart and is the current defect.
- A permanent skip after the first timeout minimizes reads but gives transient
  object-store and CPU stalls no automatic recovery. Four attempts accept a
  bounded amount of repeated work before requiring operator action.
