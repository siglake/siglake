> Historical pre-rename evidence: product names were normalized during the Siglake history migration. Source IDs, artifact digests, measurements and outcomes refer to the archived original runs, not rebuilt Siglake artifacts.

# Historical duration-run failure investigation

Task #6275 reviewed the hash-verified artifacts from the four September 24
duration attempts. The three empty committed cohorts resulted from MinIO's
minimum-free-drive threshold on their shared Docker backing filesystem. The
evidence does not show ingestion loss, retention deletion or a version-specific
visibility defect.

## Timeline

| Attempt | Failing cohort poll | First relevant MinIO refusal | Runner result |
| --- | --- | --- | --- |
| v0.2.0 24h | `validation-0001955`, 2026-09-25 12:59:42–13:01:42 UTC | 12:59:58, metadata write | Empty at the 120-second deadline |
| v0.2.0 72h | `validation-0001954`, 2026-09-25 12:59:47–13:01:48 UTC | 13:00:03, data-file write | Empty at the 120-second deadline |
| v0.1.0 72h | `validation-0004316`, 2026-09-26 09:56:14–09:58:15 UTC | 09:56:29, aggregate write; data commit failed from 09:57:01 | Empty at the 120-second deadline |

Every refusal was HTTP 507 `XMinioStorageFull`: "Storage backend has reached
its minimum free drive threshold." The two v0.2.0 runs failed six seconds apart
on isolated Compose projects backed by the same host filesystem. Their matching
response and timing bound the common cause to that shared storage service.

The passing v0.1.0 24-hour control also received 507 responses in the first
failure window. Its compactor failed writes at 13:00:41, 13:01:12 and 13:01:44,
then committed at 13:01:47 as teardown of the failed v0.2.0 projects began. The
v0.1.0 72-hour run resumed at 13:01:47 as well. The control continued to 272,000
verified committed events and passed. This recovery across both release versions
rules out a version-specific query-visibility boundary as the common cause.

## What the counters mean

The frozen runner accepted each OTLP batch before polling committed-only
visibility. It incremented `expected` immediately, but persisted `cycles`,
`acknowledged_events` and `last_progress_at` only after the cohort and aggregate
oracles passed. The summaries therefore stop at the preceding verified cohort:
195,500, 195,400 and 431,600 events. Each failing 100-event batch was accepted
after that persisted count and is absent from it by construction.

The query server had its WAL overlay disabled, so zero rows says that the cohort
had not reached Iceberg by the deadline. It does not say that the acknowledged
WAL segment had disappeared. The logs repeatedly show the compactor returning
failed batches to `sealed/` for retry. Cleanup then removed each run-owned volume,
so the artifacts cannot prove a later commit or support a post-failure row query.
They contain no retention deletion or missing-WAL evidence.

The visibility timeout was accurate: object-store writes could not complete for
120 seconds. The runner's deficient assumption was capacity isolation. Unique
Compose projects isolate names and volumes, but all four volumes consumed the
same Docker backing filesystem. The former 20 GiB requirement did not state
that it applied only to smoke or account for concurrent duration runs.

## Current main and release impact

Current main retains the same deliberate durability boundary: the default OTLP
acknowledgement waits for a WAL `fsync`, not an Iceberg commit. A failed
compactor append releases the claimed file to `sealed/`; the next cycle retries
it. The regression `a_persistently_failing_batch_is_not_re_claimed_for_the_whole_cycle`
injects a persistent commit error, proves both segments remain in `sealed/`,
then clears the cause and commits both exactly once.

No compatible product defect was found on current main, so this investigation
changes no storage or query code. It corrects the runner documentation to require
dedicated, aggregate-budgeted backing storage for duration profiles. These
historical cached-dependency attempts remain failures and do not qualify or
disqualify v0.2.1.
