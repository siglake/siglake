> Historical pre-rename evidence: product names were normalized during the Siglake history migration. Source IDs, artifact digests, measurements and outcomes refer to the archived original runs, not rebuilt Siglake artifacts.

# Release-validation evidence

This directory holds reviewed small summaries from the customer-runnable
[`scripts/release-validation`](../../scripts/release-validation/README.md) harness.
Raw metrics and logs live in the run's durable artifact directory; they are not
benchmark leaderboard inputs. Preserve failed attempts and subsequent reruns.
The public operations docs are the release-wide coverage ledger.

## September 24, 2026

| Version | Public dependency pull | Cached-dependency baseline smoke | Cleanup |
| --- | --- | --- | --- |
| v0.1.0 | Failed (MinIO HTTP 401) | Passed, 400 exact committed events | Passed, no owned resources left |
| v0.2.0 | Failed (MinIO HTTP 401) | Passed, 400 exact committed events | Passed, no owned resources left |

Both engine images were pulled anonymously by immutable digest. Cached dependency
results do not qualify the failed public install path or resolve its anonymous
MinIO pull failures.

The four duration attempts used committed harness `df7a83f` and the
`cached-diagnostic` dependency policy. Their requested profile is an attempt
label, not the elapsed duration of a failed run.

| Version | Requested profile | Started (UTC) | Finished (UTC) | Outcome | Cleanup | Original artifact directory |
| --- | --- | --- | --- | --- | --- | --- |
| v0.1.0 | 24h | 2026-09-24 19:47:37.842463 | 2026-09-25 19:49:08.254542 | Passed; workload elapsed 86,434.198801844 seconds | Passed; no owned resources remained | [`v0.1.0-24h-20260924T194737Z-f33ffd9e`](v0.1.0-24h-20260924T194737Z-f33ffd9e/) ([summary](v0.1.0-24h-20260924T194737Z-f33ffd9e/summary.json), [manifest](v0.1.0-24h-20260924T194737Z-f33ffd9e/sha256.json)) |
| v0.1.0 | 72h | 2026-09-24 19:47:37.900289 | 2026-09-26 09:59:15.615274 | Failed: `AssertionError: committed cohort differs from 100 expected IDs: got 0` | Passed; no owned resources remained | [`v0.1.0-72h-20260924T194737Z-4ca40adf`](v0.1.0-72h-20260924T194737Z-4ca40adf/) ([summary](v0.1.0-72h-20260924T194737Z-4ca40adf/summary.json), [manifest](v0.1.0-72h-20260924T194737Z-4ca40adf/sha256.json)) |
| v0.2.0 | 24h | 2026-09-24 19:46:40.254995 | 2026-09-25 13:02:23.539822 | Failed: `AssertionError: committed cohort differs from 100 expected IDs: got 0` | Passed; no owned resources remained | [`v0.2.0-24h-20260924T194640Z-85453c6f`](v0.2.0-24h-20260924T194640Z-85453c6f/) ([summary](v0.2.0-24h-20260924T194640Z-85453c6f/summary.json), [manifest](v0.2.0-24h-20260924T194640Z-85453c6f/sha256.json)) |
| v0.2.0 | 72h | 2026-09-24 19:46:40.328836 | 2026-09-25 13:02:32.604195 | Failed: `AssertionError: committed cohort differs from 100 expected IDs: got 0` | Passed; no owned resources remained | [`v0.2.0-72h-20260924T194640Z-a38924a8`](v0.2.0-72h-20260924T194640Z-a38924a8/) ([summary](v0.2.0-72h-20260924T194640Z-a38924a8/summary.json), [manifest](v0.2.0-72h-20260924T194640Z-a38924a8/sha256.json)) |

Each committed manifest also covers the raw metrics, logs, event records,
rendered Compose configuration and harness files retained outside Git in its
original artifact directory. These cached-diagnostic attempts are historical
evidence, not full release qualification or a new v0.2.1 release gate.

The three empty-cohort failures were storage-capacity failures, not evidence of
data loss. The retained logs show MinIO rejecting writes with HTTP 507
`XMinioStorageFull` during each cohort's 120-second committed-only poll and
through its deadline.
The two v0.2.0 attempts reached the shared-host threshold together; both
v0.1.0 attempts saw the same refusal, then resumed after the failed v0.2.0
projects began cleanup. The v0.1.0 24-hour control continued to completion. The
v0.1.0 72-hour attempt reached the threshold again the next day and timed out.
The [failure investigation](20260926-duration-failure-investigation.md) records
the timestamps, runner counter semantics and current-main check.

During harness development, an OTLP nullable partial-success field and Docker's
changing randomly assigned host ports exposed harness bugs. Those attempts remain
in local raw artifacts and were not classified as release defects. The committed
runner handles both and tests endpoint rediscovery after restart.
