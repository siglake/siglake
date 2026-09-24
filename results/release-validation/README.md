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
smoke results do not qualify the failed public install path. Separate 24h and 72h
diagnostics started for each version from committed harness `df7a83f` on September
24 at approximately 19:47 UTC; completion is pending, not implied by this table.

During harness development, an OTLP nullable partial-success field and Docker's
changing randomly assigned host ports exposed harness bugs. Those attempts remain
in local raw artifacts and were not classified as release defects. The committed
runner handles both and tests endpoint rediscovery after restart.
