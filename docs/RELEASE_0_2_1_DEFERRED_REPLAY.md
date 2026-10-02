# 0.2.1 deferred work replay manifest

This manifest is for the continuing `main` line after the actual 0.2.1 tag.
Do not replay any item below before that tag exists. Task #6062 must verify that
0.3.0 contains every 0.2.1 fix, test and qualification change as well as this
deferred work.

## Deferral commit

`7200fc43798c5a5e8cf90eee11ea9b433c938af5` (`fix(release): defer 0.3.0
features from 0.2.1`) removes the implementation, dependent tests, generated
artifacts and shipped-behaviour text. Revert that commit after the 0.2.1 tag,
then reconcile against changes merged meanwhile. Replaying the original source
merges one at a time is a fallback for conflict diagnosis, not the preferred
procedure: the deferral commit already retains later patch fixes.

## Replay record

The replay started from
`f93a31239664226ea2bb5a17675ff0053a927eef`, where the released v0.2.1 commit
`b9f77f86fe03102784e8ff2575cb21ae4bd4eb1a` and the deferral above are both
ancestors. It applies the inverse of the deferral and retains the later
time-aggregate cost report (`2cbec74fecd8331c746a8e4ba5ddebe202e6a868`),
conditional-write diagnostics
(`cd9e8d3b615cf0ef47451c8138440e3428e32c44`), requested-byte attribution
(`eec03d01a0f1f82ed34d4a8b5d21c477c240a025`), and public image authority
alignment (`6e46b8f9f3d250c1e5dfa0aa6372bca1f3ffa954`). This record covers source
restoration only; it is not field qualification and does not complete #6062.

## Source inventory

| Work | Source merge | Follow-on | Restore with |
| --- | --- | --- | --- |
| Puffin blob-cache resident and budget gauges | `0936984bcb1c46fe4d0aa23aaec7aa0cda5f5ee0` (#5374) | — | cache metrics, storage tests, dashboard and docs |
| Decoded-file cache accounted bytes and refused populations | `cfc5fe4bf04d179884c99f5add845336609d4a79` (#5801) | — | core/storage/query metrics, test, dashboard and docs |
| Query execution and scan correlation | `c6445da8b7c748795653ea1f0aa715835469d421` (#5934) | `402a953244e52c3372c93275cf1108a86e0c5213` (#5965) | query/storage logging and both attribution tests |
| Auto-promotion sampling and backfill cost attribution | `1a25c74f5c913c8e6fc9bb03762443e5819d64e0` (#5997) | — | compactor/storage counters, bounds fixture and qualification text |
| Dropped-index cleanup records and report-only sweep | `96196e8f2c4e42064f90c956a28a7ebdfe9e8e7d` (#6007) | — | storage implementation and fixture; keep the later conditional-write guard |
| Zero-floor compactor wake-up | `ce461f7641a387352adb7b49e017cf4d26febb31` (#6011) | `e76f4e7a7027acb4e262bb6dcd6ebde96c82b2db` (#6033) | CLI, operator, CRDs, kind capture/grader and behaviour docs |
| Mapping-aware newest-first ordering | `4946026f42ac5f95c64bc0dc210ac181b95ed00a` (#6020) | — | query, WAL-buffer and storage order contract plus fixtures |
| Per-path WAL mirror attempts and active bytes | `1e17957f6263365e8b39df045cf747c92960e419` (#6034) | `890c79d0354fdb88cf06edae8cf9738e5dbec691` (#6047) | WAL counters, chart descriptions and performance method |
| Inline-coverage census cost series | `aa4cf3556d16464e419d40b322781542711691b2` (#6104) | — | compactor/storage counters, fixture and docs |

The replay must also restore the corresponding `CHANGELOG.md` entries and the
generated operator CRDs. The design documents stay in the 0.2.1 tree as
future-facing records; the replay changes their status only when the code is
back. Run the generated-artifact gate after the CRD and OpenAPI generators.

## Acceptance tied to deferred instrumentation

The auto-promotion S3 qualification still measures sampling requests and bytes,
pass duration, and committed backfill files, input/output bytes and duration.
Those series exist at instrumentation revision
`1a25c74f5c913c8e6fc9bb03762443e5819d64e0`. The WAL mirror two-path method
uses the counters from `1e17957f6263365e8b39df045cf747c92960e419` plus the
request-boundary correction in `890c79d0354fdb88cf06edae8cf9738e5dbec691`.
Evidence collected at either revision must name it. It cannot certify the
fixes-only 0.2.1 tree, and removing the series does not waive either acceptance.

## Patch fixes that the replay must retain

Do not reverse chart mirror-prefix forwarding (#5880), cache/test isolation,
dependency-image repairs, source provenance, release qualification runners, or
the conditional-write safety work in #6331, #6339 and #6343. In particular,
dropped-index cleanup must return under the shared conditional-write verdict;
do not restore its earlier static-capability-only create path.

## Manager handoff before the 0.2.1 tag

Choose the immutable candidate SHA only after this deferral and any review
corrections are committed. Run the normal gate on that exact SHA, then retain
full strict final-tree qualification for both MinIO and Garage, including the
conditional-write application guard/race, raw S3 agreement, pagination and the
Postgres ownership suites. Run the required release qualification and clean
installation checks against the same SHA. Earlier measurements at
feature-bearing revisions, including the audit pin
`8bd9c312719d9c4be02cffbfdca2a14b5fce15fd`, do not certify this candidate.
No tag or publication is part of #6344.
