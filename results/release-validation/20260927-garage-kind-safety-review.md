> Historical pre-rename evidence: product names were normalized during the Siglake history migration. Source IDs, artifact digests, measurements and outcomes refer to the archived original runs, not rebuilt Siglake artifacts.

# Garage kind storage-safety review, September 27, 2026

This review covers source commit `e55a4003af0bd763b3baa31f572f4c2d84178df6`.
It does not qualify Garage, add kind deployment files, or launch a cluster.
The matched MinIO and Garage runs remain the live evidence: Garage returned
HTTP 200 for both an `If-None-Match: *` overwrite and a stale `If-Match`
overwrite, while MinIO returned 412 for both
([matched evidence](20260927-minio-garage-s3-evidence.md)). The Garage kind
stop therefore remains in force.

## Verdict

A normal `scripts/kind-round.sh` run would exercise writes which ask Garage to
honour conditional headers, but its topology cannot detect Garage ignoring
them. The ordinary round has one compactor
([kind values](../../deploy/kind/values.kind.yaml#L37)); that Deployment uses
`Recreate` updates
([chart template](../../deploy/helm/siglake/templates/deployment-compactor.yaml#L51)).
The round's only two-compactor arm runs after ordinary evidence and raises the
commit threshold so neither compactor commits the queued rows
([round script](../../scripts/kind-round.sh#L116),
[capture](../../scripts/kind-round.sh#L1787)). A green MinIO/Garage pair would
therefore say nothing about the stale-ETag race which the Garage endpoint
silently admits.

The proposed round also does not submit or execute a delete task and does not
delete a managed index. Its `delete_tasks` reference is a zero-valued watchdog
assertion, rather than a workload step
([round script](../../scripts/kind-round.sh#L554)). Thus it cannot test either
the repaired delete-claim path or dropped-index cleanup-record creation.

## Conditional-write reachability

| Path | Reached by the ordinary kind workload? | Result with the measured Garage behavior |
| --- | --- | --- |
| Inline side-aggregate publication | Yes. The compactor appends every committed batch through the storage append APIs ([compactor](../../crates/siglake-compactor/src/lib.rs#L5505)); every successful append publishes its aggregate delta ([storage](../../crates/siglake-storage/src/iceberg.rs#L16250)). The OpenDAL store selects `If-Match` for an existing object and maps any accepted response to `CasWrite::Written` ([storage](../../crates/siglake-storage/src/iceberg.rs#L7906)). | Sequential writes appear successful. If another writer replaces the object between load and write, Garage's accepted stale ETag makes the loser report success and silently drops the winner's fold. The round has no competing writer to create this race. |
| Wide group-count fold | Yes in the intended default round. Its 6,000 distinct hosts exceed the 4,096 inline ceiling, and the round requires the fold metric to exist ([kind README](../../deploy/kind/README.md#L73)). The maintenance fold calls `store_wide_group_counts`, which uses the same ETag/create-only split and treats an accepted response as success ([storage](../../crates/siglake-storage/src/iceberg.rs#L8980)). | One maintenance compactor normally writes the base, so the round does not demonstrate exclusion. A concurrent fold or rebuild can lose an update while both callers report success. |
| Snapshot-expiry coverage re-root | Not a deterministic round step. It runs only when expiry must move an aggregate coverage edge. Its publication uses the same `OpendalSideCas::store_if` result ([storage](../../crates/siglake-storage/src/iceberg.rs#L16618)). | An accepted stale ETag can overwrite a newer edge and claim the wrong coverage generation. Whether a particular round happened to re-root an edge would not test the race without contention. |
| Inline time-aggregate rebuild | No. The round does not run `rebuild-time-aggregates`. The command fences its replacement through `store_if` ([storage](../../crates/siglake-storage/src/iceberg.rs#L23876)). | A concurrent append can be overwritten after Garage accepts the stale ETag, while the rebuild reports publication. |
| Delete-task ownership claim | No. Before a real claim, the shared per-context probe overwrites one fixed key with `if_not_exists` and refuses an endpoint which accepts it ([storage](../../crates/siglake-storage/src/iceberg.rs#L547)). The probe result is shared by tenant contexts ([storage](../../crates/siglake-storage/src/iceberg.rs#L12373)). | This path now fails closed on the measured Garage behavior. It is the only conditional-write path with a runtime ignored-header probe. |
| Dropped-index cleanup-record creation | No. The round makes no index deletion request. Deletion records a route before dropping the catalog table ([index manager](../../crates/siglake-storage/src/index_manager.rs#L621)); creation trusts the static capability, sends `if_not_exists`, and reads back the bytes ([storage](../../crates/siglake-storage/src/iceberg.rs#L18614)). | The production caller generates a fresh UUIDv7 key ([storage](../../crates/siglake-storage/src/iceberg.rs#L17228)), so ignored create-only semantics do not alter the normal collision-free call and the exact read-back catches a different body. They remain part of the safety contract: a key collision or replay could overwrite an existing cleanup authority record instead of refusing it. There is no runtime probe on this path. |

The `CONDITIONAL_UNSUPPORTED` latch does not protect any measured Garage case.
It is set only when OpenDAL returns `Unsupported`; an HTTP success becomes
`CasWrite::Written`
([storage](../../crates/siglake-storage/src/iceberg.rs#L7936),
[merge loop](../../crates/siglake-storage/src/iceberg.rs#L7979)). The wide-object
writer also falls back only on `Unsupported`
([storage](../../crates/siglake-storage/src/iceberg.rs#L8997)).

A repository-wide search found no other object-store conditional write outside
`iceberg.rs`. The other `if_not_exists` use is an Iceberg schema-builder option,
and query-server `If-Match` is Siglake's managed-index HTTP API. WAL mirroring
uses plain PUTs. Those paths do not change this verdict.

## Why the current round cannot lift the stop

The transparent SQL and distributed merge checks are valid query acceptance:
the script calls `/api/v1/sql`, sums every per-host group count against the full
row count, and requires distributed fan-out
([round script](../../scripts/kind-round.sh#L2787)). They can prove that the
rows visible during the run agree. They cannot prove that two conditional
writes exclude one another, because the workload supplies no competing
side-object writers.

The opt-in two-compactor capture does not fill that gap. It deliberately holds
new rows below an unreachable commit threshold and measures a shared queue
gauge. Reusing it as storage evidence would misclassify a no-write interval as
a conditional-write test.

## Missing safety prerequisites

Garage kind deployment preparation remains blocked until all applicable items
below are met, or Todd explicitly narrows the supported Garage scope:

1. **Stale-ETag handling:** side-object publication must reject an endpoint
   which accepts a stale `If-Match`, or use another cross-process fence. The
   protection must cover inline publication, wide folds, coverage re-rooting
   and both rebuild commands. A static OpenDAL capability bit is insufficient.
2. **Create-only handling:** every correctness-sensitive `if_not_exists` use
   must fail closed on ignored headers. The delete-task claim now does. The
   dropped-index cleanup record still needs either the same verified store
   property, a protocol which does not depend on create-only semantics, or an
   explicit exclusion of managed-index deletion on Garage.
3. **Retained store evidence:** after a code or Garage change, repeat the
   matched signed-S3 probe. It must reject stale `If-Match` and existing-key
   `If-None-Match`; a concurrent-writer fixture must also show one winner for
   the side-object compare-and-swap contract. The existing sequential MinIO
   result does not establish atomic exclusion under concurrency.
4. **Scope decision if operations are excluded:** a single `Recreate`
   compactor avoids the ordinary round's multi-writer race, but it does not make
   Garage a safe general store. It gives up multi-compactor drain, concurrent
   maintenance or repair commands, managed-index deletion, and executable
   delete tasks unless each operation has a separate safe protocol. That is a
   product-support boundary and cannot be inferred from a green smoke test.
5. **Only then run deployment acceptance:** add the Garage manifest and values
   overlay, carry the store selector through both kind deployment stages with
   MinIO as default, and retain both store arms. Each arm must include
   transparent `/api/v1/sql` plus a cross-shard `GROUP BY` whose per-key counts
   sum to the full row count.

`docs/LIMITATIONS.md` describes stores with no conditional write and the new
delete-claim compatibility probe. It does not describe the measured case where
an endpoint advertises conditional writes and silently accepts both overwrite
forms. Whichever prerequisite closes the stop should update that user-visible
boundary.
