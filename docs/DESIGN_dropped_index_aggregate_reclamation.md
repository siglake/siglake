# Reclaiming aggregates after an index is dropped (task #3001)

**Status:** report-only record and sweeper implemented; automatic deletion is
not enabled. **Date:** 2026-09-23.

Dropping a managed index removes its catalog entry and keeps its committed
storage. Recreating the index id uses the same table location with a new Iceberg
table UUID. Aggregate artifacts are already separated by that immutable UUID:

```text
<table-location>/metadata/siglake-agg/<table-uuid>/
```

The separation prevents the replacement from reading the dropped incarnation's
inline aggregate, folded wide base, deltas, or rebuild markers. It also leaves
the dropped prefix behind. This design adds that exact prefix to the future
dropped-index cleanup record without changing the current preservation
behaviour.

## What a safe reclaimer has to satisfy

1. **Capture identity before the catalog drop.** The record must contain the
   dropped table's UUID and exact location from the table handle being dropped.
   A later lookup by namespace or index id is forbidden because that name may
   resolve to a replacement.
2. **Keep targets incarnation-specific.** The aggregate target is the exact
   descendant `metadata/siglake-agg/<dropped-uuid>/`. A request that is not a
   canonical UUID path below the recorded table location is refused.
3. **Fence delayed publication.** A writer may commit against the dropped table,
   pause before its object PUT, and publish after the first cleanup pass. One
   empty listing is therefore not proof that the prefix will stay empty.
4. **Make every retry idempotent.** Listing the prefix, deleting an object that
   is already absent, or resuming after a partial page must be safe. The cleanup
   record is removed only under a separately chosen retention rule.
5. **Keep replacement storage outside the delete set.** A replacement's UUID is
   different even though its table location is the same. The reclaimer must
   neither list nor delete `metadata/siglake-agg/<replacement-uuid>/`.
6. **Leave unattributable legacy objects alone.** The pre-#2919 flat paths carry
   no table identity. A same-name table does not prove who wrote them, so this
   cleanup record does not adopt or delete them.
7. **Separate artifact classes.** Committed Iceberg files, quarantined WAL
   segments, and aggregates have different loss consequences. Authorising the
   aggregate target must not implicitly authorise the data-file inventory or
   `stale/<dropped-uuid>/`, where a segment may be the only copy of acknowledged
   rows.

## Cleanup record

The delete operation must write and confirm a durable record before it drops the
catalog entry. If that write fails, the delete fails with the catalog entry
intact. The record lives in a warehouse control ledger outside the table
location, where removing the table's metadata tree cannot remove the only route
to its cleanup work. One object per drop avoids a shared read-modify-write
ledger; `_siglake/config/dropped_indexes/<namespace>/<drop-id>.json` follows the
existing delete-task layout without sharing its task type.

The record is immutable except for per-target observations and state. Its
identity and target fields have this shape:

```json
{
  "version": 1,
  "drop_id": "018f0d61-0f3d-7c21-9aa1-b4e5235c15bc",
  "namespace": "default",
  "index_id": "logs",
  "table_uuid": "11111111-1111-1111-1111-111111111111",
  "table_location": "s3://warehouse/default/logs",
  "recorded_at": "2026-09-23T17:00:00Z",
  "targets": {
    "aggregate_prefix": {
      "relative_path": "metadata/siglake-agg/11111111-1111-1111-1111-111111111111/",
      "authorization": "report_only"
    },
    "committed_files": { "authorization": "report_only" },
    "stale_wal": { "authorization": "operator_review" }
  }
}
```

`namespace` and `index_id` are labels for an operator; neither is an address.
The executor forms the absolute aggregate target once from `table_location` and
`relative_path`, then checks that the final path component is exactly
`table_uuid`. It never loads the named table. The record also carries the
incarnation-specific manifest or file inventory needed for committed-file
cleanup; that inventory is outside this aggregate slice.

The initial implementation should create `report_only` records and expose an
inventory such as:

```text
drop 018f0d61… table 11111111… aggregate prefix
  metadata/siglake-agg/11111111…/  37 objects  27,840,112 bytes
excluded replacement prefix
  metadata/siglake-agg/22222222…/  not listed
legacy flat aggregate paths          unowned; not listed
```

That output is a fixture contract, not permission to delete. It proves the
prefix boundary before a destructive mode is enabled.

## Deletion protocol

Once aggregate deletion is authorised for a record, an elected maintenance pass
does the following:

1. Validate the record version, canonical UUID, exact table location, and
   aggregate prefix relationship. Refuse the whole target on any mismatch.
2. List only the recorded aggregate prefix and take a bounded page. Delete the
   exact keys returned by that listing; never derive sibling paths and never
   list the table's shared `metadata/siglake-agg/` parent.
3. Start the next page from the prefix root instead of persisting a continuation
   token across mutations. A crash repeats deletes, which object stores and the
   filesystem path must treat as success when the object is already absent.
4. Mark the pass empty only when a fresh root listing finds no objects. On a
   filesystem store, remove empty
   directories below the UUID prefix after their contents are gone.
5. Keep the record eligible for later sweeps. A delayed publisher that writes
   after the empty observation lands under the dropped UUID and is removed by a
   later pass. It cannot enter the replacement's prefix.

The lasting record is the fence for an arbitrarily delayed publisher. A grace
period followed by one final list is insufficient: the current writers have no
bounded lifetime between commit and PUT. Publication intents or leases could
provide a future quiescence proof, but only if every publisher participates and
the record still handles a process paused after its last check. They are not a
prerequisite for repeated UUID-prefix sweeps.

Concurrent executors are an efficiency concern rather than an identity risk:
both hold the same immutable target and deletes are idempotent. The existing
maintenance election should keep their observations and accounting coherent.
Object deletion precedes the record's counters and empty observation, so no
crash state can record an object as removed while leaving it forgotten.

## Decisions required before deletion ships

- **Record retention.** Keeping the dropped-UUID record indefinitely gives
  delayed publishers an indefinite reap path at the cost of a permanent small
  record and periodic empty-prefix LIST. Expiring it needs a proved upper bound
  on publisher lifetime or an operator acknowledgement that later objects may
  leak. Indefinite retention is the safe default.
- **Aggregate deletion delay.** Aggregates are derived accelerators, but an
  operator may still want a recovery window after an accidental index drop.
  The delay and its override belong to the cleanup policy, not to snapshot or
  orphan-GC ages. `report_only` remains the interim behaviour.
- **Data-file and WAL authority.** The API or command that changes
  `aggregate_prefix.authorization` must not change the committed-file or WAL
  targets with it. Those classes need their own retention and recovery choices.
- **Store failure policy.** Pagination, access denial, and partial DELETE
  failures keep the record active and retryable. The implementation must choose
  its retry budget and alert threshold before an automatic mode is enabled.

## Proof required from the implementation

Keep the current `agg_incarnation_fence.rs` regressions unchanged until deletion
is implemented; today they prove preservation and reader isolation. Add a
separate cleanup fixture that:

1. Builds aggregates for incarnation A, drops it, and records A's UUID and
   location before the catalog entry disappears.
2. Recreates the same index id as incarnation B at the same location and builds
   B's aggregates.
3. Runs one A cleanup pass, writes an A delta and rebuild marker to model a
   delayed publisher, then runs the pass again.
4. Proves A's UUID prefix is empty, every captured B object remains byte-for-byte
   present, and B's Tier-1 answer still equals a direct file scan.
5. Places objects at the legacy flat paths and proves they remain untouched.

Add crash fixtures after a listed-object delete and before its observation is
recorded, and after a partial page. Both retries must converge on an empty A
prefix without a request outside it. A malformed UUID, a prefix outside the
recorded location, or a prefix whose UUID differs from the record must fail
before the first DELETE.

## Scope of the implementation follow-up

The follow-up owns the cleanup-record type and durable write-before-drop
ordering, report-only inventory, the explicit aggregate-deletion authority,
the repeated sweeper, metrics, and the proof above. It must not delete legacy
flat aggregate artifacts. Committed-file and stale-WAL deletion remain separate
targets even if the same record eventually inventories them.

## Implementation status (task #6007)

`DELETE /api/v1/indexes/{id}` now writes and reads back one v1 record before
dropping the catalog entry. The record captures the UUID and location from the
loaded table, the exact UUID aggregate prefix, the current metadata JSON and
every object reachable through its retained snapshots. All committed-file
entries remain inventory only, and the stale-WAL target remains operator
review.

`IcebergContext::sweep_dropped_index_aggregates` validates every record before
the first delete and lists only the recorded UUID prefix. Report-only records
return object and byte counts without mutation. An `aggregate_delete` record
is page-bounded and starts each new page at the prefix root; an absent object
is success on retry, and the record remains for later passes. There is no
production API that grants `aggregate_delete` while the retention, recovery
delay and store-failure policy above remain open.

The storage fixture covers a same-location A→B replacement, byte preservation
under B, a delayed A delta and marker after an empty pass, legacy flat-path
preservation, Tier-1 agreement with a direct scan, malformed-target refusal,
and restart after an already-applied delete. Committed-file and stale-WAL
deletion remain out of scope.
