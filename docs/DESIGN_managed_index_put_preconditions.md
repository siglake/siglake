# Design: optional managed-index PUT preconditions (#2567)

Status (2026-09-19): **implemented by #5473.** A false `If-Match` returns
`412 Precondition Failed`, including when the condition becomes false after a
lost catalog CAS and transaction replay. This supersedes the earlier `409
Conflict` proposal; headerless mapping conflicts keep their existing status.

## Current behavior and scope

`PUT /api/v1/indexes/{id}` replaces the full `IndexConfig`, subject to the
additive mapping rules. A request without `If-Match` carries no version. If
another writer changes the mapping after the caller reads it,
`SetIndexMappingAction` re-validates the body against every Iceberg commit
base and refuses an
incompatible body. The HTTP response is `400`, because the server cannot tell
whether the client intended to condition the update on the older mapping.

The extension is optional optimistic concurrency for this one route:

- `GET /api/v1/indexes/{id}` returns a strong `ETag` for the managed-index
  representation it returns.
- `PUT /api/v1/indexes/{id}` accepts `If-Match`. A matching condition permits
  the existing additive validation and commit. A failed condition returns the
  mapping observed at the refusal and its ETag.
- A PUT without `If-Match` keeps today's behavior, including `400` for an
  additive-rule refusal and a no-op for an already-stored update.

Index-template policy is outside this design. The proposed refusal counter is
also outside it: no operator, alert or dashboard has been named as its reader.

## What the validator identifies

The entity-tag identifies one table incarnation's full managed-index
configuration, not one Iceberg snapshot or metadata file:

```text
SHA-256(
  "siglake:index-config-etag:v1\0" ||
  table_uuid || "\0" ||
  deterministic_IndexConfig_JSON
)
```

The wire value is the base64url encoding of that digest inside the quotes of a
strong entity-tag. Clients treat it as opaque. The `v1` domain allows a later
serialization change without making old and new validators compare equal.

The inputs have these consequences:

- The full `IndexConfig` is covered, including `doc_mapping`, retention and
  `index_at_flush`, because PUT replaces all of them.
- Appending data, compacting files, expiring snapshots and other commits that
  leave the config unchanged leave the validator unchanged. A metadata
  location, snapshot id or table sequence number would incorrectly refuse a
  mapping update after any of those commits.
- The table UUID makes a deleted and recreated index a different entity even
  when its id and config bytes are identical.
- The digest is computed from the parsed config's server-owned deterministic
  serialization, not the raw table-property bytes. Whitespace or an older
  property writer's formatting does not create a different validator.

The ETag is a validator for the API representation. It is not a public mapping
version that a client can increment or interpret, and it makes no promise to
survive a server release that changes the representation.

## Header contract

The implementation should parse `If-Match` using the HTTP entity-tag grammar.
One or more strong tags match when any equals the current tag. A weak tag never
strongly matches. `*` matches any existing index; it supplies existence
protection, not mapping-version protection. Malformed header syntax is `400`.

The handler performs body decoding, path/body id comparison and intrinsic
`IndexConfig` validation as it does now. It then loads the table once, deriving
the current config, table UUID and ETag from that same table object. This avoids
a delete/recreate or mapping commit splitting the returned config from its
validator.

When `If-Match` is present:

1. A false condition returns the conditional-failure status and does not run
   the update.
2. A true condition proceeds to the additive-only checks. An invalid mapping
   remains `400`; a matching ETag does not make a drop, reorder, retype or
   required-field append valid.
3. The expected condition travels inside `SetIndexMappingAction`. The action
   recomputes the current ETag before changing properties on every commit
   attempt. A false condition there is a typed precondition failure, distinct
   from every existing `IndexManagerError` mapping refusal.

Step 3 is required even though the handler checked the same condition. Iceberg
refreshes a transaction's base after a lost catalog compare-and-swap and
replays its actions. A check made only before transaction construction can be
true for the first base and false for the base that is committed.

If an intervening commit changes data only, the replay sees the same ETag and
continues. If it changes the managed-index config, the replay refuses before
the mapping or schema action is emitted. Existing additive validation still
runs after the ETag check; a schema contradiction with an unchanged config is
an invalid update (`400`), not evidence that the mapping precondition failed.

## Conditional-failure response

The `412 Precondition Failed` response contract is:

```json
{
  "error": "managed index `logs` changed since the supplied If-Match value",
  "code": 412,
  "current": {
    "index_id": "logs",
    "doc_mapping": {},
    "retention": null,
    "index_at_flush": null
  }
}
```

The response carries the matching current validator in `ETag`. The actual
`doc_mapping` is the full stored value; it is abbreviated above. A dedicated
OpenAPI response DTO should describe `current` instead of relying on the shared
error body's allowance for extra properties. GET and successful PUT responses
also carry the ETag for their returned config.

`current` means the config from the exact transaction base against which the
condition failed, paired atomically with that base's ETag. In a two-writer race
it is the winner that made the loser stale, so the loser can re-derive its
append without another GET. It is not a lock or a claim that the value remains
current when the client receives it: a third writer may commit after the
refusal. Retrying with the returned ETag can therefore receive another
conditional failure.

The implementation decision was:

| status | fit | cost |
| --- | --- | --- |
| `412 Precondition Failed` | RFC 9110 section 13.1.1 defines this exact answer when `If-Match` is false. Generic HTTP clients and middleware already understand it. | It changes #2567's original `409` acceptance and adds a status not otherwise used by this route. |
| `409 Conflict` (rejected) | It matched the original proposal and reads as an application-level conflict beside index-create's existing `409`. | It gives standard `If-Match` a nonstandard false-condition response; clients must know this route's special rule. |

`428 Precondition Required` does not apply because the header remains optional.
Task #5473 chose `412` for both an initial mismatch and one detected after a
transaction retry.

## Race qualification

The storage action already has deterministic fixtures for the two commit
windows. `a_stale_retention_edit_is_refused_after_a_concurrent_column_addition`
puts the winner between preparation and commit.
`a_retried_attempt_is_revalidated_against_the_base_it_lost_to` injects the
winner after the losing attempt has built its update but before its catalog
compare-and-swap, forcing action replay on the winner's base. #5473 extends
that seam with the typed conditional refusal and route-level response.

The implementation acceptance matrix is:

| interleaving | required result |
| --- | --- |
| A and B read E0; A appends `a` with E0; B appends `b` with E0 after A commits | A returns `200`/E1. B returns `412` with A's config and E1. Only `a` is stored. |
| B's first commit attempt checks E0, then A wins the catalog CAS | B is replayed on E1 and fails there. It must not commit from its first check. |
| A commits data only between B's read and B's mapping PUT | B's E0 still matches and B commits. |
| B sends E0 and a non-additive body while E0 is current | `400`; the body is invalid rather than stale. |
| B sends no header and loses an incompatible mapping race | Existing `400`, with the winner stored. |
| A and B send the same update without a header | Existing idempotent no-op behavior; no second metadata version. |
| An index is deleted and recreated with the same id and config | The old ETag fails because the table UUID changed. |

The HTTP tests should exercise an initial mismatch and the replay-after-lost-CAS
hook, assert the response config/ETag pair, and prove a matching PUT commits.
The storage tests should also force an unrelated data commit between the first
check and a replay so the mapping ETag is proven independent of table metadata
churn.

## Implementation slice (#5473)

The implementation slice completed the public contract and its generated
artifacts:

1. Added the validator/config pair and typed precondition failure to the storage
   index manager, including the action replay check.
2. Added GET/PUT ETag headers, `If-Match` parsing and the `412` response status
   and body to the query server.
3. Added the race matrix's storage and route tests.
4. Regenerated `docs/api`, updated `ARCHITECTURE.md` and `LIMITATIONS.md`, and
   filed the merge-gated siglake-docs contract update.

No metric belongs in that slice until a named dashboard, alert or operator
workflow will read it.
