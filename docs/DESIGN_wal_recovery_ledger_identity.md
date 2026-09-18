# Ledger-backed root identity for `siglake wal-recover` (task #4974)

**Status:** design and local qualification. **PROCEED with revisions** — the
rules below are settled and measured, and the production `--catalog` slice is
its own card. Nothing in this document is shipped: `wal-recover` behaves
exactly as #4973 left it. **Date:** 2026-09-18.

Option D of `docs/DESIGN_wal_recovery_root_identity.md`. #4973 shipped option
C: the command plans unless it is given `--apply`, and the same listing carries
a root verdict read off the two markers siglake writes at a fixed depth under
the mirror root. The population with neither marker — no managed index, active
mirroring off — is the default install, and its listing one component above the
mirror root is indistinguishable from a legitimate mirror whose first tenant is
named after a prefix. `wal_segments` can distinguish them, because the uploader
recorded where each object belongs before the volume was lost.

The evidence is `crates/siglake-storage/tests/wal_ledger_identity_prototype.rs`
— 19 hermetic cases, each against a SQLite ledger it builds itself, one of
them driving the shipped `plan_recovery` over a real `file://` mirror.

## What the ledger knows, and why it is exact

`register` records `(id, tenant, index_id, segment_url, …)` for every object
the uploader PUT, where `id` is the segment basename without `.arrow` and
`segment_url` is the key it wrote, relative to the warehouse root
(`crates/siglake-storage/src/catalog_claim.rs:480-509`). The filesystem drain's
`mark_committed_local` inserts the same three columns for a segment with no row,
composing the url from the configured prefix and the local WAL layout
(`crates/siglake-compactor/src/lib.rs:2906-2918`).

Those three columns are write-once. Every `UPDATE wal_segments` in
`catalog_claim.rs` sets `status`, `claimer`, `claimed_at_ms`,
`committed_at_ms`, `attempts` or `not_before_ms`, and none of them names
`tenant`, `index_id` or `segment_url`. A row's identity is therefore whatever
the writer that created the object recorded, whatever the segment has done
since — claimed, committed, released, quarantined. That is what makes a
lifecycle column irrelevant to this check and `status` unread.

The id is the join key and it is a uuid7 basename, so two objects cannot share
one. If they ever did, `ON CONFLICT(id) DO NOTHING` means the ledger keeps the
first row, and the check would see the second object's key disagree with it —
a refusal, not a reroute.

## The two spellings, and the only comparison that is safe

`--from` is a full URL and the store is rooted at it, so a listed key is
relative to the mirror root: `acme/orders/<id>.arrow`. `segment_url` is
relative to the warehouse root and carries the mirror prefix:
`wal-mirror/acme/orders/<id>.arrow`. Neither string can be compared with the
other, and the url's head must not be compared with `--from` at all — restoring
from a COPY of the mirror in another bucket is a legitimate DR shape, and a url
match would refuse it
(`a_relocated_mirror_copy_is_confirmed_because_only_the_tail_is_compared`).

Two derived facts do the work:

1. **Routing.** The key implies `(tenant, index_id)` through the same layout
   contract `recovery_target` routes on; the row states it. Agreement is the
   verdict.
2. **Prefix above.** The components of `segment_url` left over when the listed
   key is stripped off its tail. At the mirror root that is the mirror prefix,
   one or more components. `--from` one component too high makes the listed key
   carry the prefix itself, so the leftover is EMPTY — which is the signal, and
   the only one available for a key recovery refuses on its depth.

An `_active/<tenant>/<id>.arrow.partial` object has no row of its own:
`register` runs on the sealed upload. It is compared in its sealed form, with
the `_active/` component and the `.partial` tail removed
(`an_active_mirror_object_is_compared_in_its_sealed_form`). Getting that wrong
would turn every active-mirror object into a disagreement and refuse the one
population #4973 can already confirm.

## The verdict, and what each state does

| state | when | effect |
|---|---|---|
| **Confirmed** | ≥1 listed object matched a row, every match agrees on routing, and the leftover prefixes agree with each other | the root is settled; `--apply` proceeds |
| **Contradicted** | any match disagrees on routing, or its leftover prefix is empty, or two matches disagree about the prefix | refuse whole, naming the directory to pass instead |
| **Silent** | the ledger was read and no listed object matched a row | no evidence; #4973's verdict and the plan stand unchanged |
| **Unavailable** | the ledger could not be opened or has no `wal_segments` | reported, never downgraded to Silent |

Precedence against #4973's marker verdict:

- a marker `Contradicted` is final. `--catalog` may not overturn it, for the
  reason Todd settled `--force` against on 2026-09-17: the way past a
  contradicted root is to pass the directory the refusal names. A ledger that
  confirms a listing whose marker contradicts is itself a contradiction —
  report both and refuse.
- a marker `Unverified` with a ledger `Confirmed` is the whole point of this
  slice: the default install gets a definite answer.
- a marker `Confirmed` with a ledger `Contradicted` refuses. Both markers are
  conclusive where they appear and the ledger is exact; a listing they disagree
  about is not one mirror root.

Two listed segments registered under DIFFERENT prefixes refuse even though the
routing agrees for both
(`two_prefixes_in_one_listing_are_refused_even_though_the_routing_agrees`): the
listing is a union of two mirrors, and a restore cannot be right for both.

## Partial matches

Retention deletes a row as soon as its object is gone
(`purge_committed_ids`, `catalog_claim.rs:1500-1530`), so a mirror routinely
holds objects with no row and a partial match is the ordinary case rather than
the exception. The rule has two halves, because the two questions are different:

- **The root is a property of `--from`, not of an object.** One agreeing match
  settles where `--from` points, and that is what the Confirmed state claims.
- **Certification is per object.** An unmatched object is certified by nothing.
  It keeps the routing its key implies — the same routing it would have had
  with no `--catalog` at all — and the plan prints how many there are. No
  object is ever moved to a routing the ledger states: a disagreement refuses,
  and the refusal reports both the ledger's routing and the key's without
  applying either
  (`a_partial_match_confirms_the_root_and_certifies_only_what_it_matched`,
  `one_disagreeing_row_refuses_a_listing_the_rest_of_which_agrees`).

What that leaves open, and the residual risk to accept: a listing whose matched
objects are a genuine mirror and whose unmatched objects came from somewhere
else is Confirmed, and the unmatched ones are restored on their key evidence.
This is the pre-#4974 behaviour for those objects exactly, so `--catalog` makes
nothing worse — it just does not make everything better.

## Read-only inspection, measured

`SqlSegmentClaim::connect` runs `ensure_schema` (`catalog_claim.rs:291`), so
reusing it would migrate the catalog a recovery PLAN is inspecting. The reader
opens SQLite `mode=ro` instead and issues one statement shape, a `SELECT` of
the four identity columns.

A/B'd on one file in
`a_read_only_lookup_leaves_the_ledger_file_byte_identical`: after the lookup
the database and its sidecars are byte-identical, and after
`SqlSegmentClaim::connect` on the same file `table_leases`,
`mirror_sync_cursors` and `consumed_proof_watermarks` exist.

The open mode is not a free choice. Measured on this SQLite:

| journal mode | directory | `mode=ro` | `mode=ro&immutable=1` |
|---|---|---|---|
| rollback | writable | opens | opens |
| rollback | read-only | opens | opens |
| WAL | writable | opens | opens |
| WAL | read-only, no `-shm` present | **`attempt to write a readonly database`** | opens |

A WAL-mode database needs a `-shm` file created beside it, which a read-only
mount refuses, though every statement is a SELECT. sqlx does not set
`journal_mode` unless it is asked to
(`sqlx-sqlite-0.8.6/src/options/mod.rs:177-181`) and nothing in this workspace
asks, so siglake's own SQLite catalogs are rollback-journal and `mode=ro`
covers them — including one a live deployment still holds open
(`a_ledger_a_live_deployment_is_still_writing_opens_read_only`). `immutable=1`
is what a WAL-mode catalog on a rescued read-only volume costs, and it is not
a default: it ignores the `-wal` sidecar, so against a live database it returns
a stale snapshot and the check would call it exact
(`a_read_only_mount_reads_a_rollback_catalog_but_needs_immutable_for_a_wal_one`).

Postgres has no URI equivalent of `mode=ro`, so the production reader runs its
SELECTs inside `START TRANSACTION READ ONLY` and lets the server refuse a
write. There is no Postgres in a lane, so the statements are parse-gated in the
Postgres dialect (`the_read_only_statements_parse_as_postgres`), the same cover
the watermark statements get.

## Cost

The card's constraint is that the listing may be far larger than the retained
ledger. The lookup is keyed by the LISTING, `IN (…)` 256 ids at a time — the
same conservative width `purge_committed_ids` uses — so its queries scale with
the listing and its memory with the MATCHED set. Measured
(`a_listing_far_larger_than_the_ledger_costs_one_query_per_chunk`, debug build,
file-backed SQLite):

| listing | ledger | form | queries | rows read | resident | wall |
|---|---|---|---|---|---|---|
| 2,000 | 200 | chunked `IN` | 8 | 200 | 42.8 kB | 6.9 ms |
| 2,000 | 200 | whole-ledger scan | 1 | 200 | 42.8 kB | 2.9 ms |
| 200 | 2,000 | chunked `IN` | 1 | 200 | 42.8 kB | — |
| 200 | 2,000 | whole-ledger scan | 1 | 2,000 | 428 kB | — |

Resident is the lookup table's own string bytes plus its fixed per-entry cost,
counted rather than sampled from RSS. The chunked form is the one to ship: the
whole-ledger scan is one query whatever the listing, and holds the whole ledger
to do it, and the ledger is the side a restore does not bound — a fleet's
backlog is not limited by the objects one `--from` happens to list. At 214 B of
resident per row, a million-row backlog is ~214 MB in the scan form and the
size of the listing's intersection in the chunked one.

Against the plan's own cost the lookup disappears: the plan already pays one
LIST plus one GET per candidate it would write (#5077). A mirror of 20,000
segments is 79 SELECTs next to 20,000 GETs.

## What the production slice has to carry

1. **A read-only reader**, in `siglake-storage` beside the claim: SQLite
   `mode=ro`, Postgres `START TRANSACTION READ ONLY`, chunked lookup, no DDL,
   no `ensure_schema`, and `wal_segments` missing reported as unavailable
   rather than empty.
2. **Candidate ids out of the plan.** `RecoveryPlan::candidates` is private and
   `PlanGroup` carries one sample key, so nothing today can hand the ledger the
   ids a plan would write. Either the plan exposes them or `plan_recovery`
   takes an already-resolved `HashMap<String, LedgerRouting>`. The second keeps
   `siglake-wal` free of sqlx, which is why the prototype's verdict arithmetic
   is pure: the CLI has both crates, `siglake-wal` has neither a catalog
   dependency nor a reason to grow one.
3. **The skipped keys too.** A key recovery refuses on its depth still has an
   id, and looking it up turns the generic "restored nothing, `--from` must
   name the mirror root" bail into a proof with a directory in it
   (`a_deep_mirror_one_component_up_is_refused_from_keys_the_plan_skipped`).
4. **The plan's own lines**: the verdict, the matched count, the uncertified
   count, and the prefix the ledger recorded. An operator reading `matched 412,
   uncertified 3` is reading how much of the restore is vouched for.
5. **`docs/LIMITATIONS.md` and `docs/ARCHITECTURE.md`** move with the flag, and
   the DR runbook in siglake-docs with them.

## Decisions this needs before the flag ships

- **An unavailable ledger is a hard error under `--catalog`, not a downgrade.**
  An operator who asked for exact evidence and silently got a plan is the
  failure this card was split out to avoid; the remedy is to drop the flag, and
  the message says so. The alternative — warn and fall back to #4973 — makes
  `--catalog` safe to leave in a runbook forever, which is also what makes it
  worthless there.
- **A Silent ledger is not an error.** A fully-drained mirror has no rows, and
  that is the healthy deployment.
- **No `--catalog-immutable` in the first slice.** The WAL-mode read-only-mount
  case is reported with both remedies named (copy the database somewhere
  writable, or the flag if it is added), because a flag that silently reads
  around a live `-wal` would break the exactness this whole slice is for.

## Disposition

**PROCEED with revisions.** Every rule the card left open is settled above and
measured, the cost is negligible against the plan's, and the read-only
constraint holds mechanically rather than by discipline. The revisions relative
to the card's sketch:

- the verdict rests on ROUTING agreement, not on matching `segment_url` against
  the `--from` URL, so a relocated mirror copy is not refused;
- a partial match confirms the root and certifies only what it matched, and
  never reroutes;
- the check reads the ids of keys the plan skipped, not only its candidates;
- `--catalog` never overturns a marker refusal.

The production slice is a separate card, blocked on this document. What it does
not need is another qualification: the arithmetic is pinned by 19 hermetic
cases, and the only thing a round could add is a Postgres arm, which the
compose step is the place for.
