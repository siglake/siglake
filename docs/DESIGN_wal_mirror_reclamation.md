# Reclaiming WAL mirror objects under the local drain (task #3060)

**Status:** Option C implemented in #4913, off by default
(`compactor.mirrorLedgerReclaim` / `SIGLAKE_MIRROR_LEDGER_RECLAIM`). The open
decisions below are answered; see "What shipped". **Date:** 2026-09-17.

Since #2953 the WAL mirror is on by default. Retention for mirror objects lives
in the catalog-claim drain, which deletes an object because it is the thing that
claimed and committed it (`crates/siglake-compactor/src/lib.rs:1174-1293`). The
default install runs the local filesystem drain, which commits out of local
`sealed/` and never reads the mirror, so `run_retention_bounded` returns at its
first line — `self.catalog` is `None`
(`crates/siglake-compactor/src/lib.rs:1205-1207`). The prefix grows for as long
as the cluster ingests. `docs/LIMITATIONS.md` says so and tells the operator to
add a lifecycle expiry.

This document prices three ways to close that and recommends one. It also
records three defects found while reading the path; each has its own card.

## What grows, and how fast

From `docs/PERF_WAL_MIRROR_2026-09-11.md` (loopback, filesystem-backed store,
4096-event roll), one object per sealed segment:

| rate | objects/s | per day | bytes/day |
|---|---|---|---|
| 20,000 EPS | 4.9 | 421,632 | 37.3 GB |
| documented 50K EPS floor | 12.2 | 1,054,080 | 93 GB |
| loopback saturation (248K EPS) | 61 | 5.3 M | 466 GB |

At 20K EPS an unmanaged prefix reaches 12.6 M objects and 1.1 TB in 30 days, and
keeps going. Bounded at the current `committedRetentionSecs` default of 86,400 s
it holds 422 K objects and 37 GB in steady state. The PUT bill is the same
either way — the mirror pays it at upload — so reclamation buys storage and
list/GET cost, not request cost. S3 DELETE requests and lifecycle expirations
are not billed.

A second population grows with it, and the limitation text does not mention it.
The ingester registers every successful upload in the `wal_segments` table
whenever a catalog URI is configured (`crates/siglake-cli/src/main.rs:2124-2155`),
which the chart sets on every pod
(`deploy/helm/siglake/templates/_helpers.tpl:405`). Those rows are inserted
`status = 'sealed'` (`crates/siglake-storage/src/catalog_claim.rs:430-459`).
Under the local drain nothing ever claims them, nothing marks them committed,
and retention only deletes `committed` rows
(`catalog_claim.rs:1248-1270`). So the default install accumulates one Postgres
row per segment at the same 4.9/s, forever. That is a liability on its own and
an asset for the design below: the ledger the reclaimer needs is already being
written.

## What a safe reclaimer has to satisfy

1. **Commit proof.** Delete only objects whose rows are in Iceberg. An object's
   age is not proof; a backlog estimate is not proof.
2. **Exact object identity.** The key deleted must be the key this drain's
   segment was uploaded to — never a neighbour's, never a previous incarnation's.
3. **Delayed uploads.** The upload is asynchronous and retried
   (`crates/siglake-wal/src/mirror.rs:346-416`). A segment can commit locally
   before its object exists. Deleting a key that is not there yet, and having
   the uploader create it afterwards, leaks an object no pass will revisit.
4. **Catch-up re-creation.** `catch_up_sweep` uploads and re-registers anything
   in `sealed/` or `mirror-pending/` that the mirror is missing
   (`mirror.rs:567-758`). A reclaimer must not race it into recreating what it
   just deleted.
5. **Crash and retry ordering.** Every interruption must leave an inert state
   that the next pass repairs, and repairs must be idempotent.
6. **Table identity.** An index dropped and recreated reuses its `<tenant>/<index>`
   path. The mirror owner marker and its superseded history exist for exactly
   this (`mirror.rs:96-213`); a reclaimer must not launder an unattributable
   object into a delete.
7. **Restart and concurrent drains.** Evidence must survive a compactor restart,
   and two compactor replicas must not both act on it.
8. **The knob operators already hold.** `compactor.committedRetentionSecs`
   (default 86,400 s, `0` opts out, non-zero floored at 901 s —
   `crates/siglake-compactor/src/lib.rs:268-337`) must govern the bound, and `0`
   must keep meaning "delete nothing".

## What the code already gives us

- **Commit is a durable local fact.** The FS drain renames
  `processing/<seg>` → `committed/<seg>` after the Iceberg append returns
  (`finish_segment`), and the file sits there until
  `sweep_committed_coordinated` removes it — soft floor
  `DEFAULT_RETENTION` 60 s, hard ceiling `CONSUMER_MAX_RETENTION` 3600 s
  (`crates/siglake-compactor/src/lib.rs:70-77, 2585-2605`).
- **Upload state is a durable local fact.** `mirror-pending/<seg>` is a hard
  link created before the queue send and removed only after the object is
  confirmed present, including the stat-after-error path
  (`mirror.rs:346-426, 503-541`). Pin absent means the object landed or the
  segment was never enqueued; pin present means an upload is still owed.
- **The key is derivable and already recorded.** `mirror_key_suffix` is
  `<file>`, `<tenant>/<file>` or `<tenant>/<index>/<file>`, and the registered
  row carries the full `segment_url` the uploader used.
- **Object-first deletion, with the reasoning written down.** The claim path
  deletes the object, then the row, so an interruption leaves a committed row
  with no object (inert) instead of an unknown object that a listing
  re-registers (`compactor/src/lib.rs:1174-1193`).
- **Single-owner maintenance.** `maintenance_lease("retention")` plus the
  operation-scoped mirror-reconciliation lease already serialise this work
  across replicas (`compactor/src/lib.rs:296-301, 3530-3552`).

## Option A — object-store lifecycle expiry on the mirror prefix

Ship a lifecycle rule for the mirror prefix in `deploy/terraform/aws` and
document it in the chart.

**Correcting the premise in the card and in `docs/LIMITATIONS.md:754`.**
`deploy/terraform/aws/s3.tf:31-54` does not add a current-object expiry for the
warehouse prefix. It adds one optional rule, gated on
`warehouse_lifecycle_days_to_glacier > 0`, with `filter {}` — bucket-wide — that
transitions objects to `GLACIER_IR` and expires noncurrent versions after 30
days. Two consequences: no current object is ever expired anywhere in the
bucket, and because the rule is bucket-wide it already covers mirror objects
when it is enabled, so a long-lived mirror backlog is being transitioned to
`GLACIER_IR` rather than expired.

**And the prefix in the values file is wrong.** `values.yaml:838-840` says the
mirror prefix "lives as a sibling to `s3.warehousePrefix`, not underneath it".
The uploader's operator is built from the warehouse URL, path included
(`crates/siglake-cli/src/main.rs:2114`, `build_opendal_operator` at `:3567-3592`),
and the key is `<prefix>/<suffix>` relative to that root. Measured on this box
with `--warehouse-url file://<dir>/warehouse`, three OTLP records produced
`<dir>/warehouse/wal-mirror/default/<ingester>-<uuid7>.arrow`. Under the chart
the real key is `s3://<bucket>/<s3.warehousePrefix>/<wal.mirror.prefix>/…`. An
operator who writes the documented rule against `wal-mirror/` gets a rule that
matches nothing, and no signal that it matched nothing. That is the specific
failure this option is most exposed to.

| | |
|---|---|
| Effort | ~1 day, no Rust: one `aws_s3_bucket_lifecycle_configuration` rule, one variable, chart and README wording. |
| Bound | Whatever days the rule names. |
| Proof | None. It expires by age, which is the thing the card refuses as a default. |
| Failure mode | Silent on both sides: a wrong prefix expires nothing, a short window expires un-drained segments and the loss is only visible after a PVC failure. |

Age expiry is a legitimate **operator** choice on an install whose drain backlog
they can bound themselves, and it is the only option that also catches objects
no drain will ever account for (a dropped incarnation's segments, an ingester
whose PVC was destroyed before its segments drained, `_active/` blobs). It is
not a legitimate default, and it should not be the thing that bounds the prefix
on an install that never touched it.

## Option B — a durable reclaim journal owned by the compactor

The compactor appends `(<mirror key>, <commit ms>)` to a rotating journal on the
WAL volume after each commit, fsyncs it, and a periodic pass deletes objects
older than `committedRetentionSecs`, then truncates. Self-contained: no catalog,
works with SQLite installs and with no catalog at all.

| | |
|---|---|
| Effort | ~3–5 days: a new durable on-disk format, rotation, crash-truncation recovery, its own retention pass, metrics, alerts, tests. |
| Bound | `committedRetentionSecs`, measured from the commit. |
| Proof | Yes — the journal entry is written after the Iceberg commit. |
| Cost | ~30 MB/day of journal at 20K EPS; one append+fsync per commit batch on the commit path, not the seal path. |

It satisfies every requirement, and it duplicates machinery that exists and is
already exercised: the purge query, the object-first ordering, the page budgets,
the leases, the metrics and the two alerts. A second durable format with its own
crash semantics is the part to weigh; siglake already carries the WAL, the pins,
the owner markers and the mirror-sync cursor.

## Option C — mark the ledger that already exists (recommended)

The ingester already writes a `wal_segments` row per uploaded object, with the
exact `segment_url`. The local drain already knows, durably, which segments it
committed. Connect the two: after the local commit, transition that segment's
row from `sealed` to `committed`, and let the existing retention pass delete the
object and the row.

Shape:

1. **Ledger-only catalog config.** A new compactor mode that connects the claim
   store and builds the mirror operator (`--catalog-uri` + `--warehouse-url` +
   the existing `--mirror-prefix`, which already defaults to `wal-mirror` and is
   parsed regardless of claim mode) **without** claiming. The claim path, the
   mirror-to-catalog reconciliation sweep and the abandoned-claim reclaim stay
   off: this mode must never register an object it did not commit, which is the
   only thing that could turn a listing into a delete.
2. **A mark step in the FS drain, driven off `committed/`, not off the commit
   call.** On every cycle, for each file in `committed/`, upsert its row to
   `committed`. Driving it off the directory rather than the commit return makes
   it idempotent and crash-repairing for free: a compactor that dies between the
   Iceberg append and the mark finds the file still in `committed/` next cycle.
3. **Couple the local sweep to the mark.** `sweep_retention_at` may only remove a
   `committed/` file whose mark is durable. That is the invariant that makes the
   evidence outlive every crash: local evidence is destroyed only after remote
   evidence exists. A hard ceiling still applies — see the open decision below.
4. **Skip pinned segments.** A segment with a live `mirror-pending/` link has an
   upload owed. Mark it, but do not let retention delete its object until the
   pin is gone. The simplest form: the mark step skips pinned segments entirely
   and picks them up on a later cycle, and the ceiling in (3) covers a pin that
   never clears.
5. **Retention unchanged.** `run_retention_bounded` already deletes the object
   then the row, in pages, under two leases, with `committedRetentionSecs` and
   its `0` opt-out and 901 s floor.

### Against each requirement

- **Commit proof** — the mark is written after the Iceberg append and is driven
  off the `committed/` rename, which is itself ordered after it.
- **Exact identity** — the row's `segment_url` is the key the uploader wrote.
  Where the row is absent, the key derived from the WAL layout is a fallback,
  and the two should be compared where both exist, with a counter on
  disagreement. Segment names are `<ingester_id>-<uuid7>`
  (`crates/siglake-wal/src/lib.rs:943`), so two writers cannot collide on a key.
- **Delayed uploads** — no row means no object: the registrar inserts only after
  a confirmed upload. The pin rule in (4) covers the reverse window. A late
  catch-up upload registers with `ON CONFLICT DO NOTHING`
  (`catalog_claim.rs:444`), so a `committed` row written first survives and
  retention still collects the object. This is why the mark should be an upsert
  rather than an update: an update alone loses the race and leaks the object.
- **Catch-up re-creation** — `catch_up_sweep` reads `sealed/` and
  `mirror-pending/` only. A locally committed segment is in `committed/` and, by
  (4), unpinned, so no candidate exists to re-upload.
- **Crash ordering** — object-then-row, unchanged. Mark-then-sweep, new. Both
  leave inert states.
- **Table identity** — segments of a dropped incarnation are quarantined into
  `stale/` by the FS drain's owner check and never commit, so they are never
  marked and never deleted. They stay in the mirror; only Option A collects
  them. Say so in the limitation rather than widening the delete.
- **Restart and concurrency** — evidence is the `committed/` directory plus the
  ledger, both durable. Retention already runs under
  `maintenance_lease("retention")` and the mirror-reconciliation lease; the mark
  step is an idempotent upsert and needs no lease.
- **The knob** — `committedRetentionSecs` governs it, `0` still deletes nothing,
  and the bound lands exactly where the card asked for it.

### Cost and effort

| | |
|---|---|
| Effort | ~2–3 days: one SQL method (`sealed`→`committed` upsert with a guard), a ledger-only config, the mark step, the sweep coupling, metrics, tests. No new durable format, no new alert. |
| Steady state | 422 K objects / 37 GB at 20K EPS with the default 24 h, and the Postgres row population becomes bounded at the same time. |
| Added request cost | One catalog UPDATE per commit batch; DELETEs are free. No listing. |
| Requires | A catalog URI, which the chart sets on every pod, and a reachable object store from the compactor, which the claim path already assumes. |

### What Option C does not cover

- Installs with no explicit catalog URI. `--catalog-uri` / `SIGLAKE_CATALOG_URI`
  unset means the ingester's registrar never starts, so there are no rows to
  mark; the Iceberg context's own SQLite fallback under the warehouse directory
  is not the same thing. The chart sets the URI on every pod, so this is the
  single-binary dev shape, where the prefix growing is not the problem it is on
  a cluster.
- Objects no local drain ever commits: a dropped incarnation's segments, an
  ingester whose volume was lost before its segments drained, and `_active/`
  blobs. These need Option A, and the limitation should keep saying so.

## Recommendation

Implement Option C, keep Option A documented as the operator-side complement,
and leave the limitation in `docs/LIMITATIONS.md` in place until C ships. The
deciding argument is not effort: it is that C's evidence — the `committed/`
rename and a ledger row written by the process that uploaded the object — is
already durable, already crash-ordered, and already being maintained by the
default install, while B asks for a second durable format to hold the same fact
and A holds no fact at all.

## What shipped (#4913)

Option C, opt-in. The answers to the two open decisions below:

1. **Ledger unreachable:** sweep at the 3600 s `CONSUMER_MAX_RETENTION` ceiling
   and count the leak. `sweep_committed_gated` takes the set of names whose
   mark is durable and holds everything else — except past the ceiling, where
   the file goes anyway and `siglake_compactor_mirror_unreclaimed_total`
   records that its object is now beyond this drain's reach. That counter and
   `siglake_compactor_mirror_mark_errors_total` are pre-registered at 0 on
   every compactor and read by panel 163 of `deploy/grafana/siglake-overview.json`.
   Neither pinned bytes nor the accumulated mirror leak is bounded by this; the
   WAL volume is.
2. **Not on by default.** Default-on needs a retained object-store acceptance
   run and a separate non-patch release decision, not one release elapsing.

What the implementation adds, against the shape above:

- `Compactor::with_mirror_ledger` — the ledger-only config. Ignored if a
  catalog claim is already attached.
- `SqlSegmentClaim::mark_committed_local` — the upsert. It is not
  `mark_committed` (which requires `status = 'processing' AND claimer = ?`),
  it writes no consumed-proof watermark, it stamps `committed_at_ms` once and
  preserves it across the per-cycle re-mark, and it leaves a `processing`,
  `released` or quarantined row untouched.
- The mark runs inside the per-directory retention sweep, so every cycle shape
  that sweeps also marks, and the gate is computed from the same listing.
- `catch_up_sweep` no longer uploads a candidate whose only remaining local
  name is `committed/`. Discovery is not proof that the upload is still owed:
  a candidate found in `sealed/` at the start of a pass can be committed,
  marked and reclaimed before the pass reaches it, and the old
  `processing/`-or-`committed/` read fallback would then recreate the object.
  `processing/` keeps the fallback — that commit has not returned.
- A per-directory cache of ids already seen `committed` keeps the re-mark to
  one catalog round trip per new file, and stops a re-mark from resurrecting a
  row retention has already purged.

Not covered, and still true: a locally-committed segment whose row retention
purged while its local file is held past the retention window (a stuck
consumer, i.e. > 901 s after the mark) can have that row re-inserted once by a
restarted compactor, which costs a no-op object delete and a bounded row. A
second `committed_retention` window collects it.

## Open decisions for the maintainer

*Both answered above; kept for the reasoning.*

1. **What happens when the ledger is unreachable and `committed/` cannot be
   swept.** Coupling the sweep to the mark means a long Postgres outage grows
   the WAL volume. The existing `CONSUMER_MAX_RETENTION` (3600 s) is the natural
   ceiling: past it, sweep anyway, count the object as leaked
   (`siglake_compactor_mirror_unreclaimed_total`) and let Option A or a manual
   pass collect it. That trades a bounded leak for a bounded disk. The
   alternative — never sweep until marked — trades the disk for the leak.
2. **Whether the ledger-only mode is on by default.** On by default closes the
   growth for every existing install at upgrade; off by default keeps the
   compactor's dependency surface where it is today (the FS drain currently
   needs no claim-store connection) and leaves the limitation standing for
   anyone who does not opt in.
3. **Whether to add the Terraform rule as well**, given the prefix correction
   above — and if so, whether it is an expiry or only the documented example.

## Verification an implementation card must carry

All but the last are in `mirror_ledger_reclaim_tests`
(`crates/siglake-compactor/src/lib.rs`), `local_commit_mark_tests`
(`crates/siglake-storage/src/catalog_claim.rs`) and the gate and sweep tests in
`crates/siglake-wal`:

- A test that an object whose segment was committed locally is deleted after
  `committedRetentionSecs`, and its row with it. —
  `a_locally_committed_segment_is_reclaimed`
- A negative control: with `SIGLAKE_COMMITTED_RETENTION_SECS=0`, nothing is
  deleted. — `the_retention_opt_out_reclaims_nothing`, driven through
  `committed_retention_from(Some("0"))` rather than the process environment.
- A test that a segment with a live `mirror-pending/` pin is not reclaimed, and
  that an upload landing after the mark still leaves a `committed` row. —
  `a_pinned_segment_is_neither_marked_nor_swept`,
  `a_mark_that_precedes_the_registration_still_collects`
- A crash test: kill between the Iceberg commit and the mark; the next cycle
  marks it from `committed/` and the object is still reclaimed. —
  `a_crash_between_the_append_and_the_mark_is_repaired`
- A test that a dropped incarnation's quarantined segments produce no delete. —
  `quarantined_segments_produce_no_delete`
- An object-store acceptance run (MinIO or a prepared AWS round) measuring the
  prefix's object count reaching steady state rather than growing, with the
  measurement stated next to `committedRetentionSecs`. — **outstanding**, and
  the gate on any decision to make this default-on.

## Defects found while reading this path

1. **`siglake wal-recover` recovers nothing from any URL with a path.**
   FIXED (#4912). `run_wal_recover` built the operator rooted at the whole
   `--from` URL and then passed the same path as the listing prefix, so it
   listed `<path>/<path>/`. Measured with the release binary against a
   `file://` store holding one segment: `pulled 0 segments`, for both
   `--from …/warehouse/wal-mirror` and `--from …/store`. This is the documented
   DR path and the justification for the mirror existing. The prefix is now
   relative to the operator's root — empty on the CLI path, since
   `build_opendal_operator` roots the store at the URL — and
   `recover_from_object_store` accepts an empty prefix instead of listing `"/"`
   and stripping `"/"` off relative keys. `crates/siglake-cli/tests/cli/wal_recover_cli.rs`
   runs the binary against a `file://` mirror, with and without a trailing
   slash.
2. **`_active/` blobs are never reclaimed by anything.** The active mirror
   overwrites one key per in-flight segment, and when that segment seals its
   blob is left behind; mirror-to-catalog sync explicitly skips `_active/`
   (`crates/siglake-compactor/src/lib.rs:6040-6042`) and no retention path
   touches it. Off by default (`activeIntervalSecs: 0`), so it bounds nothing
   today, but an install that turns it on leaks one object per segment even in
   claim mode — and since #5055 that is one per (tenant, index, write shard,
   segment), because the loop covers every writer that holds rows rather than
   the one root writer it used to be handed.
3. **The ingester's local WAL sweep never looks at per-index WAL directories.**
   `local_wal_sweep_once` walks tenant directories and the root
   (`crates/siglake-cli/src/main.rs:1529-1534`), while the catch-up sweep and the
   drain also walk `<tenant>/<index>/` (`mirror.rs:579-587`). In claim mode the
   PVC-fill defect that sweep was written to close is therefore still open for
   managed indexes.
