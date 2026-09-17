# Root identity for `siglake wal-recover` (task #4964)

**Status:** option C is SHIPPED (#4973, 2026-09-17): `wal-recover` plans unless
it is given `--apply`, and the listing carries the root verdict. No `--force`,
no 0.1.1 default change, and `--catalog` (option D) is still a follow-on slice.
The sections below are the investigation that chose it; the measured behaviour
in "What the restore does, measured" is the PRE-#4973 picture, kept because it
is what the remedy is priced against. **Date:** 2026-09-17.

#4928 made a restore that recognised NO key exit nonzero. The case it cannot
see is `--from` exactly ONE component above the mirror root. The shallowest key
under the root is `<tenant>/<segment>.arrow`; one component up the same object
is `<prefix>/<tenant>/<segment>.arrow`, which `recovery_target`
(`crates/siglake-wal/src/mirror.rs:1008-1014`) reads as the
`<tenant>/<index>/` layout. Segments are restored under a tenant named after
the mirror prefix, the command prints `pulled N segments` and exits 0.

This document records what a `file://` qualification measured, shows why no
rule reading only the keys can fix it, prices four remedies and recommends one.
The evidence is
`crates/siglake-cli/tests/cli/wal_recover_root_identity.rs`, hermetic
`file://` cases that ran the real binary. They measured the behaviour
described below; #4973 rewrote them onto the shipped contract, so they now
assert the refusal where a marker is misplaced and the plan-and-no-write
where none is, and they still pin the legitimate installs no key can
distinguish from the mistake. Two of them came from #4972, which fixed the
unrelated discovery-dir defect this qualification turned up (see "Defects
found while reading this path").

## Why one component, and why it is the likely mistake

`wal.mirror.prefix` is relative to the warehouse URL, so the mirror root is
`s3://<bucket>/<s3.warehousePrefix>/wal-mirror/`
(`deploy/helm/siglake/values.yaml:864-867`). Its parent is the warehouse URL —
the string already in the operator's values file, their environment and their
shell history, and the one a DR runbook reader is most likely to paste.
Pointed there, recovery skips every Iceberg object (none ends in `.arrow`) and
pulls the mirror's shallow keys into the wrong place:
`the_warehouse_url_an_operator_already_has_is_the_one_component_miss`.

## What the restore does, measured

| `--from` | mirror layout | report | where segments land |
|---|---|---|---|
| root | any | `pulled 3 segments` | correct |
| one up | `<tenant>/<index>/<seg>` | `pulled 0`, skipped | nothing written |
| one up | `<tenant>/<seg>` | `pulled N segments` | `<wal>/<prefix>/<tenant>/sealed/` |
| one up | `<seg>` (legacy flat) | `pulled N segments`, **no skip count** | `<wal>/<prefix>/sealed/` |
| two up | any | `pulled 0`, all skipped, **exit 1** | nothing (#4928) |

Only the deepest layout is caught, and it is caught by accident: its keys land
past the depth limit. The legacy flat mirror is the worst line in the table —
the report is character-for-character a correct restore
(`a_legacy_flat_mirror_one_component_up_reports_a_clean_restore`).

## What the drain then does

The card asserted the drain commits into the invented namespace; the scope note
answered that `ensure_index` stops unresolved indexes. Measured, the outcome
splits three ways by key depth, and neither of those was the common branch.

1. **Legacy flat mirror → wrong namespace, committed.**
   `<wal>/<prefix>/sealed/` is a tenant events dir. The tenant walk calls
   `ice_for_tenant`, which calls `for_namespace`, which `ensure_namespace`s and
   `ensure_events_table`s on the spot
   (`crates/siglake-storage/src/iceberg.rs:9933-9966`). There is no owner gate
   on the events path — `run_once_at` is called with `owner: None`
   (`crates/siglake-compactor/src/lib.rs:2346-2355`). The rows commit into
   `tenant_<prefix>` and the namespace they belong to stays empty:
   `a_misplaced_flat_restore_commits_rows_into_an_invented_namespace`.
2. **Tenant- or index-scoped mirror → walked, and refused at the index gate.**
   The restore leaves `<wal>/<prefix>/` with index-shaped children; as measured
   on this card it had no `sealed/` of its own, and `list_layout_dirs`
   enumerates a child only if it HAS one
   (`crates/siglake-wal/src/lib.rs:2188`), so `<prefix>` was not a tenant, its
   children were never walked, `ensure_index` was never reached and no
   namespace was created — segments on the volume with no commit, no
   `siglake_compactor_index_unresolved_total`, no backlog gauge and nothing in
   `orphans/`. **#4972 fixed the cause** (the restore now rebuilds the tenant
   discovery dir), so the misplacement is walked: `tenant_<prefix>` is created,
   the real tenant name is read as an index, `ensure_index` refuses it, and the
   segments wait in `sealed/` under a counted backlog.
   `a_misplaced_index_restore_is_walked_and_refused_at_the_index_gate` records
   that; the rows still do not reach the namespace they belong to, which is
   what this document is about.
3. **Mixed mirror → both**: the flat keys commit into `tenant_<prefix>` and the
   deeper ones stop at `ensure_index`.

The flat population gets a wrong-table commit. Every other population stalls at
the index gate with the rows intact but in the wrong namespace's queue. Both
are worse than the report the operator is handed.

## Why the key cannot decide

The valid key set at the mirror root is

```
<seg>.arrow                              legacy flat, default tenant
<tenant>/<seg>.arrow
<tenant>/<index>/<seg>.arrow
_active/[<tenant>[/<index>/]]<seg>.arrow.partial
<tenant>/<index>/owner                   catalog-claim drain only
```

One component up, that set shifts down by one, and the shifted depths 1 and 2
are still inside the valid set. Three measured cases show the collision is not
theoretical:

- A tenant may legitimately be called `wal-mirror` — `sanitize_tenant` allows
  `[A-Za-z0-9_-]` and reserves only `active`, `sealed`, `processing`,
  `committed`, `consumers` (`crates/siglake-core/src/tenant.rs:29-55`,
  `crates/siglake-core/src/index_config.rs:42`). Its keys and its correct
  restore are identical to the misplacement's:
  `a_tenant_named_after_the_prefix_produces_the_same_keys_and_the_same_restore`.
- The prefix is configurable and `wal-recover` is never told it: `--from` is the
  only input the command has. A mirror at `lake/m` misreads the same way under
  the name `m`: `a_custom_prefix_misreads_the_same_way_under_a_different_name`.
- A legacy flat mirror one component up is byte-identical to a per-tenant
  mirror at its root.

A blacklist of prefix-shaped names would therefore refuse a legitimate install,
still miss every prefix not on the list, and establish nothing about identity
even when it fires. Rejected, as the card directed.

## Identity evidence that already exists

| evidence | where | present when | what it proves |
|---|---|---|---|
| `_active/…<seg>.arrow.partial` | depth 1 under the root | `wal.mirror.activeIntervalSecs > 0` (default 0) | conclusive: only the active loop writes a first component `_active` with a `.partial` tail (`crates/siglake-wal/src/mirror.rs:945-948`); a sealed key never ends in `.partial` |
| `<tenant>/<index>/owner` | depth 2 under the root | catalog-claim drain has run for a managed index | conclusive: a key whose last component is `owner` exists at exactly that depth (`mirror_owner_key`, `crates/siglake-wal/src/mirror.rs:96-98`) |
| `wal_segments.segment_url` | catalog DB | a catalog URI is configured; the DB survived the PVC loss | exact: the row holds `<prefix>/<suffix>` plus the true `(tenant, index_id)` (`crates/siglake-storage/src/catalog_claim.rs:480-500`) |
| WAL frame header | every v2 segment body | since #2693 | the object IS a segment (`LWAL` magic) and names its Iceberg table UUID — but says nothing about depth, and resolving the UUID to a name needs a catalog |
| nothing | — | local drain, no managed index, active mirroring off | this is the default install |

Both store-side markers are visible at a KNOWN depth, so the same listing that
produces the plan also produces the verdict: a marker one level deeper than it
should be means `--from` is one level too high, and it names the directory the
operator meant. `the_markers_that_do_pin_the_root_sit_at_a_known_depth` records
that they exist, that recovery currently counts them as unrecognised keys
alongside a stray `README.md`, and that a bare mirror has neither.

The last row is the reason a marker check cannot be the whole remedy: the
population with no evidence is the default install, and it is the same
population as the legacy flat mirror that commits into the wrong namespace.

## Options

### A — a new marker object at the mirror root

The uploader PUTs `<prefix>/_siglake_wal_mirror` once at startup, holding the
prefix, the deployment namespace and a format version; `wal-recover` requires
it, or warns when it is absent.

Cost: one idempotent PUT per ingester start, a new object in the layout, and a
compatibility mode for every mirror written before it ships. That compatibility
mode is the problem: an absent marker has to stay acceptable for years, so the
check is advisory exactly where the evidence is missing, which is exactly the
population at risk. It also duplicates what `_active/` and `owner` already do
for the installs that have them. Out of scope for this card in any case (it
changes object layout), and not worth the layout change on its own.

### B — refuse a first component matching the configured prefix

Rejected above: the command is not told the prefix, the name is legitimate as a
tenant, and a name match is not identity.

### C — plan, then apply (recommended)

`siglake wal-recover --from URL --to WAL_ROOT` becomes a PLAN. It lists,
reconstructs the layout, prints what it would write, and writes nothing.
`--apply` performs the restore. The plan is exactly the work the command
already does before its first GET — `recover_from_object_store` collects the
whole listing into `candidates` before it reads one body
(`crates/siglake-wal/src/mirror.rs:1102-1145`) — so a plan costs one LIST and
zero segment GETs. The two invocations each pay their own LIST, which is the
one line on the bill the split adds: an apply must decide on the listing that
is current when it writes, not on the one the plan run saw.

The plan is per `(tenant, index)`: segment count, byte total, and a sample key
with the destination it reconstructs. An operator looking at

```
tenant=wal-mirror index=acme    412 segments   3.1 GiB   <wal>/wal-mirror/acme/sealed/
```

sees a tenant they do not have, before a byte is written. That is the checkpoint
the card asks for, and it is the only option that covers the evidence-free
default install.

On top of the plan, the verdict from the evidence table:

- a `.arrow.partial` under a first component `_active`, or a key ending `/owner`
  at depth 2 ⇒ **root confirmed**;
- either marker exactly one component deeper than that ⇒ **refuse**, naming the
  directory to pass instead, and `--apply` fails. There is no override: Todd
  settled the open decision below against `--force` on 2026-09-17, so the way
  past a contradicted root is to pass the directory the refusal names;
- neither present ⇒ **unverified**, and `--apply` proceeds on the operator's
  reading of the plan.

A listing holding markers at BOTH depths refuses too. A marker at root depth
is not an alibi for a misplaced one — no mirror root has both — and the
refusal reports the confirming key alongside the misplaced one.

### D — `--catalog <uri>`, as a complement to C

Given a catalog URI, look the listed segment ids up in `wal_segments` and take
the true prefix and the true `(tenant, index_id)` from `segment_url`. This is
exact where it applies, and it upgrades the verdict to definitive in both
directions. It is additive to C and should be its own slice: the DB is a second
failure domain, the ids have to be matched against a listing that may be much
larger than the ledger, and a partial match needs a rule of its own.

## Recommendation

Ship C. Take D as a follow-on slice. Do not ship A, and do not ship B.

C is the only option whose guarantee does not depend on the deployment having
opted into something. The tradeoff it accepts is an operator-contract change:
the runbook's single command becomes two, and a script that calls
`wal-recover` in the old form stops writing. That is a breaking CLI change and
it needs the docs to move with it — which is cheap next to the current
behaviour, where the same script writes segments into a namespace that does not
exist and reports success.

The alternative reading — keep writing by default and add `--dry-run` — was
considered and rejected on the card's own terms: a warning followed immediately
by writes into a live drain root is not a checkpoint, and the operator who most
needs the plan is the one who did not know to ask for it.

## How validation completes before drain-visible publication

The apply path keeps the durability contract #3149 established and adds
nothing: body to a `.tmp` sibling, fsync, rename onto the final name, fsync the
`sealed/` directory, then count
(`crates/siglake-wal/src/mirror.rs:1159-1177`). Nothing is staged elsewhere and
nothing is renamed in bulk, so there is no second ordering to get right.

Publication to the drain is the rename, and it happens only under `--apply`.
The plan run touches the WAL root not at all — including
`create_wal_dir(wal_root)`, which the current code runs first
(`crates/siglake-wal/src/mirror.rs:1087`) and a plan must not: creating the
root is itself a visible change on a volume the operator may be inspecting.
`--to` is still validated in the plan (the `sealed/` refusal at
`crates/siglake-cli/src/main.rs:2638`), because a plan that cannot be applied
should say so before the listing.

Ownership checks are unchanged and stay where they are. Recovery does not write
filesystem `owner` markers and must not start: a restored directory reads as
`WalOwner::Unmarked`, the drain stamps it for the live table, and each v2
segment is then admitted or quarantined on its own frame identity
(`retain_owned_sealed`, `crates/siglake-compactor/src/lib.rs:4002-4044`).
Synthesising a marker from a mirror key would vouch for segments on the
strength of the same key shape this document is about.

## What the implementation carried (#4973)

1. The plan/apply split, with `--apply` the only path that writes:
   `plan_recovery` and `apply_plan` in `crates/siglake-wal/src/mirror.rs`,
   one LIST per invocation.
2. The per-`(tenant, index)` plan, with counts, reconstructed destinations, a
   sample key and the skip and already-present counts #4928 added. Bytes come
   from the listing, so a store that reports no size in a listing (opendal's
   `fs` and in-memory services; S3 does report it) prints `size unknown`
   rather than a per-object stat the plan's cost claim does not allow.
3. The marker verdict, `--apply` refusing a contradicted root, and no
   `--force`. The refusal happens before `create_wal_dir`, so a contradicted
   apply leaves the volume as it found it.
4. Hermetic `file://` CLI tests in
   `crates/siglake-cli/tests/cli/wal_recover_root_identity.rs`, updated rather
   than deleted, plus unit tests for the plan and the verdict in
   `mirror.rs`. The refusals were A/B'd against the pre-change binary: both
   marker fixtures exited 0 and restored into the invented tenant before, and
   exit nonzero with `--to` never created after.
5. `docs/ARCHITECTURE.md`, `docs/LIMITATIONS.md`, the chart's DR recipe and
   the alert runbook line moved with the contract; siglake-docs #4995 carries
   the DR runbook.

## Decisions (settled 2026-09-17)

- **The plan/apply break ships in 0.2.0.** Plan by default, `--apply` the only
  writing form, no 0.1.1 default change and no `--yes` alias. The alternative
  that preserved the single command — default to writing, require `--apply`
  only when the markers contradict the root — leaves the evidence-free default
  install exactly where it is today, and that population is the whole problem.
- **No `--force`.** A refusal with no override is a support escalation the
  first time a marker is stale; an override is a flag that ends up in the
  runbook, and the escape hatch it would provide already exists — pass the
  directory the refusal names.
- **`--catalog` is a later slice**, not this release. It is the only exact
  answer, and it is the answer for the population that has a catalog but no
  markers.

## Defects found while reading this path

- **Recovery did not rebuild the tenant discovery dir, so a correct restore of
  an index-only tenant was never drained. Fixed on #4972.**
  `list_layout_dirs` enumerates a child only if it has its own `sealed/`
  (`crates/siglake-wal/src/lib.rs:2188`), and the ingester creates
  `<tenant>/sealed/` before it opens any per-index lane precisely so that
  happens — the code calls it the "tenant discovery dir"
  (`crates/siglake-ingest/src/lib.rs:571-580`). `recover_from_object_store`
  rebuilt `<tenant>/<index>/sealed/` and not that, so a mirror holding only
  index segments for a tenant — an Elasticsearch-bulk-only tenant whose events
  lane never sealed — restored into a layout the drain never walks, from the
  RIGHT `--from`, with a clean report. The restore now creates it durably,
  before the already-present skip, so re-running the command is the repair for
  a WAL root restored by the old code:
  `a_correct_restore_of_an_index_only_tenant_is_drained` and
  `an_index_only_restore_whose_index_does_not_resolve_reaches_the_index_gate`.
  This was independent of root identity, and it is what made outcome 2 above so
  quiet.

  Recovery was the only writer that could produce the state. Both ingest paths
  create the discovery dir before the lane's own directory
  (`crates/siglake-ingest/src/lib.rs:571-580`,
  `crates/siglake-ingest/src/backpressure.rs:620-626`). The compactor creates
  directories only inside one it is already draining
  (`recover_orphaned_processing`, `quarantine_stale_wal_dir`, the orphan
  sweep's `sealed/`), which it reached through the same enumeration, and
  nothing removes a `sealed/` once it exists. `siglake wal-requeue` enumerates
  with `list_tenant_dirs`/`list_index_dirs` (`wal_dirs_under`,
  `crates/siglake-cli/src/main.rs:2715-2725`) and so shares the blindness, but
  it creates nothing and cannot reach the state: a poisoned segment exists only
  where a drain ran, and a drain running is what the missing directory
  prevented.
- Recovery counts the mirror's own `owner` markers as unrecognised keys, so a
  healthy fleet mirror reports a skip count proportional to its managed index
  count. Harmless today, and noise against the signal #4928 added.
