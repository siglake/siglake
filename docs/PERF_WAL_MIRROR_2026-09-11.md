# What the default-on WAL mirror costs ingest (task #2953)

**Date:** 2026-09-11 · **Harness:** `siglake ingest-server` + `siglake-loadgen`
on loopback, without a cluster or any container image · **Verdict: throughput is
unchanged within run-to-run spread; the ack path pays a consistent sub-millisecond
latency cost.** Every claim below is from a filesystem-backed object store on the
same NVMe as the WAL. Nothing here was measured on S3.

## Why measure

Task #2953 flipped `wal.mirror.enabled` to true. Todd's condition on the flip was
to report "whether there is any significant impact on write performance during
ingest": a sealed segment now gets read back off disk and PUT to the object store
on a background task, and a background task is not free.

## Method

One fresh data directory and one fresh server per arm. Pinned across every arm:
the #2950 fsync ack default, the 4096-event / 5 s WAL roll, `--warehouse-url
file://<dir>` (opendal `services::Fs`), a SQLite catalog, one tenant, the
`events` index, 16 loadgen workers, 50 events per request. The single variable is
`--wal-mirror-prefix`: unset (the new default, `wal-mirror/`) versus empty (the
opt-out).

No compactor runs in either arm, so nothing commits or deletes segments while the
measurement is in flight: `sealed/` accumulates identically in both, and the only
difference between the arms is the upload itself.

Arms alternate `off, on, off, on, …` — five pairs of each shape, so host drift
lands on both. Medians below; the per-pair deltas are what carry the small
effects, since the box is shared and one off-arm ran 7 % hot.

Two shapes:

- **Fixed rate**, 20,000 EPS for 60 s. Both arms deliver the rate, so this
  isolates latency at ~8 % of the box's ceiling.
- **Saturation**, workers flat out for 30 s. Each worker sends serially, so
  achieved EPS is the throughput ceiling.

Upload lag is read two ways: the `siglake_wal_segments_sealed_total` −
`siglake_wal_mirror_segments_total{outcome="ok"}` gap sampled every 250 ms
through the run, and the wall-clock from the last request to the moment the
uploads catch up with the seals.

## Results — fixed rate, 20,000 EPS (5 pairs)

| | mirror off | mirror on | Δ |
|---|---|---|---|
| delivered EPS | 20,008 | 20,009 | — |
| HTTP 503 / errors | 0 / 0 | 0 / 0 | — |
| client p50 | 1.53 ms | 1.80 ms | **+0.27 ms** |
| client p95 | 4.52 ms | 5.11 ms | +0.59 ms |
| client p99 | 5.40 ms | 5.80 ms | +0.40 ms |
| server mean request | 1.58 ms | 2.01 ms | +0.43 ms |
| segments sealed | 292 | 292 | — |
| objects uploaded | 0 | 292 (26 MB) | — |

Per-pair p50 deltas: +204, +395, +235, +343, +151 µs — five of five positive, so
the p50 cost is real rather than drift. It is also small in absolute terms: the
fsync dominates, and the mirror adds a fifth of a millisecond to it.

## Results — saturation (5 pairs)

| | mirror off | mirror on | Δ |
|---|---|---|---|
| achieved EPS | 249,920 | 247,901 | −0.8 % |
| client p50 | 2.049 ms | 2.125 ms | +0.08 ms |
| client p95 | 5.58 ms | 5.63 ms | +0.05 ms |
| client p99 | 7.85 ms | 8.14 ms | +0.29 ms |
| server mean request | 2.30 ms | 2.33 ms | +0.03 ms |
| segments sealed | 1,828 / 30 s | 1,814 / 30 s | — |
| uploaded | 0 | 164 MB (5.5 MB/s) | — |

Per-pair EPS deltas: +4.1k, −14.9k, −18.8k, −3.8k, +0.3k — mixed in sign, and
the off arms alone spread 248.2k–266.7k (7.5 %), wider than the −0.8 % median
difference. There is no throughput effect to report at this rate on this box.
Per-pair p50 deltas are +74, +101, +98, +86, +68 µs: the same sub-millisecond
tax as the fixed-rate shape, five of five positive.

## Upload lag

With the mirror on, the sealed-minus-uploaded gap never exceeded **one segment**
in any arm of either shape — including saturation, where the writer seals 61
segments per second. Catch-up after the last request was 22–24 ms at 20k EPS and
22–33 ms at saturation. `siglake_wal_mirror_failures_total` and
`siglake_wal_mirror_upload_abandoned_total` stayed at zero throughout.

PUT volume is one object per sealed segment: 4.9/s at 20k EPS, 61/s at
saturation, with the default 4096-event roll. That is the number an S3 bill sees,
and the operator prices it; the default `wal.mirror.activeIntervalSecs: 0` adds
none of its own.

## What this does not measure

- **S3.** A filesystem store has no network round-trip, no request signing and
  no retry. These numbers bound the local CPU and disk cost of the upload path;
  the 22 ms catch-up does not transfer to an object store whose PUT latency is
  tens of milliseconds. The upload queue is unbounded, so with S3 the backlog —
  and the ack→upload loss window with it — is set by remote PUT latency and by
  how the retry path behaves under failure. No AWS round has measured this.
- **A drain competing for the same disk.** No compactor ran, so neither arm pays
  for commit reads or `committed/` cleanup alongside the uploads.
- **Mirror retention.** Nothing claims from or reclaims the prefix here; a
  default single-replica FS drain never reads the mirror, which is the README
  limitation this flip makes reachable.
- **The active-segment mirror** (`activeIntervalSecs > 0`), off by default and
  off here.
- **Multi-tenant fan-out.** One writer, one index; per-tenant writers each hold
  their own mirror handle.

## Where the numbers went

README "Ingest path" carries the one-paragraph version;
`deploy/helm/siglake/values.yaml` under `wal.mirror` carries the operator-facing
version. The per-arm loadgen summaries and metric scrapes were written under the
run's `$TMPDIR/2953/` and are not retained.

---

# Re-measurement 2026-09-13: the durable queue pin (task #3758)

**Commit:** `a3a69a9` — #3745's merge `46d4991` plus this run's harness scripts
and nothing else in `crates/`. **Harness:** `wal-mirror-ab.sh` in the loopback
bench harness, new here; the 2026-09-11 harness was ad hoc and was not retained.
**Evidence:** its `results/wal-mirror-2026-09-13/` directory, twenty arms from
one session.
**Verdict: the pin costs 0.47 ms of synchronous seal time at saturation and
0.58 ms at 20K EPS — 0.48–0.66 ms in every one of the ten arms. At 20K EPS that
is invisible in delivered rate and shows only in the tail; at the saturation
ceiling it takes 3.2 % off throughput, and that is now measurable where the
pre-fix −0.8 % was not.**

#3745 put a hard link and a directory fsync on the seal path: `WalWriter::seal`
calls `WalMirrorHandle::enqueue` (`crates/siglake-wal/src/lib.rs:914`), which
calls `pin_segment` before the queue send (`crates/siglake-wal/src/mirror.rs:244`,
link and `sync_dir` at `:512-513`). Todd's condition on the #2953 default-on flip
was to report write-performance impact, and the tables above predate that path.

## Method

Same shapes, same pinned settings, same alternation as the Method section above
— 20,000 EPS for 60 s and workers flat out for 30 s, five `off, on` pairs each,
fresh data directory and server per arm, `file://` warehouse, SQLite catalog, 16
workers, 50 events per request, no compactor. Two additions: the harness is in
the tree, and it retains per arm the loadgen summary, the full final `/metrics`
scrape, a 250 ms sample of sealed / uploaded / queue depth, and the catch-up
wall clock. `wal-mirror-report.py <dir>`, beside it, reproduces every number
below.

Durations are histogram means (`_sum / _count`). The shipped `_seconds` buckets
start at 1 ms, so a quantile would read 1 ms for every seal and every queue wait
here.

## Results — fixed rate, 20,000 EPS (5 pairs)

| | mirror off | mirror on | Δ |
|---|---|---|---|
| delivered EPS | 20,008 | 20,008 | — |
| HTTP 503 / errors | 0 / 0 | 0 / 0 | — |
| client p50 | 1.524 ms | 1.512 ms | −0.012 ms |
| client p95 | 4.96 ms | 5.36 ms | +0.39 ms |
| client p99 | 6.07 ms | 7.09 ms | +1.02 ms |
| server mean request | 1.690 ms | 1.674 ms | −0.016 ms |
| mean seal | 3.637 ms | 4.248 ms | **+0.611 ms** |
| mean framed rename | 3.110 ms | 3.142 ms | +0.032 ms |
| mean seal outside that rename | 0.527 ms | 1.106 ms | **+0.579 ms** |
| segments sealed | 292 | 292 | — |
| objects uploaded | 0 | 292 (25.9 MB) | — |

Per-pair seal deltas: +589, +494, +611, +429, +807 µs — five of five positive,
and all of it lands outside the framed rename (+566, +502, +557, +513, +662 µs).
That difference is the pin: the rename histogram closes after the `sealed/`
fsync and before the enqueue, and the prologue it excludes (`finish` plus the
active-file fsync, 0.52 ms) is identical in both arms.

Delivered rate is unaffected because the writer has the headroom: 4.9 seals/s ×
0.58 ms is 0.3 % of a writer-second. Client p50 does not move either — only
292 of 24,016 requests carry a seal, so a per-seal cost cannot reach the median.
The tail does move, but this shape is a thin sample of it: per-pair p99 deltas
are +2,340, +360, +1,596, −572, +2,796 µs, four of five positive and spread far
wider than the seal cost. Read the saturation shape for the tail.

The 2026-09-11 table reported +0.27 ms on p50 here, five of five positive. That
does not reproduce: per-pair p50 deltas are now +193, −197, +62, +107, +6 µs
against off arms that held 20,007–20,009 EPS. The old figure looks like drift on
a busier box — the original run recorded one off arm running 7 % hot — and the
p50 claim should not survive into the README.

## Results — saturation (5 pairs)

| | mirror off | mirror on | Δ |
|---|---|---|---|
| achieved EPS | 256,789 | 248,699 | **−8,090 (−3.2 %)** |
| HTTP 503 / errors | 0 / 0 | 0 / 0 | — |
| client p50 | 2.036 ms | 2.029 ms | −0.007 ms |
| client p95 | 5.75 ms | 6.23 ms | +0.48 ms |
| client p99 | 6.55 ms | 7.04 ms | +0.49 ms |
| server mean request | 2.199 ms | 2.312 ms | +0.114 ms |
| mean seal | 3.494 ms | 3.980 ms | **+0.486 ms** |
| mean framed rename | 2.931 ms | 2.942 ms | +0.011 ms |
| mean seal outside that rename | 0.564 ms | 1.038 ms | **+0.474 ms** |
| segments sealed | 1,879 / 30 s | 1,819 / 30 s | — |
| uploaded | 0 | 164.8 MB | — |

Per-pair EPS deltas: −8,838, −8,227, −7,679, −10,373, −14,488 — five of five
negative, and each one larger than the whole 2.2 % spread among the off arms
(252,929–258,416 EPS). Per-pair p99 deltas are +416, +504, +436, +524, +768 µs,
also five of five, and per-pair seal deltas hold to +479 ± 15 µs across all ten
arms.

The throughput loss is the pin, by arithmetic: at 60.6 seals/s a 0.474 ms
synchronous cost consumes 29 ms of every writer-second, 2.9 % of it, against a
measured 3.2 %. The seal blocks the writer, so at the ceiling that time comes
straight off the rate; at 20K EPS it comes out of slack.

## Queue depth and dequeue wait

Both are `mirror.rs` metrics and both start *after* the pin
(`enqueued_at`, `:259`), so neither includes the synchronous cost above.

| | 20K EPS | saturation |
|---|---|---|
| mean `siglake_wal_mirror_queue_wait_seconds` | 0.335 ms | 0.628 ms |
| max `siglake_wal_mirror_queue_depth` (250 ms samples) | 0 | 1 |
| max sealed − uploaded | 1 segment | 1 segment |
| catch-up after last request | 20.2 ms | 20.8 ms |
| failed / abandoned uploads | 0 / 0 | 0 / 0 |

The uploader keeps up: a segment waits sub-millisecond for a worker, the queue
was never sampled above one entry, and the sealed-minus-uploaded gap stayed
within one segment at 61 seals/s — the same bound the pre-fix run reported.
Catch-up is 20–21 ms, against 22–33 ms before. The pin is paid on the seal; the
queue behind it and the rate it drains at are unchanged.

## What changed against 2026-09-11

| | 2026-09-11 | 2026-09-13 |
|---|---|---|
| saturation throughput Δ | −0.8 %, mixed sign, inside a 7.5 % off-arm spread | **−3.2 %, 5/5 negative, outside a 2.2 % spread** |
| p50 ack Δ at 20K EPS | +0.27 ms, 5/5 positive | −0.012 ms, no consistent sign |
| synchronous seal cost | not measured | **+0.47 to +0.58 ms, 10/10 arms** |
| upload lag | ≤1 segment, 22–33 ms catch-up | ≤1 segment, 20–21 ms catch-up |

The throughput line is the one that changed, and the seal measurement explains
it. This run did not build the pre-fix binary, so the −3.2 % is not a direct
A/B against #3745's parent; the attribution rests on the pin accounting for the
entire on-versus-off seal delta and for 2.9 of the 3.2 points. What the old run
could not resolve at a 7.5 % off-arm spread, a quieter box resolves at 2.2 %.

## What this does not measure

Everything the 2026-09-11 "What this does not measure" section lists still
applies unchanged — S3, a competing drain, mirror retention, the active-segment
mirror, multi-tenant fan-out — and two more:

- **The pre-fix binary.** See above: no arm ran #3745's parent.
- **A slow or unavailable store.** Every upload here completed against the local
  filesystem, so `mirror-pending/` never held more than a segment or two. The pin
  cost is per seal and does not change with store latency, but the directory the
  pins accumulate in has only been exercised near-empty.

---

# Can the pin share a directory sync? (task #3787)

**Date:** 2026-09-17 · **Evidence:** `report_pin_cost_breakdown`, added on this
branch; four runs, numbers reproducible by re-running it · **Verdict: no, and the reason is
not the crash window. The fsync is 94 % of the pin, and there is never a second
unsynced pin to share it with: one writer owns each `mirror-pending/` directory
and seals under its own lock, so the directory holds at most one unsynced entry
at any instant. Every "batched sync" is therefore a deferred sync, and the
happens-before a deferral needs is against the compactor — which on the volume
the chart renders is a different process on a different node.**

## Where the 0.47 ms goes

`pin_segment` (`crates/siglake-wal/src/mirror.rs:503`) does three things:
resolve the segment's current local name (`find_segment`, four `exists()`
probes), hard-link it into `mirror-pending/`, and fsync that directory. #3758
measured the three together and said so. `report_pin_cost_breakdown`
(`crates/siglake-wal/src/mirror.rs`, `#[ignore]`d) prices them apart: each
iteration lays a segment into `sealed/` the way `WalWriter::seal` does — body
fsynced, renamed, `sealed/` fsynced — so the journal is in the state the pin
actually meets, and then times the three steps separately.

200 seals of 92,160 B (the saturation arm's 164.8 MB over 1,819 segments), ext4
on NVMe, load average 6.9–7.7, four runs. Microseconds per pin, means:

| batch | `find_segment` | `hard_link` | `sync_dir` per pin | pin total | idle barrier |
|---|---|---|---|---|---|
| 1 | 4.9 | 21.2 | 393.8 | 419.8 | 18.5 |
| 2 | 4.6 | 20.9 | 196.1 | 221.6 | 18.1 |
| 4 | 4.6 | 21.0 | 101.1 | 126.6 | 19.7 |
| 8 | 4.9 | 21.8 | 49.7 | 76.3 | 17.7 |
| 16 | 4.6 | 21.2 | 24.8 | 50.6 | 22.4 |
| 64 | 4.7 | 21.8 | 6.1 | 32.7 | 37.2 |

Whole `pin_segment`, same runs: 410–427 µs mean, 406–423 µs p50, against the
474 µs #3758 read out of the seal histogram — the same number to within the
spread between two sessions on a shared box, and reached a different way, which
is the cross-check. The lookup and the link together are 26 µs —
6 % of it. The directory fsync is the rest, and it amortizes almost linearly:
one sync per 8 pins would cost 76 µs per seal instead of 420, and at the
measured 60.6 seals/s that returns 2.1 of the 2.9 points of writer-second the
pin consumes. The arithmetic in #3787's premise is right.

The last column is an fsync of the same directory immediately repeated, with
nothing pending: 18–37 µs by the mean, an order of magnitude under a dirty one.
It is the noisiest column here — there are only `200 / batch` samples of it per
run, and one sample at batch 64 read 131 µs, which is most of that column's
37. A barrier that fires when there is nothing to persist is still cheap enough
to ignore, which matters to the drain-side design below.

## There is no batch to form

`WalMirrorHandle::enqueue` has exactly one caller, `WalWriter::seal`
(`crates/siglake-wal/src/lib.rs:928`), and `hard_link` exactly one production
call site. `mirror-pending/` is per WAL directory (`pending_path`,
`mirror.rs:492`), and a WAL directory is one `(tenant, index)` lane, held by one
`Arc<Mutex<WalWriter>>` in `TenantWalRouter`
(`crates/siglake-ingest/src/lib.rs:343`). `seal` takes `&mut self` and calls
`enqueue` before it returns, so a lane's pin is created and synced inside its
own critical section.

61 seals/s is a rate through that serialized writer, not a concurrency. The
count of unsynced pins in one directory is one, at every instant, at any ingest
rate. Multi-tenant fan-out raises pins per second but gives each lane its own
directory, and `fsync(2)` on a directory covers that directory. Nothing else
writes into `mirror-pending/`: the catch-up sweep only removes from it.

So a batched pin is a pin whose sync is performed after the seal that created
it, by someone else. What has to be shown is not that N pins can share a sync,
but that the deferral closes before it matters.

## What the fsync is against

The segment is already durable in `sealed/` when the pin is taken: `seal`
fsyncs `sealed/` at `lib.rs:885`, 43 lines before the `enqueue` at `:928`. So
the pin is redundant to `catch_up_sweep` — which discovers candidates from
`mirror-pending/` and `sealed/` and from nowhere else (`mirror.rs:605`) — until
something durably retires the sealed name. Four things remove one:

1. `claim_segment` (`lib.rs:2269`) renames `sealed/ → processing/` for the FS
   drain and fsyncs both directories. After it, only the pin is left in the
   sweep's discovery set.
2. `quarantine_stale_wal_dir` (`lib.rs:1122`) moves a dropped incarnation's
   residents to `stale/<uuid>/` and fsyncs both. The sweep does not scan
   `stale/` either.
3. `delete_segment` from the ingester's local sweep
   (`crates/siglake-cli/src/main.rs:1558`) unlinks a sealed segment outright,
   but only one the catalog reports committed and settled for 600 s — which in
   claim mode means the mirror object was the commit source, so the upload
   succeeded and the pin is already gone.
4. `sweep_committed_coordinated` and `dispose_orphans_at` delete from
   `committed/` and `orphans/`, never from `sealed/`.

The pin's fsync therefore exists against (1) and (2) alone, and the invariant is
exact: **a pin must be durable no later than the removal of the last sealed name
it stands in for.** Both actors are the compactor, and both make that removal
durable with an fsync of `sealed/` they issue themselves.

## The crash window a deferral opens

Suppose the writer links and does not sync. The window runs from the `hard_link`
to whatever fsync eventually persists it. Three crash points:

- **Before the drain claims.** The sealed name is still there and still durable.
  The restarted ingester's first `catch_up_sweep` pass finds the segment in
  `sealed/`, PUTs it and registers it. Losing the pin dirent costs nothing.
- **After the claim rename, before the claim's fsync of `sealed/`.** The removal
  is not durable, so recovery sees the segment under its sealed name (or under
  both names, which the sweep dedups by mirror key, `mirror.rs:618`). No hole.
- **After the claim's fsync of `sealed/`.** Only here does the deferral bite.
  The segment is now discoverable only through `mirror-pending/`. If the link
  dirent did not survive, the segment is invisible to every sweep the cluster
  will ever run, the PUT never happens, and the mirror is permanently missing a
  sealed segment. Nothing reports it: `catch_up_sweep` repairs "uploaded but not
  registered" and "sealed but not uploaded", and has no notion of a segment it
  cannot see. That is #3745's bug with a smaller window, and the loss is the one
  the mirror exists to prevent — `recover_from_object_store` (`mirror.rs:1003`)
  restores a lost PVC from the prefix, and what is not in the prefix is not
  restored.

So a deferral is admissible exactly when the pin's dirent is guaranteed durable
before the drain's fsync of `sealed/` is. Two ways to guarantee that, both
refused.

### Journal ordering: not ours to rely on

On ext4 in `data=ordered` with the default journal, an fsync commits the running
transaction, and that transaction carries every earlier completed metadata
operation on the filesystem — including a hard link in another directory. The
drain's fsync of `sealed/` would persist the ingester's link for free, and the
harmful ordering above could not be produced.

That is an implementation property of one journal mode, not a guarantee POSIX
makes or that the code may assume. ext4's `fast_commit` feature logs per inode
rather than committing the whole transaction, precisely to avoid paying for
unrelated work; XFS's log has its own ordering rules. A durability argument that
holds only while nobody runs `tune2fs -O fast_commit` is not an argument, and it
would be invisible when it stopped holding — the failure is a missing object in
a DR copy nobody reads until they need it.

### A barrier in the drain: the wrong process, on the wrong volume

The explicit form: the writer links without syncing, and the compactor calls
`sync_dir(mirror-pending)` once before it makes a claim batch durable. One fsync
per batch of up to 64 segments instead of one per seal, on the compactor rather
than the writer, establishing the happens-before by construction rather than by
journal accident. The idle-barrier column says it costs 18–37 µs when there is
nothing pending, so the compactor would barely feel it.

It fails on the deployed shape. The WAL claim is `ReadWriteMany`
(`deploy/helm/siglake/values.yaml:800`), EFS on EKS, shared between the ingester
Deployment and the compactor Deployment — the chart's own comment at `:784`.
`fsync(dirfd)` in the compactor's process, on its own NFS client, on another
node, is not a statement about data the ingester's client wrote. It flushes what
that client has pending, and the ingester's link is not its to flush. The
barrier would compile, pass every local test on ext4, and mean nothing on the
volume the chart renders.

Two further costs, either of which would be enough on its own:

- It makes the ingester's mirror directory a dependency of the drain. A pin
  failure today increments `siglake_wal_mirror_failures_total{reason="pin"}`,
  logs, and does not block the seal (`mirror.rs:245`). Under the barrier an EIO
  on `mirror-pending/` has to fail the claim batch, or the barrier is not one.
- It does not cover the catalog-claim drain, which never touches the ingester's
  filesystem, or an external WAL consumer reading the same volume
  (`docs/CONSUMING_SEGMENTS.md`). Each new reader of the directory would have to
  learn the protocol.

A writer-side deferral — seal K's sync performed by seal K+1 — fails earlier.
It closes the window against the next seal, and the actor the window is against
is the compactor, which can claim K between the two seals. At 61 seals/s that
gap is 16 ms wide, and the drain's claim loop runs continuously.

## What can be given up, and what it is worth

The mirror protocol takes two more directory fsyncs per segment, both on the
unpin side: `remove_pin` after a successful upload (`mirror.rs:536`) and
`remove_candidate_pin` per swept candidate (`mirror.rs:783`). Neither carries
the invariant above — losing an unlink leaves a stale pin, the next
sweep stats the key, finds the object present, and removes it again
(`mirror.rs:646`). They are idempotent repairs of cheap work, and they could be
dropped or batched with no crash-ordering argument at all.

They are also not on the seal path — they run on the mirror worker and the sweep
task — so removing them recovers no throughput directly. What they do cost is
another ~380 µs of journal work per uploaded segment, 61 times a second at
saturation, on the same device the writer is fsyncing. That is a candidate for
the 0.3 points between the 2.9 % the pin accounts for by arithmetic and the
3.2 % #3758 measured. Unmeasured here; filed as a follow-up.

## The number this argument does not have

Every figure above is ext4 on local NVMe, and so is #3758's −3.2 %. The volume
the chart renders is EFS. NFS directory operations are server-side durable
before the reply — the protocol requires it for `LINK`, unlike `WRITE` — which
would make `pin_segment`'s `hard_link` carry the durability and its `sync_dir`
close to free, and the pin's whole measured cost an artifact of the local
filesystem the bench runs on. That would also mean the README's "0.47–0.58 ms of
synchronous seal time" is a loopback number that does not transfer to the
deployed shape. No round has measured a seal histogram on EFS; a follow-up asks
for one.

That changes the shape of this argument without changing its verdict. The two
cases arrive at the same place from opposite directions: where the fsync is
expensive (local ext4) the deferral cannot be closed safely, and where the
deferral would be unnecessary (a filesystem whose `LINK` returns durable) the
fsync is already cheap. Whether it can be skipped is a per-mount property —
which filesystem, which mount options, which server — that the writer cannot
detect and must not guess, so it pays for the weakest assumption every time.
That is the sense in which the cost is irreducible: no code in the writer can
tell when the fsync is buying nothing.

## What this does not measure

- **Any filesystem but ext4.** XFS, NFS/EFS and overlay mounts were not
  measured. The `fast_commit` claim above is a reason not to depend on journal
  ordering, not a measurement of what `fast_commit` does.
- **A non-empty `mirror-pending/`.** The measurement links into a directory that
  grows to 200 entries. An outage backlog is larger, and neither the link nor
  the fsync was priced against a directory holding thousands.
- **The batched arms end to end.** The batch columns price `sync_dir` amortized
  over N links in one process. No arm ran a writer with a deferred pin, because
  the argument above says none should ship.
- **Concurrent drain pressure.** The box ran no compactor; the sealed directory
  was never being renamed out from under the pin.

# What the unpin fsync costs, and why it stays (task #4919)

**Date:** 2026-09-18 · **Source:** `dfd0541` · **Evidence:**
`report_unpin_cost_breakdown` (`crates/siglake-wal/src/mirror.rs`,
`#[ignore]`d), three runs, plus an interleaved loopback A/B.
**Verdict: KEEP both unpin directory syncs.** One sync costs 384–448 µs on
this ext4 volume and about one journal transaction. The 20K-EPS/default-roll
shape delivered the same rate with and without it. Saturation runs were too
variable to establish a benefit at the shipped roll; one quiet 512-event-roll
block measured a 4.6 % candidate gain, but its repeat did not establish a
stable magnitude. A rolled-back unlink can also recreate an object after mirror
reclamation, so deleting or batching the barrier has a correctness cost.

#3787's follow-up said the mirror takes two more directory fsyncs per segment on
the unpin side and that both could go, because a lost unlink costs one stat the
next sweep pass repeats. The premise was wrong twice over. The two call sites are
alternatives, not a pair: `remove_pin` after a confirmed upload
(`mirror.rs:529`) and `remove_candidate_pin` per swept candidate
(`mirror.rs:809`) both unlink one pin, and a healthy writer reaches the first
once per uploaded segment and the second never — so the cost is one unpin fsync
per segment, not two. And the repair is not always a repeated stat: once the
mirror-ledger reclamation of `docs/DESIGN_wal_mirror_reclamation.md` is on, a
resurrected pin can put a collected object back.

## One unpin, priced

The same shape as #3787's pin table, turned around: 200 segments sealed the way
`WalWriter::seal` leaves them and pinned, then unpinned, `TMPDIR` on the ext4
`/home` volume, three runs. Microseconds per unpin, means:

| batch | `remove_file` | `sync_dir` per unpin | unpin total | idle barrier |
|---|---|---|---|---|
| 1 | 8.3–9.3 | 379.2–433.4 | 387.4–442.7 | 4.9–5.7 |
| 2 | 7.0–7.8 | 191.3–195.6 | 198.2–203.5 | 4.3–6.2 |
| 4 | 6.3–6.9 | 94.7–99.0 | 101.0–105.8 | 4.0–6.1 |
| 8 | 6.1–6.4 | 48.3–51.9 | 54.5–58.3 | 4.2–5.9 |
| 16 | 6.2–6.4 | 24.5–25.2 | 30.7–31.6 | 4.1–5.3 |
| 64 | 6.0–6.1 | 6.3–6.6 | 12.3–12.6 | 4.3–6.6 |
| 200 | 5.9–6.0 | 2.4 | 8.3–8.4 | 4.6–13.5 |

Whole `remove_pin`, same runs: 384–391 µs mean with the sealed name still
present, 431–448 µs when the pin is the segment's last local name and the unlink
also frees its blocks. The unlink is 6–9 µs of that — under 2.5 %. The
proportion is the pin's: the fsync is the cost, and it amortizes almost linearly,
down to 2.4 µs per unpin at one sync per 200.

At the 60.6 seals/s of #3758's saturation arm, ~390 µs per uploaded segment is
24 ms of journal work per second on the device the writer is fsyncing.
`remove_pin` runs on the mirror worker after `notify_uploaded`
(`mirror.rs:361`), and `remove_candidate_pin` runs on the sweep task. The A/B
below measures whether that concurrent work reaches delivered throughput.

## A/B method and identities

The source and candidate both enabled the mirror against a `file://` warehouse
on the same ext4 volume as the WAL. The only candidate changes removed the
`sync_dir` calls from `remove_pin` and `remove_candidate_pin`; pin creation and
every other durability barrier stayed in place. Each arm started a fresh
ingest server and data directory in the default durable acknowledgement mode,
with 16 load workers and 50 events per request. Pair order alternated. Fixed
arms ran at 20,000 EPS for 60 seconds; saturation arms ran for 30 seconds.

| artifact | identity |
|---|---|
| unchanged binary | `425b8f985c809493f28d7dbe16c7ec34461d982e34d9c31624d68f23b4f5e403` |
| no-unpin-sync candidate | `1fa201e030c27af9e1d005d007a5c605d42deacc1b126b9ec4666a3baba3fd79` |
| load generator | `fd1e098cd898aa987a2bf1e21a2d3b3ab5cc8ebd85b13d81016dfec547683250` |

The raw arm files were written under the ignored
`results/wal-mirror-unpin-2026-09-18/` directory. The identities, counts and
spread needed for the disposition are retained here. No candidate production
change remains in the tree.

## Throughput results

| roll / load | complete pairs | observed seal rate | delivered-rate result |
|---|---:|---:|---|
| 4096 / 20K EPS | 5 | 4.9/s | source and candidate 20,007–20,009 EPS; 0 errors and 0 `503`s |
| 4096 / saturation | 8 | 48.2–60.6/s | unresolved: paired deltas −19.1 % to +21.4 % |
| 1024 / saturation | 6 | 137.0–176.1/s | unresolved: paired deltas −18.3 % to +27.0 % |
| 512 / saturation | 6 | 207.3–255.5/s | candidate median +4.6 %; five pairs +3.5 % to +5.3 %, one −14.3 % |
| 512 / saturation repeat | 4 | 206.4–250.8/s | candidate arm median +0.9 %; paired deltas −16.5 % to +19.0 % |

The default-roll saturation session overlapped other work: one-minute load
average entering its arms rose from 2.1 to 16.1, and same-arm throughput spans
exceeded 21 %. The 1024-event point had the same failure mode, including one
source arm 20 % below the other source arms. Those sessions give no useful
throughput bound.

The first 512-event block was quieter. Its six source arms spanned 2.3 %, and
five candidate arms ran above their paired sources. Across those five, mean
seal time fell by 0.14 ms and mean request time by 0.24 ms; the one candidate
outlier fell 14.3 % below its source. The repeat encountered two opposite-arm
outliers and then two close pairs at +1.5 % and +0.7 %. The saved journal work
can reach ingest at roughly eight times the shipped seal rate, while the size
of that effect did not reproduce on this shared host. At the shipped roll and
20K EPS, the measured throughput effect is zero.

## The mechanism is measurable

`/proc/fs/jbd2/dm-1-8/info` was sampled around every saturation arm. This
counter is device-wide, so unrelated activity adds transactions. Normalizing
by each arm's sealed count still showed the same direction in every pair:

| roll | pairs | source transactions/seal | candidate transactions/seal | mean saved/seal |
|---|---:|---:|---:|---:|
| 4096 | 8 | 20.557–21.065 | 19.637–20.016 | 0.868 |
| 1024 | 6 | 9.327–9.467 | 8.484–8.603 | 0.830 |
| 512 | 6 | 7.417–7.434 | 6.508–6.600 | 0.898 |
| 512 repeat | 4 | 7.415–7.498 | 6.515–6.584 | 0.902 |

Final scrapes in every complete arm had
`siglake_wal_mirror_segments_total{outcome="ok"}` equal to
`siglake_wal_segments_sealed_total`. The resulting successful upload /
`remove_pin` unlink / unpin-sync counts were:

| roll / load | source | candidate |
|---|---:|---:|
| 4096 / 20K EPS | 1,460 / 1,460 / 1,460 | 1,460 / 1,460 / 0 |
| 4096 / saturation | 13,502 / 13,502 / 13,502 | 13,060 / 13,060 / 0 |
| 1024 / saturation | 29,626 / 29,626 / 29,626 | 28,778 / 28,778 / 0 |
| 512 / saturation | 43,648 / 43,648 / 43,648 | 44,209 / 44,209 / 0 |
| 512 repeat, four complete pairs | 28,518 / 28,518 / 28,518 | 28,652 / 28,652 / 0 |
| **total** | **116,754 / 116,754 / 116,754** | **116,159 / 116,159 / 0** |

`remove_candidate_pin` performed zero successful unlinks in these healthy
arms: no sweep ran and `sweep_unpin` recorded no failure. The one pin found
after each server stopped belongs to the shutdown seal emitted after the final
metrics scrape.

## Crash and reclamation disposition

Two tests cover both states a rolled-back unlink can expose. With the remote
object present, `catch_up_sweep` stats it, returns its registration and removes
the pin without a second PUT. If mirror reclamation has already deleted the
object, the same stale pin is the segment's last local name: the sweep reads
it, uploads the object again and returns a fresh `sealed` registration for a
segment already committed to Iceberg.

That second state conflicts with the reclamation order in
`DESIGN_wal_mirror_reclamation.md`: the local mark skips a live pin, then
retention deletes only after the pin is gone. An acknowledged unlink without a
directory sync can appear gone to the mark and return after a crash. One sync
at the end of a candidate sweep reduces the number of names exposed during the
pass, but a crash before that sync leaves the same state. Batching upload-side
unpins leaves each completed upload exposed until the later batch barrier.

The two current barriers stay. This accepts about one ext4 journal transaction
per uploaded segment in exchange for keeping the reclamation proof intact. A
future removal needs durable tombstone or ordering machinery that prevents a
resurrected pin from uploading a collected key; the throughput measurements
alone do not justify that machinery.

## Limits of this measurement

- The storage path was local ext4 with a `file://` object store. EFS, S3, XFS
  and overlay filesystems were not measured.
- The shared host was quiet for only part of the run. The paired spread is
  reported instead of turning noisy medians into a throughput claim.
- The 512- and 1024-event rolls are stress points. The shipped default remains
  4096 events.
- No retention worker ran during the load arms. The recovery tests construct
  the present-object and reclaimed-object states directly.
