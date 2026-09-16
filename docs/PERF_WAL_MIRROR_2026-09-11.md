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
