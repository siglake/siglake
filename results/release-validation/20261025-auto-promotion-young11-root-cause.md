# Young11 auto-promotion visibility investigation

Task #7335 investigated the adverse Young11 result retained for #5065. The
historical result remains failed. This source change adds attribution for a
future matched run; it does not re-grade Young11 or claim a performance repair.

## Evidence and source boundary

The immutable handoff is under
`20261019-release-repair/young11-final-handoff` in the manager investigation
store. `OUTCOME.md`, `independent-adverse-handoff-site.json`, and
`terminal.json` bind the engine to `669fad7c` and the benchmark source to
`60432dfe`. Their bound raw artifacts are under the benchmark result
`20261019-promotion-wave-successor-a11`.

The opt-in facts below come from
`promotion-in/bucket-before-destroy/controller-frozen/i-0d1f306e243a147d4/bench/auto-promotion-wave/opt-in/{execution.json,freshness.json,freshness.log,compactor.log,metrics-before.txt,metrics-after.txt}`.
The comparison arm uses the corresponding `opt-out` files below controller
`i-0702aca10c06404ff`. The reader contract and verdict are retained in
`acceptance.json` and
`independent-assessment-site/runtime-assessment-site.json` at the result root.

The historical engine commit is an ancestor of the source reviewed here. From
that engine through current `main`, the compactor's auto-promotion control flow,
the backfill selector, and its concurrent bin dispatch did not change. Current
storage changes elsewhere do not repair or invalidate this behavior. The
current compactor change before this report only added query-audit expiry work.

## What happened in the opt-in arm

The retained window was 13:25:55.586–13:31:45.586 UTC. Its first recluster pass
had started before the window and completed at 13:26:54.386 after 76.823 s. It
rewrote 15 files containing 11.21 million rows into two files. The pass's own
stage line assigned 28.2% to input, 2.0% to assembly, and 69.8% to write.

Auto-promotion sampled `logs-bench` after that pass. The next recluster pass
started at 13:27:02.902. At 13:27:08.644 it selected two promotion-backfill
bins:

- three files totaling 11,276 bytes;
- five files totaling 2,547,825,294 bytes.

The small bin committed. The large bin was still running at the end of the
measurement window. The completion-only series therefore reported three files,
1.298 s, and the corresponding small byte count even though the pass had also
admitted the 2.55 GB rewrite. The retained log has no commit or stage line for
the large bin before the window closed.

The compactor selected both bins while sealed backlog was below the default
256-segment preemption gate. Concurrent dispatch checks the gate before each
spawned bin starts. Once several bins have started, a later gate crossing does
not cancel them. Sealed pending rose from 168 to 1,663 while the drain and the
large rewrite overlapped. This establishes overlap between the growing queue and admitted rewrite work;
it does not isolate the cause of delayed visibility. The run does not identify how the unfinished bin split
time between object reads, Parquet decode, merge assembly, encode, and upload.

The SQL probe was not the delay. All 440 polls returned HTTP 200. The longest
query took 583 ms and the largest polling gap was 785 ms. The opt-in p95 was
117,840.2 ms against 21,105 ms for opt-out, or 5.58352× against the unchanged
1.25× ceiling. Opt-in completed six trials and opt-out completed 15.

## Completion-marker contract

The qualification reader requires `auto-promotion pass finished`, the exact
namespace, and the exact table on one line inside the original window. It
explicitly refuses a metrics-only substitute. The original reader correctly
returned `not_assessed`: neither its captured compactor log nor the retained
container stderr contains that line for `logs-bench` in the window.

The product did complete the promotion branch. The before/after metrics show
one `outcome="promoted"` pass and one sampling-duration observation, totaling
3.898 s. Historical source incremented that counter immediately before building
the bounded diagnostic fields and emitting the INFO event. There was no await
between the counter and event. The same container stream contains other INFO
events, and no task panic or join failure was retained. The available artifacts
therefore establish a missing product-to-capture event, but do not distinguish
an emission-format failure from loss outside the process. No reader repair is
supported by this evidence.

The marker is now a small event emitted before the completion counter. The
bounded promoted and declined key lists move to a separate details event. A
deterministic test captures the event and requires the marker, namespace,
table, and outcome on the same line. On a future run, a completion-counter
delta without the marker will identify the capture boundary rather than leave
the product branch ambiguous.

## Added measurement

Backfill measurement now separates:

- selected bins, files, and bytes;
- started bins, files, and bytes;
- the current in-flight bin count;
- the existing committed files, bytes, and duration.

The in-flight gauge uses a drop guard, so success, error, watchdog cancellation,
and task cancellation all release it. Start, commit, and stopped-before-commit
events retain the table, file count, and input bytes. Merge phase counters now
also carry table and bin kind on a new series, preserving the existing aggregate
series. The phase series is emitted only after merge output closes, before catalog
commit; unfinished merges contribute no phase sample. It aggregates by table,
bin kind, path, and stage, so concurrent backfill bins are not individually
distinguishable. It separates completed backfill merge work from ordinary
merge work without establishing the unfinished bin's phase split.

## Remaining qualification

Automatic promotion stays default-off. Thresholds 0 and 50, four files × 4,096
rows × 16 columns, the 240–590 s window, the 1.25× freshness ceiling, and every
row, physical, and oracle check are unchanged. A matched rerun is still needed
to measure the new phase series and to test any scheduling repair. Young11 does
not qualify a mature table or total S3 cost.

No scheduling change is made here. Cancelling, splitting, or serializing a
large backfill bin changes convergence and abandoned-upload cost. The retained
run identifies the admitted work and its overlap, but lacks the phase evidence
needed to select among those changes.
