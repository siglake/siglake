# Clipped admission after file pruning (#7515)

[#7500](CLIPPED_EQUALITY_PUSHDOWN_LAYOUTS_2026-10-26.md) found that
predicate pruning can leave the clipped admission ramp with fewer partitions.
Its `rare_clustered` layout planned four partitions instead of sixteen, opened
all four under the factor-2 ramp and was neutral against the unpruned control
despite much lower I/O. This follow-up changes only the per-session
`ClippedAdmissionWave` from 2 to 4 on #7500's four snapshots.

The result supports partition-count-sensitive widening. Factor 4 removed
12.037 ms of paired-median collect time from the four-partition clustered
layout without changing its I/O. Applying the same factor to the
sixteen-partition rare-scattered layout doubled requested and decoded data and
had mixed timing. The shipped default remains 2; no production policy, chart
value or public knob changed.

## Matched method

The fixture, SQL, pushed equality predicate, exact residual, projection,
8,192-row reader batches, range settings, target partition count and
deterministic file order are #7500's. Both arms plan against the same snapshot:

```sql
SELECT timestamp, raw
FROM events
WHERE source = 'us-east-2'
LIMIT 100
```

Each layout runs seven pairs with alternating arm order. The test asserts that
the arms have the same physical wrappers, planned partition count and planned
file identities. Every execution returns 100 distinct matching rows. Reader
caches are bypassed. Counters are read only after every scan partition settles,
so cancelled work is included.

The `admitted/opened` count is settled `files_read`. Each fixture scan
partition owns one file task, so its per-partition requested and decoded
counters give the per-file breakdown. A file with zero requested and decoded
bytes opened its footer before cancellation but did not reach data.

## Result

Times are root-stream collect medians. The difference is the median of the
seven paired `factor 4 - factor 2` differences, which is the timing comparison
used for the recommendation.

| layout | planned | factor 2 collect | factor 4 collect | paired factor 4 - factor 2 | factor 2 admitted/opened | factor 4 admitted/opened |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `interleaved` | 16 | 29.363 ms | 25.900 ms | -3.039 ms | 1 | 1 |
| `rare_scattered` | 16 | 42.934 ms | 43.070 ms | +1.938 ms | 3 | 4 |
| `rare_clustered` | 4 | 36.656 ms | 24.671 ms | **-12.037 ms** | 4 | 4 |
| `rare_clustered_single_file` | 1 | 12.278 ms | 12.165 ms | -0.485 ms | 1 | 1 |

The interleaved arms did identical work and the admission ramp never widened,
so their timing difference cannot come from the factor. The single-file
layout is the stronger unchanged-path control: pruning removes the
`ClippedLimitCoalesceExec`, all counters are equal, and the paired difference
is below half a millisecond.

### Settled bytes

| layout | factor | requested data bytes | decoded Arrow bytes | requested bytes by opened file | decoded bytes by opened file |
| --- | ---: | ---: | ---: | --- | --- |
| `interleaved` | 2 | 29,413 | 1,229,144 | `[29,413]` | `[1,229,144]` |
| | 4 | 29,413 | 1,229,144 | `[29,413]` | `[1,229,144]` |
| `rare_scattered` | 2 | 197,906 | 231,376 | two files at 98,953; 0-2 footer-only opens | two files at 115,688; 0-2 footer-only opens |
| | 4 | 395,810 | 462,752 | `[98,952, 98,952, 98,953, 98,953]` | four files at 115,688 |
| `rare_clustered` | 2 | 152,500 | 666,192 | `[38,123, 38,125, 38,126, 38,126]` | four files at 166,548 |
| | 4 | 152,500 | 666,192 | `[38,123, 38,125, 38,126, 38,126]` | four files at 166,548 |
| `rare_clustered_single_file` | 2 | 36,310 | 370,248 | `[36,310]` | `[370,248]` |
| | 4 | 36,310 | 370,248 | `[36,310]` | `[370,248]` |

Factor 4 changes the four-partition layout's schedule rather than its amount
of work. Both factors open and decode all four files, but factor 2 reaches them
through the `1 -> 2 -> 4` ramp while factor 4 reaches all four at its first
widening. Factor 4 is faster in all seven pairs: its collect times are
23.186-26.220 ms against 35.040-39.528 ms for factor 2.

The sixteen-partition rare-scattered layout shows the cost of making that a
global change. Factor 2 reads data from two files in every pair and sometimes
opens one or two more footers while cancellation lands. Factor 4 starts four
data readers in every pair, doubling requested and decoded bytes. Its paired
timing is slower in four pairs and faster in three, so the certain extra work
does not buy a repeatable collect-time improvement.

## Recommendation

Keep the configured and shipped factor at 2 for scans with more than four
planned partitions. For a future production change, cap the first widening at
all planned partitions when pruning leaves two to four: factor 2 already fills
two, while an effective factor 4 fills three or four. Keep the existing
one-partition bypass.

That rule gives the measured four-partition case its 12.037 ms saving without
paying the rare-scattered layout's twofold data and decode cost. This card does
not implement the rule. The evidence covers one equality predicate and these
local layouts, so a production patch should retain this test as its regression
and keep #7487's cancellation checks in the crate gate.

## What this does not establish

- No AWS, object-store or distributed-query measurement. This is a local
  diagnostic build over `file://` on a shared 32-core lane.
- No benefit for three planned partitions; the recommendation follows the
  measured factor-4 cap and the ramp's arithmetic.
- No range, multi-column or text predicate and no other limit or selectivity.
- No default or automatic scaling change is qualified by this investigation.

## Reproduction

```sh
cargo test -p siglake-storage \
  --test clipped_equality_pushdown_layouts \
  clipped_admission_wave_layout_attribution \
  -- --ignored --nocapture
```

The retained run completed in 218.76 seconds. The ignored test is a by-hand
measurement; deterministic assertions cover plan matching, exact answers,
settled counter totals, per-file counter reconciliation and the unchanged
single-partition path. Collect time is not a CI threshold.
