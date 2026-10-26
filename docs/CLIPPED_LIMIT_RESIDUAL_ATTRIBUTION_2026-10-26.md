# Residual clipped-`LIMIT` latency in the e136 matched captures (#7446)

Source-side reading of the retained 2026-10-26 field round. No run was made for
this document and none is authorised by it; everything below comes from frozen
artifacts and from the current tree at `e8e8f44`.

Artifacts, read-only:

- `…/controller-coalescer-a2/partial-phase-readout-a1/readout.json`
- `…/controller-coalescer-a2/partial-phase-results-a1/policy/*/absolute/stdout`
- `…/field-coalescer-a2/images.json`, `…/indexed/run.json`
- `…/field-coalescer-a2/indexed/bucket-before-destroy/controller-frozen/i-00b2114a66deea360/`
  — `{restored-original,matched-baseline,matched-candidate}/` and the three
  `siglake-query-server-*-stderr.log` captures

(`…` is the retained investigation directory
`20261023-engine-7106-qualification-7204`.)

Node: `m6i.4xlarge`, 16 CPUs, 4 GiB query pod, result cache off,
`TF_VAR_peer_count=2`. Table: 15 live files, 98,466,115 rows.

## The round ran two builds, not three

`restored-original` and `matched-candidate` both ran image digest
`sha256:b74cc8dc…` — the candidate, `e136f22`. `matched-baseline` ran
`sha256:93b21ca…`, build `5fc9b6a`. The per-phase `siglake-query-config.json`
`image` field and `images.json` agree, and `run.json` says so directly:
"fresh e136 original/candidate and unchanged baseline".

So the readout's `original` column is a repeat of the candidate binary, taken
before the baseline phase rather than after it. Read that way it is a
same-binary repeatability band for this node and layout:

| shape | original (e136) | candidate (e136) | spread | baseline (5fc9b6a) |
| --- | --- | --- | --- | --- |
| keyword | 20.05 | 19.64 | 0.41 | 23.01 |
| keyword_last25 | 25.08 | 23.11 | 1.97 | 26.14 |
| keyword_last5 | 24.70 | 23.18 | 1.52 | 26.78 |
| keyword_and_label | 85.18 | 85.63 | 0.45 | 86.32 |
| substring_scan | 13.18 | 12.08 | 1.10 | 15.67 |

The candidate−baseline differences (3.37, 3.03, 3.60, 0.69, 3.59 ms) are of the
same order as the same-binary spread on two of the five rows.

## The phase difference is in planning, not in the scan

`siglake-core.json` and `siglake-edge.json` carry the phase split the harness
measured on the graded executions. Across the three phases the scan is flat:

| shape | phase | rows_scanned | plan µs | collect µs | render µs | server p50 µs |
| --- | --- | --- | --- | --- | --- | --- |
| keyword | original | 395,609 | 1,228 | 14,754 | 103 | 19,273 |
| keyword | baseline | 393,970 | 1,117 | 14,725 | 96 | 22,278 |
| keyword | candidate | 395,404 | 1,118 | 14,990 | 93 | 18,850 |
| substring | original | 372,262 | 1,149 | 8,231 | 95 | 12,446 |
| substring | baseline | 381,068 | 1,111 | 8,202 | 96 | 14,944 |
| substring | candidate | 383,116 | 1,090 | 7,956 | 94 | 11,399 |

`collect` is the only phase the clipped coalescer can touch, and it does not
move. The server-side difference sits outside it.

The query-server log resolves it. Medians over the five measured executions of
each shape, from the `sql query profile` and `sql execution profile` records:

| shape | phase | register | logical | **estimate** | physical | collect | total |
| --- | --- | --- | --- | --- | --- | --- | --- |
| keyword | original | 0.623 | 0.138 | **0.118** | 1.149 | 14.403 | 17.081 |
| keyword | baseline | 0.572 | 0.140 | **1.781** | 1.168 | 15.255 | 19.961 |
| keyword | candidate | 0.608 | 0.129 | **0.114** | 1.246 | 14.808 | 17.988 |
| substring | original | 0.585 | 0.118 | **0.127** | 1.057 | 7.485 | 10.429 |
| substring | baseline | 0.547 | 0.115 | **1.839** | 1.060 | 7.860 | 11.862 |
| substring | candidate | 0.612 | 0.118 | **0.113** | 1.054 | 7.587 | 10.500 |

(ms). `estimate_ms` is `cost::estimate` (`crates/siglake-query-server/src/sql.rs:4958`
→ `crates/siglake-query-server/src/cost.rs:138`). It is 1.67 ms higher on the
baseline image for both shapes and accounts for most of the measured
candidate−baseline difference.

`5fc9b6a..e136f22` is 54 commits. Commit `1c72767` in that range — task #7100 —
replaced `ice.list_indexes()`, which enumerates the namespace and does an
uncached `load_table` per managed index on every estimate, with
`ice.cached_index_config(table)` (`cost.rs:166-184`). That is the change the
1.67 ms step matches.

The #7341 coalescer repair is not visible in this comparison. The partition
profiles say the same: the per-partition first-batch staircase and elapsed
times for the `keyword` executions are indistinguishable between the two
builds.

| build | first_batch_ms, 15 partitions sorted |
| --- | --- |
| baseline `5fc9b6a` | 2.07 3.94 4.00 4.27 4.30 4.41 4.70 5.11 5.30 5.76 6.42 6.54 6.69 6.69 7.29 |
| candidate `e136f22` | 1.99 3.75 3.91 3.92 4.27 4.32 4.41 4.48 4.52 6.20 6.21 6.24 6.30 6.33 7.00 |

## Where the failing rows spend their time

Candidate phase, graded p50s, from `siglake-core.json` / `siglake-edge.json`:

| | keyword (19.64 ms, ceiling 10) | substring (12.08 ms, ceiling 10) |
| --- | --- | --- |
| physical plan | 1.12 | 1.09 |
| buffer delta | 0.07 | 0.07 |
| collect | 14.99 | 7.96 |
| render | 0.09 | 0.09 |
| accounted | 16.27 | 9.21 |
| server p50 | 18.85 | 11.40 |
| **unattributed** | **2.58** | **2.19** |

Of the unattributed remainder the log names `register` 0.61, `logical` 0.13 and
`estimate` 0.11 — about 0.85 ms. The rest, 1.3–1.7 ms, is covered by no phase
counter: the server's own `total_ms` (17.99 / 10.50) is already below the
`server_micros` the client records (18.85 / 11.40).

> Corrected by #7456. That last sentence reads the remainder as ONE term
> sitting above `total_ms`; it is two. 0.87 ms (keyword) and 0.45 ms
> (substring) are inside `total_ms`, in handler phases that had no timer —
> response serialization among them, which runs before the terminal line.
> The 0.86 / 0.90 ms above `total_ms` is the server header's wider span, which
> also covers reading the request body off the socket.
> `docs/SQL_PHASE_ACCOUNTING_2026-10-26.md` has the boundaries and the
> per-request measurement.

So roughly 3.7 ms per request on both shapes is spent outside the scan. Against
a 10 ms ceiling, a scan reduced to zero still leaves substring at about 3.7 ms
and no margin for the decode the shape actually needs.

## The ramp delays partitions; it does not stop them

`siglake_query_scan_clipped_admission_total{outcome="queued"} 868` against
`{outcome="immediate"} 77` in the candidate scrape. Every queued partition was
admitted: all 15 partition profiles of every `keyword` execution report nonzero
`output_rows`.

The candidate `keyword` execution 306 reads as follows. `stream_build_ms` is
0.012–0.046 everywhere, so the staircase is admission, not setup:

| partition | planned rows | first batch | batches | rows | decoded | elapsed | max gap |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 7,071,994 | 2.03 | 77 | 78,848 | 5,853,228 | 14.41 | 7.12 |
| 1 | 7,071,336 | 3.73 | 33 | 33,792 | 2,372,188 | 13.63 | 7.35 |
| 2 | 7,071,305 | 3.92 | 50 | 51,200 | 3,613,432 | 13.05 | 6.48 |
| 3–7 | ~7.07M each | 4.41–6.02 | 18–27 | 18k–28k | 1.3–1.9 MB | 13.0–13.3 | 2.9–7.0 |
| 8–13 | ~7.06M each | 5.51–6.77 | 16–18 | 16k–18k | 1.1–1.4 MB | 13.0–13.1 | 4.5–6.8 |
| 14 | 8,332 | 4.47 | 9 | 8,332 | 628,540 | **4.71** | 0.04 |

1 → 2 → 4 → 8 → 15 is the `wave = 2` ramp (`ScanAdmission`,
`query_provider.rs:5026`). It reaches full width by 6.8 ms and the 14 whole-file
partitions then run to ~13 ms, when the drop unwinds them together. 396,428
source rows are decoded to return 100. Partition 14 — the 8,332-row file — is
the only one that ends on its own, which is the same small-partition effect run
#90 had and this layout does not.

Multi-ms `max_batch_gap_ms` on every whole-file partition shows the bounded
channel is applying backpressure as #7341 intended. It bounds how far each
partition runs ahead; it does not change how many run.

## `match_terms` costs 62× its `LIKE` control per row

The two failing shapes read comparable row counts and comparable bytes from the
same files, and differ almost entirely in predicate cost. Medians over the five
measured candidate executions:

| predicate | leaf rows | df_elapsed_compute | collect |
| --- | --- | --- | --- |
| `raw LIKE '%checkout%'` | 381,068 | 1.28 ms | 7.59 ms |
| `match_terms(raw, 'queen')` | 396,428 | 79.53 ms | 14.81 ms |

79.53 ms of CPU across 16 partitions is the 7.2 ms of extra wall time. Per row
that is about 200 ns.

`match_raw` (`crates/siglake-query-server/src/udfs.rs:278`) calls
`Tokenizer::tokenize` on every cell. `default_tokenize`
(`crates/siglake-bloom/src/lib.rs:120`) returns a `Vec<String>`, allocating one
lowercased `String` per token up to `MAX_TOKENS_PER_ROW`; `MatchMode::AllTerms`
then builds a `HashSet<&str>` over that vector and tests the query terms
against it. For a single-term query over a ~70-byte log line that is tens of
small allocations and a hash table per row, to answer whether any token equals
`queen`. Nothing in the path short-circuits on the first hit and nothing reuses
a buffer across rows.

This is the largest single source-side term in the keyword shapes, and it is
not the whole breach: substring has no predicate cost worth naming and is still
12.08 ms.

## The pushdown decode path is larger again

Outside the two failing rows, the same round retained a cleaner control for the
decode side:

| query | leaf rows | df_elapsed_compute | collect |
| --- | --- | --- | --- |
| `WHERE raw LIKE '%checkout%' LIMIT 100` | 381,068 | 1.28 ms | 7.59 ms |
| `WHERE region = 'us-east-2' LIMIT 100` | 351,405 | 0.57 ms | 73.56 ms |

The second has almost no filter cost and a smaller row count, and takes ten
times as long to collect. Its plan pushes `predicate:[region = "us-east-2"]`
into the scan, where the first does not. `keyword_and_label` carries the same
pushdown and sits at 85.63 ms against a 100 ms ceiling, so this cost is inside
the round's passing rows rather than its failing ones. It is unexplained here.

## The 8,292-row term is summed operator throughput

The raw candidate records correct the earlier arithmetic premise. They include
the final limit's 100 rows once, then 8,292 for each contributing partition:

| execution | `df_output_rows` | `leaf_rows_scanned` | difference |
| ---: | ---: | ---: | ---: |
| 336 | 403,205 | 361,645 | 41,560 = 100 + 5 × 8,292 |
| 338 | 426,457 | 409,773 | 16,684 = 100 + 2 × 8,292 |
| 346 | 373,109 | 364,717 | 8,392 = 100 + 1 × 8,292 |

These are `sql execution profile` records in the retained candidate
`query-server.log`. The prior claim that the raw difference was an exact
multiple of 8,292 omitted the root's 100 rows.

`clipped_limit_output_metrics_separate_runahead_from_emitted_rows` reproduces
the retained four-node shape over fifteen local files and reads each node's
per-partition `output_rows` directly. A focused execution had two contributing
partitions:

| node | per-partition rows | aggregate rows |
| --- | --- | ---: |
| `ClippedLimitCoalesceExec` | partition 0: 100 | 100 |
| `FilterExec: fetch=100` | two partitions: 100 each | 200 |
| `RepartitionExec: RoundRobinBatch(16)` | the same two: 8,192 each | 16,384 |
| `SiglakeIcebergTableScan` | 15 partitions: 16,384–20,000 | 276,736 |

The two non-leaf partitions therefore add
`2 × (8,192 + 100)`, and the final limit adds 100 once. DataFusion's
repartition receiver coalesces its downstream output to the configured 8,192
row batch size. Each filter partition evaluates that 8,192-row batch, applies
its own pushed fetch, and emits 100. The root accepts one of those filtered
batches, slices it to its global remaining fetch, drops the coalesced input and
emits 100. The other filtered batch can win the sender race before that drop;
it is operator throughput rather than another result batch.

The settled scan's 276,736 rows show upstream work that had already run before
the cancellation reached its producers. Those leaf rows were emitted by the
scan; they are separate from the 16,384 rows the repartition emitted to polled
filter partitions, the 16,384 rows those filters evaluated, and the 200 rows
the filters emitted. `df_output_rows` adds all four stages, so subtracting its
leaf term does not measure unique rows, predicate evaluations or result rows.

The fixture finds no per-partition fetch violation. No LIMIT-termination repair
follows from the 8,292 pattern; the existing admission, coalescer and release
thresholds stay unchanged.

## Not established

- That the ceilings are reachable on this layout. The 10 ms figures come from
  #4329's scanned-path measurements (keyword 6.1 ms, substring 4.5 ms on
  14 × 7.34M rows); this round's fixed non-scan overhead alone is ~3.7 ms.
- ~~What the 1.3–1.7 ms of server time below `total_ms` and above the recorded
  phases is spent on.~~ Answered by #7456, which also corrects the boundary
  this bullet assumed: it is two terms, not one. About 0.87 ms (keyword) and
  0.45 ms (substring) is INSIDE `total_ms`, in phases that had no timer —
  rewrite, result-cache probe, session build, plan-stats summarize and response
  serialization, which runs before the terminal line rather than after it. The
  remaining 0.86 / 0.90 ms is the header's own wider span, which includes
  request-body receipt off the socket. See
  `docs/SQL_PHASE_ACCOUNTING_2026-10-26.md`.
- Any AWS latency a source change would produce. Source fixtures do not
  establish it and no run was made.

## What a repair would have to carry

Each candidate below needs a control that holds the scheduler and wrapper
settings fixed and fails on the old behaviour, per #7446:

1. **`match_raw` allocation.** A non-allocating token walk with short-circuit
   on the first hit. Oracle: a local CPU measurement against the retained
   62× `LIKE` ratio, plus exact-result controls over the existing UDF tests.
   Bounded by substring's 12.08 ms — it cannot close the keyword row alone.
2. **Fixed per-request overhead.** Named by #7456 — see
   `docs/SQL_PHASE_ACCOUNTING_2026-10-26.md`. The non-scan time that a repair
   could act on is planning (`physical_plan` + `logical` + `estimate`), not the
   accounting residual.
3. **Pushdown decode.** The `region` control above, on its own card.

None of these is argued from this document to close #7106's qualification. The
matched field round remains the acceptance.
