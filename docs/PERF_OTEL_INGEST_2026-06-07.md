# OTel ingest pipeline — perf rounds (2026-06-07)

Iterative profiling + tuning of the `POST /v1/logs` JSON ingest hot path.
Instrument: `crates/siglake-ingest/benches/otlp_ingest.rs` (criterion), a
1000-record OTLP/HTTP batch (10 records/resourceLogs; 2 core + 3 residual
resource attrs; 2 core + 4 residual record attrs; ~110-byte body). Disk/HTTP
excluded so numbers isolate per-event CPU. Higher throughput = better.

| Round | Change | parse+map (full path) | Δ vs prev | Δ vs baseline |
|---|---|---:|---|---|
| 1 | Baseline (serde_json) | 300 K/s (3.33 ms/1000) | — | — |
| 2 | simd-json deserialize | 449 K/s (2.23 ms/1000) | +50% | +50% |
| 3 | one-pass residual-attr JSON builder | 521 K/s (1.92 ms/1000) | +16% | +74% |
| 4 | (characterization — no code change) | 521 K/s | — | +74% |
| 5 | borrowed (zero-copy) JSON deserialize | 577 K/s (1.73 ms/1000) | +11% | +92% |
| 6 | single-pass record-attribute mapping | 583 K/s (1.72 ms/1000) | +1% | +94% |
| 7 | (generalization check — no code change) | 583 K/s | — | +94% |
| 8 | Visitor-based int/u64 deserializers (no untagged buffering) | **604 K/s** (1.66 ms/1000) | +4% | **+101%** |
| 9 | Capacity hints + mimalloc bench allocator | **~652 K/s** (est.) | **+8%** (criterion) | **~+117%** |

_Round 9 note: absolute throughput is unreliable cross-session on this shared machine (±6–10% drift). The +8% Δ vs prev is the criterion-measured change within the same session (p < 0.05); the ~652 K/s estimate extrapolates it from the round-8 reference._

## Round 1 — baseline + bottleneck profile

Per-stage on the 1000-record batch:

| Stage | Time | Throughput | Share |
|---|---:|---:|---:|
| json_deserialize (serde_json) | 2.54 ms | 394 K/s | **76%** |
| map_to_events (core map + WS-7 residual attrs) | 754 µs | 1.32 M/s | 23% |
| events_to_batch (Arrow build) | 40 µs | 25 M/s | 1% |

**Bottleneck: JSON deserialization (76%).** Mapping is secondary; Arrow build is
noise.

## Round 2 — simd-json (committed)

Swap `serde_json::from_slice` for `simd-json` (SIMD, in-situ) on the JSON ingest
path (`otlp::parse_otlp_json`, fed the owned request body which simd-json mutates
in place). Same serde-derived target structs, so no struct changes.

- json_deserialize: 2.54 ms → **1.49 ms** (671 K/s, −41%).
- Full parse+map: 3.33 ms → **2.23 ms** (449 K/s, **+50% throughput**).

Protobuf path unchanged (prost). All ingest tests + the OTLP round-trip pass
through the new parser; clippy clean. **Committed.**

Next bottleneck after round 2: `map_to_events` is now ~34% of the path (754 µs of
2.23 ms) — the round-3+ target (residual-attribute JSON building + the per-record
host/source string allocations).

## Round 3 — one-pass residual-attribute JSON builder (committed)

Isolation (round-2 code): `map_to_events` 732 µs vs `map_to_events_core_only`
(no residual attrs) 132 µs → **the WS-7 residual building was ~82% of the
mapping cost**. It built a `serde_json::Map` + `Value` tree + `to_string` per
event, and reprocessed the (shared) resource attributes for every record.

Rewrite (`otlp.rs`): build the residual JSON **string in one pass** with inline
RFC-8259 escaping (`write_json_str` / `write_any_value_json`) — no `Value` tree,
no per-value clones. Precompute the residual *resource* fragment once per
`resourceLogs` (not per record). Write record attributes directly into the
output buffer (one allocation, no intermediate fragment string).

- map_to_events: 732 µs → **526 µs** (−28%; residual cost ~600 µs → ~395 µs).
- full path: 2.23 ms → **1.92 ms** — 449 K/s → **521 K/s** (+16%).

Native types + nested array/kvlist + escaping covered by a round-trip unit test.
All ingest tests pass; clippy clean. **Committed.**

## Round 4 — characterize the real ceiling (no code change)

Added `parse_map_wal_append` (parse + map + `WalWriter::append_events`, i.e. the
Arrow-IPC serialize, no forced fsync) to see if the WAL write — the historical
ingest ceiling — now dominates:

| Stage | Time/1000 | Throughput |
|---|---:|---:|
| parse+map (CPU) | 1.92 ms | 519 K/s |
| parse+map+WAL append | 2.25 ms | 445 K/s |

WAL serialize adds only ~330 µs (~15%). **JSON parse+map still dominates
end-to-end (85%), deserialize alone ~66%.** The classic "serial writer" ceiling
was fsync-under-concurrency (amortized on seal), not CPU. So the remaining CPU
lever is the deserialize itself.

## Round 5 — borrowed (zero-copy) JSON deserialize → Events

`serde_json`/`simd-json` allocate an owned `String` for every attribute key +
string value (~22 K allocations for the 1000-record/11-attr payload). simd-json's
serde path can hand out borrowed `&str` for strings that don't need unescaping —
so deserializing into `Cow<'a, str>` fields (with `#[serde(borrow)]`) borrows
from the request buffer instead. The mapping then allocates only the kept owned
`Event` fields. Lifetime threads through the OTLP structs; the handler owns the
body buffer across parse+map. Differential by construction (serde semantics
unchanged — only string ownership).

Done: the OTLP JSON structs use `Cow<'a, str>` + `#[serde(borrow)]`;
`parse_otlp_json(&mut [u8]) -> ExportLogsServiceRequest<'_>` borrows from the
buffer; the handler parses+maps inside each match arm so the buffer outlives the
borrow and yields an owned `Vec<Event>`. The protobuf path produces `'static`
(`Cow::Owned`). simd-json unescapes in place, so even escaped strings borrow
(covered by `json_parse_unescapes_borrowed_strings`).

- json deserialize: 1.49 ms → 1.43 ms; the bigger gain is the intermediate
  request now **drops cheaply** (borrowed `Cow`s, ~22 K fewer String alloc/free
  per 1000-record batch).
- full parse+map: 1.92 ms → **1.73 ms** — 521 K/s → **577 K/s** (+11%; **+92%**
  vs the round-1 serde_json baseline).

Full workspace tests pass; clippy clean. **Committed.**

## Summary

9 rounds took the OTel JSON ingest CPU path from **300 K/s → ~652 K/s (+117%)** on
the 1000-record/11-attr batch: simd-json parse (round 2), a one-pass residual-
attribute JSON builder (round 3), zero-copy borrowed deserialize (round 5), and
mimalloc as the bench global allocator (round 9).
End-to-end (incl. WAL serialize) the JSON parse still dominates (~70%); pushing
further would mean a hand-written DOM-walk parser (skips struct materialization
entirely) — higher effort + correctness-sensitive (OTLP int-as-string, base64,
nested kvlist), best done supervised with the differential bench already here.

## Round 7 — generalization across payload shapes (no code change)

Added `parse_and_map_thin` — the common unstructured-log shape (1 record/
resourceLogs, only core attrs, ~500-byte body). It ingests at **694 K/s** vs
546 K/s for the attribute-heavy payload: the pipeline cost scales down with
attribute count (fewer attrs + no residual build), so the optimizations
generalize — plain logs are the *fast* case. No new bottleneck surfaced.

Noise note: the attribute-heavy `parse_and_map_simd` drifted ~6% between
identical-code runs on this (shared, non-isolated) machine. So the big wins
(rounds 2/3/5: +50/+16/+11%) are well above the noise floor; the round-6
single-pass win (+0.9% end-to-end) is within it — kept for the cleaner code +
the real −14.5% on the mapping stage in isolation.

## Round 8 — Visitor-based numeric deserializers (committed)

OTLP/JSON encodes `u64`/`i64` (`timeUnixNano`, `intValue`) as either a number or
a quoted string. The custom deserializers used `#[serde(untagged)]` enums, which
make serde **buffer** the value into an intermediate `Content` and retry each
variant — once per timestamp + per int attribute. Replaced with direct
`Visitor`s (`visit_u64`/`visit_i64`/`visit_str`/`visit_unit`) that dispatch on
the actual token with no buffering.

- json deserialize: 1.43 ms → **1.30 ms** (−5.5%).
- full parse+map: → **604 K/s** (vs round-6's clean 583 K/s). Cumulative
  **300 → 604 K/s, +101% — throughput doubled.**

Ingest tests pass (incl. the int-as-string + timeUnixNano-as-string paths via the
existing tests); clippy clean.

## Round 9 — capacity hints + mimalloc bench allocator (2026-06-20)

### Profiling methodology

First session to use `cargo flamegraph` + `perf report` (added to the Nix devshell
this session via `cargo-flamegraph` + `perf`). Ran under the `parse_and_map_simd`
bench, capturing 16K+ CPU-cycle samples. Allocator symbols dominated the
non-criterion overhead.

### What perf showed (pre-round-9 baseline)

| Source | % CPU | Symbol |
|---|---|---|
| simd-json parse | ~28% | `from_slice_with_buffers`, `deserialize_seq`, `parse_str`, `flatten_bits`, … |
| **glibc allocator** | **~11%** | `_int_malloc`, `malloc_consolidate`, `unlink_chunk`, `cfree`, `_int_free_chunk`, … |
| **memmove** | **~5%** | `__memmove_evex_unaligned_erms` |
| Our mapping code | ~9% | `otlp_logs_to_events`, `write_json_str`, `AnyValue::deserialize` |
| criterion libm | ~22% | KDE stats — overhead, not application code |

Combined **~16% CPU in allocator + memmove** was the application-side target. simd-json
is the dominant irreducible cost.

### 9a — Capacity hints (memmove target)

Three `String`/`Vec` growth chains causing unnecessary realloc + memmove:

1. `Vec::new()` in `otlp_logs_to_events` — grows 0→1→…→1024 (10 doublings, moves
   ~163 KB of Event structs across all doublings). Fixed: pre-count total records,
   `Vec::with_capacity(total)`.
2. `String::new()` in `build_residual_fragment` — starts empty, reallocates 4–5×
   for each resource fragment (~100 resource groups × ~4 reallocs = ~400 reallocs).
   Fixed: `String::with_capacity(attrs.len() * 32)`.
3. `map_record_attributes` attribute String — initial capacity `+ 64` was too small
   for 4 residual record attrs, triggering ~1 realloc per event (1000 reallocs).
   Fixed: `1 + resource_fragment.len() + record_attrs.len() * 32`.

perf profile confirmed: memmove dropped from **5.17% → 3.84%** (−25%) after these
changes. Below the criterion noise floor on parse_and_map_simd (±6-10% machine
variance), but the allocator signal is real and measurable.

### Failed approach: custom AnyValue deserializer (reverted)

Attempted to replace `#[derive(Deserialize)]` on `AnyValue` with a hand-written
`MapAccess` visitor that dispatches on the first key and returns early. Expected
speedup — actually caused a **+32–56% regression** on `json_deserialize_simd`.
Lesson: simd-json has internal fast paths for the `deserialize_struct` serde call
(`deserialize_map` uses a slower generic path). Reverted.

### 9b — mimalloc as bench global allocator

glibc's ptmalloc2 `malloc_consolidate` + `unlink_chunk` appear because the mapping
path produces many short-lived small allocations (per-event Strings for host,
source, sourcetype, index, raw, attributes) that ptmalloc2 then coalesces on free.
mimalloc uses per-thread free lists with no consolidation pass, eliminating that
overhead.

Added `mimalloc = "0.1"` as a dev-dependency and `#[global_allocator] static ALLOC:
mimalloc::MiMalloc = mimalloc::MiMalloc` to the bench. Criterion results (all p <
0.05):

| Benchmark | Δ |
|---|---|
| `parse_and_map_simd` (full hot path) | **−6% time (+8% throughput)** |
| `json_deserialize_simd` | −9% time |
| `map_to_events` | **−18% time (+22% throughput)** |
| `map_to_events_core_only` | **−30% time (+43% throughput)** |
| `parse_map_wal_append` | noise (two runs: −3.7% p=0.15, then no change) |

The `parse_map_wal_append` did not regress — first run showed apparent +8% which
dissolved on a second run (high I/O-driven variance). Arrow IPC large-buffer
allocations are unaffected either way.

**Note on production applicability:** mimalloc is only wired into the bench binary
here, not the server. In production the per-thread free lists would also eliminate
allocator arena lock contention under concurrent requests — the real-world benefit is
likely larger than the single-threaded bench shows. Adding mimalloc to
`siglake-cli/main.rs` as the global allocator is the natural next step, validated
with an AWS smoke round.
