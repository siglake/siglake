# On-disk format versioning

**Status:** contract, from v0.1.0.

Siglake stores its data in Parquet under an Apache Iceberg table, both of which
have their own compatibility stories. This document covers the layer *we* own:
the accelerators Siglake writes into Parquet footer key-value metadata, into
Iceberg table/snapshot properties, and into side objects. Those are our formats,
so their evolution is our problem.

## The rule

**Every siglake-owned persisted format carries a version, and a reader that does
not recognize a version ignores the artifact rather than guessing at it.**

Versions live in two places, and most formats want both:

- **In the key** — `siglake.group_counts.v1`. This is the unit of *replacement*.
  A reader that only knows `.v1` simply doesn't find `.v2` and behaves as though
  the artifact were absent; a `.v2` reader can still choose to read `.v1`. No
  coordination, no migration step.
- **In the payload** — a magic string plus a version byte. This is the unit of
  *detection*: it catches a truncated blob, a foreign blob, a blob from a
  future writer, and a key whose meaning drifted. The key alone cannot do this,
  because a key match is not proof the bytes mean what you think.

## Two classes, because the failure modes differ

Which rule bites depends on what a wrong answer costs.

**Accelerators** — group-count footers, time-bucket footers, snapshot
aggregates, layout metadata. These answer a question that could also be answered
by scanning. A misread costs latency and nothing else, so the requirement is
just that a bad payload is *detected*: return "no summary", and the caller
scans. Every such reader already treats `None` as "scan this file instead".

**Pruning artifacts** — trigram blooms, inverted indexes. These decide what
*not* to read. A misread that produces a false negative means a file or row
group is skipped, and rows silently disappear from a result: no error, no
warning, a plausible-looking answer. These **must fail closed** — an
unrecognized payload is ignored entirely (scan everything) and is never probed
on a guess.

That distinction is why the bloom payload carries its own magic and version even
though its key does too. Before v0.1.0 it carried neither, and
`TokenBloom::from_bytes` would accept essentially any byte string: changing the
hash function, the trigram definition, or the `k`/`m` sizing would have produced
confidently wrong pruning against files already on disk.

## Writer rules

- **Write exactly one version.** Dual-writing an old and new encoding defeats
  the reason most format changes happen (the compact group-count footer exists
  because the JSON one was multiple MB per file; writing both would have kept
  the cost).
- **Never rewrite files for format reasons.** A table converges to the current
  format as compaction rewrites files for its own reasons. Old files keep
  working — or, at worst, stop being accelerated — until then.
- **Bump the version for any change to bytes or meaning.** Including changes
  that are "compatible": a different hash function or tokenizer produces a
  structurally valid artifact with different semantics, which is exactly the
  case a version byte exists to catch.

## The registry

| Artifact | Key | Version | Class | Unknown version ⇒ |
|---|---|---|---|---|
| Group-count footer | `siglake.group_counts.v1` | key + `LGCF`\|ver | accelerator | scan |
| Time-bucket footer | `siglake.time_buckets.v1` | key | accelerator | scan |
| File layout metadata | `siglake.layout.v1` | key | accelerator | ignore |
| Per-file trigram bloom | `siglake.raw_trigram_bloom.v1` | key + `LKBF`\|ver | **pruning** | **scan (no prune)** |
| Per-row-group trigram blooms | `siglake.raw_trigram_rowgroup_blooms.v1` | key + `LKBL`\|ver | **pruning** | **scan (no prune)** |
| Inverted index (footer/Puffin) | `siglake.inverted_index.v1[.<col>]` | key | **pruning** | **scan (no prune)** |
| Doc mapping | `siglake.doc_mapping.v1` | key | config | reject |
| Promoted columns | `siglake.promoted.v1` | key | config | reject |
| Promotion backfill marker | `siglake.promotion_backfill_complete.v1` | key | config | reject |
| Snapshot aggregates side object | `metadata/siglake-agg/<table-uuid>/siglake-aggregates.json` | JSON, additive | accelerator | scan |
| Group-count delta | `metadata/siglake-agg/<table-uuid>/siglake-agg-deltas/<seq>.json` | JSON, additive | accelerator | scan |
| Folded wide group counts | `metadata/siglake-agg/<table-uuid>/siglake-agg-wide.json` | JSON, additive | accelerator | scan |
| WAL segment frame | — | `LWAL`\|ver | data | **reject frame** |

Deliberately **unversioned**, and why: `siglake.consumed_segments`,
`siglake.retention_cutoff`, `siglake.text_tokenizer`,
`siglake.legacy_sort_order_id`, `siglake.delete_task_*`. These are scalar or
delimited-list *state*, not encodings — there is no parse to get wrong, and a
future change would introduce a new key regardless. `consumed_segments` in
particular is load-bearing for the WAL-buffer transition invariant; renaming it
would make a table's history invisible to the buffer for no format benefit.

The three aggregate objects carry no identity field of their own: the
`<table-uuid>` component of their path is what binds them to one table
incarnation (#2919). An object at the pre-#2919 flat path
(`metadata/siglake-aggregates.json` and siblings) is therefore not readable by
any incarnation — its name is shared by every table that has held the index id,
and a name proves nothing — so it reads as "unreadable" per the table above and
degrades to a scan. Nothing deletes those objects.

The Iceberg **schema** version (`EVENTS_SCHEMA_VERSION`) is a different concept
and is not covered here: it counts additive schema evolutions and has its own
migration path (`siglake migrate-schema`).

## Why everything is v1 at launch

v0.1.0 has no installed base, so it ships **no compatibility shims**. The
group-count footer briefly had a JSON encoding and a compact one with a fallback
between them; the JSON encoding never shipped publicly, so it and its fallback
were deleted rather than carried forever. Two dead pre-trigram bloom keys
(`siglake.raw_token_bloom`, `siglake.raw_token_rowgroup_blooms`) were removed
the same way.

The next format change is the first one that has to be compatible with anything.
