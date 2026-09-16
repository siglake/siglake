# BM25 relevance scoring (design)

*2026-07-14. Status: designed, not implemented. Track 3 of
the 2026-07-11 track plan. Note: phrase matching is already
shipped (`match_phrase` UDF — adjacency-ordered token matching in the
residual verifier, pruned via `all_terms` like any FTS UDF); the
genuine remaining gap vs the competition is scoring + a proximity slop
operator.*

## Shape of the feature

`SELECT timestamp, raw, bm25(raw, 'connection refused') AS _score
FROM logs ORDER BY _score DESC LIMIT 100` — score-ordered top-K over
the existing boolean-FTS candidate set. Boolean pruning (token/trigram
blooms) stays the recall filter; BM25 only *ranks* rows the residual
verifier already touches, so the incremental cost is arithmetic on
data the scan tokenizes anyway.

## What BM25 needs that we don't store

`score(q,d) = Σ_t IDF(t) · tf(t,d)·(k1+1) / (tf(t,d) + k1·(1−b+b·|d|/avgdl))`

- **tf(t,d), |d|**: free at match time — the residual verifier already
  tokenizes `raw` per candidate row.
- **IDF(t) = ln(1 + (N − df + 0.5)/(df + 0.5))** and **avgdl**: need
  corpus-level `df(t)` (docs containing t), `N`, and total token count.
  These are the only new stored statistics.

## Storage: per-file doc-frequency sketch (the WS-7-sibling footer blob)

Per data file, at write time (ingest drain + re-cluster rewrites, the
same two sites that write blooms today), a footer KV blob:

```
siglake.dfsketch.v1 = {
  docs: u64,                  // rows in file
  tokens_total: u64,          // Σ per-row token counts (for avgdl)
  vocab: [(token, df)] capped // top-K tokens by df, K ≈ 16–64k
}
```

- Cap + tail policy: tokens evicted from the cap are RARE by
  construction (low df), and rare ⇒ high IDF; a query term missing
  from every file's vocab gets `df ≈ 0` ⇒ max IDF. That
  over-weights genuinely-rare terms slightly — the direction users
  want — and never under-ranks a common term (common terms always
  fit the cap).
- Encoding piggybacks the existing footer-KV machinery (trigram bloom
  hex, inverted-index blob); zstd the vocab list. Expected size at
  16k vocab ≈ 200–400 KB/file; acceptable next to the index blob.
  Start K=16k, measure.

## Query time

1. **Corpus stats**: sum sketches over the serving snapshot's live
   files → `df(t)` per query term, `N`, `avgdl`. Cached per
   `(table, snapshot)` — snapshot-keyed, never TTL'd (standing cache
   invariant). Only the QUERY'S terms need summing: read each file's
   sketch once into a per-file cached map (the footer cache already
   holds the metadata); the sum is a walk over live files — same
   shape as `cached_side_aggregates`, warmed by the query-cache
   warmer.
2. **`bm25(raw, 'terms')` UDF**: binds `(IDF map, avgdl, k1=1.2,
   b=0.75)` at plan time via the same tenant/table binding hook the
   hot-cache UDTFs use; per row it tokenizes (existing tokenizer),
   counts tf, applies the formula. Returns Float64.
3. **Candidate pruning**: `WHERE match_any(raw, 'terms')` (or the
   rewrite injects it) so blooms prune files/row-groups before any
   scoring; BM25 never widens the scan.
4. **Top-K**: `ORDER BY _score DESC LIMIT k` rides DataFusion's TopK
   (no full sort; no early-stop — score isn't the storage order — but
   the candidate set post-bloom is small).
5. **Distributed**: workers score with the COORDINATOR's corpus stats
   (shipped in the shard request like the #89 snapshot pin) so scores
   are comparable across shards; coordinator merge-sorts on `_score`.
   Without shipping stats, per-shard IDF skews ranks.

## Slices

1. Sketch write + footer KV + a `bm25_stats` debug endpoint (verify
   sizes/costs on a real corpus). No query change.
2. Corpus-stats cache + `bm25()` UDF, single-pod.
3. Distributed: stats in `ShardQueryRequest`, `_score` merge.
4. Proximity slop (`match_phrase(raw, 'a b', slop=2)`) — position-list
   variant of the existing adjacency matcher; independent of BM25.

## Non-goals

Fuzzy matching (edit-distance) — different machinery (needs an FST or
n-gram distance index); revisit only on a real ask.
