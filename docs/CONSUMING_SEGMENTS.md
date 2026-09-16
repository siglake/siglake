# Consuming siglake segments

siglake writes every accepted event to a write-ahead log before anything else
touches it, and the compactor drains that log into Iceberg. This document
describes the supported way for a **separate process** to read the same stream —
a detector, a router, a mirror into another system, an audit trail.

It is a real interface with tests behind it, not an invitation to read the
directory yourself. The parts that look trivial are the parts that are easy to
get wrong: a segment is renamed underneath you mid-read, retention deletes what
you have not processed, a restart re-reads a day of data, a truncated segment
decodes to fewer rows than it holds.

## Where this fits

| | reads | latency | you get |
|---|---|---|---|
| **`siglake_wal::consumer`** | the WAL | seconds (seal cadence) | every accepted event, before commit |
| `siglake subscribe` | an Iceberg table | commit cadence | committed rows, queryable, deduped |
| `POST /api/v1/sql` | an Iceberg table | on demand | anything SQL can express |

Use the WAL consumer when you want events **early** and **exactly as accepted**.
Use the table paths when you want the queryable, compacted view.

## The interface

```rust
use siglake_wal::consumer::SegmentConsumer;

let mut c = SegmentConsumer::open("my-detector", "/siglake/wal/default", "/var/lib/mine")?;
loop {
    for seg in c.poll()? {
        for batch in c.read(&seg)? {
            process(batch)?;              // your work
        }
        c.commit(&seg)?;                  // AFTER processing
    }
    std::thread::sleep(std::time::Duration::from_secs(1));
}
```

Four calls, and the order of the last two is the whole contract.

- **`open(consumer_id, wal_dir, state_dir)`** — `consumer_id` identifies you to
  the retention sweep and must be **stable across restarts**; a changing id
  looks like a brand-new consumer while the old one's watermark goes stale.
  Distinct replicas need distinct ids. `state_dir` holds your cursor and must
  survive restarts.
- **`poll()`** — segments you have not committed, oldest first. A directory
  listing, not a read; cheap to call in a loop.
- **`read(&seg)`** — Arrow `RecordBatch`es, CRC-verified.
- **`commit(&seg)`** — advances the durable cursor **and** publishes the
  retention watermark. The cursor is `fsync(2)`ed before the call returns (the
  temp file, then the state directory), so a power loss resumes from the
  position your last commit reported and not from an older one.

## What you can rely on

**At-least-once delivery.** `commit` runs after you have processed a segment, so
a crash in between re-delivers it. If you must not double-count, key your
idempotence on `SegmentRef::name` — it is stable and unique.

**Ordering.** Segment names embed a UUIDv7, so lexicographic order is time
order. Segments arrive oldest-first and the cursor is simply the last name
processed. Events *within* a segment are in arrival order.

**Retention waits for you.** Committing publishes a watermark that the
compactor's sweep reads, so a committed segment is not deleted until every live
consumer has passed it. This is what makes lossless consumption possible at all.

**Reads survive the compactor.** A segment moves `sealed/` → `processing/` →
`committed/` while you hold a path to it. Listing spans all three and `read`
retries by name, so a rename mid-cycle neither loses a segment nor yields it
twice.

**Integrity.** Segments are CRC-framed. A truncated or corrupted segment is an
error, never silently short data.

## What you must handle

**You can still fall behind.** The watermark holds retention open, but not
forever — a hard ceiling applies, and a watermark that stops moving is treated
as absent after a stale window. This is deliberate: without it one stuck
consumer would fill an ingester's disk. So the guarantee degrades to "you missed
some", not "the cluster fell over". **Alert on your own lag.** If `read` returns
an error saying a segment is no longer present, that is what happened.

**One consumer, one directory.** siglake's layout is `<wal>/<tenant>/` and, for
user indexes, `<wal>/<tenant>/<index>/`. Use `list_tenant_dirs` and
`list_index_dirs` to enumerate them and run a consumer per directory.

**Sharding is yours.** To spread work across replicas, give each a distinct
`consumer_id` and have it drop the records it does not own. Every replica reads
every segment — the WAL is not key-partitioned on disk.

**A segment you cannot process.** Do not commit it. Committing skips it
permanently and silently; not committing means it is re-delivered next cycle.
If it is genuinely poison, record that yourself and commit past it deliberately.

## Tailing a table instead: the subscription cursor

`siglake subscribe` (and `IcebergSubscription` behind it) tails committed rows
from an Iceberg table rather than the WAL. Its cursor is a **pair** — the last
event time seen and the Iceberg `current_snapshot_id` last acknowledged — and a
consumer that persists both resumes exactly where it stopped. Persist both or
neither: a time cursor alone re-bootstraps with a full scan and drops
late-arriving rows older than it.

**Delivery is incremental over snapshots.** Each poll walks
`parent_snapshot_id` links back from the table's current snapshot to your
cursor's snapshot and reads only the data files those commits added. That is
what makes late-arriving rows (committed after your cursor, timestamped before
it) show up at all.

The first, time-filtered bootstrap poll also binds its scan and acknowledgement
to one captured table generation. A commit that lands after capture is not
included in that scan or acknowledged by it; the next snapshot-incremental poll
delivers the commit, including rows older than the time cursor. A failed
bootstrap read advances neither half of the cursor.

**Compaction does not re-deliver rows.** A poll interval routinely contains
commits that add data files full of rows you already have: the re-clustering
compactor merges small files, retention drops old ones, a delete task rewrites a
file without the deleted rows. Every one of those is an Iceberg `overwrite`
commit, and its replacement files are `ADDED` in the manifest exactly like an
append's are. The subscription classifies each commit in range by its summary
before reading anything — `append` commits deliver their files, everything else
is skipped and the cursor advances past it. So compaction with no ingest behind
it yields zero rows, and an interval holding both an append and a re-cluster
yields exactly the appended rows (late-arriving ones included). Skips are
counted by `siglake_subscription_rewrite_commits_skipped_total{table,origin}` —
in *your* recorder, if you installed one; see "Subscription metrics are
library-only" below.

**External overwrite semantics are not supported.** siglake stamps its own
rewrites with the `siglake.rewrite` snapshot-summary property
(`origin="siglake"`), and none of them adds a row a subscription has not already
been offered. If *another* engine commits a non-append to a siglake table — a
Spark `INSERT OVERWRITE`, a `MERGE`, a row-level delete — the subscription skips
it the same way, counts it with `origin="foreign"` and logs a WARN: any rows
that commit genuinely added are **not delivered**, because nothing in the
manifest distinguishes a replacement file from a new-row file. Recover those
rows by querying the interval (`POST /api/v1/sql`), as with a history gap, or
write through siglake's ingest path instead. If you are an embedded consumer
exporting the counter, alert on the `origin="foreign"` series; under the
`siglake subscribe` CLI the WARN line is the whole signal. (Re-clusters siglake
committed before the marker existed land in the same bucket; the treatment is
identical.)

**Subscription metrics are library-only.** `IcebergSubscription` increments
these counters through the `metrics` facade, which records nothing until a
process installs a recorder. Only `siglake ingest-server` and `siglake
compactor` do that, and neither runs a subscription; the shipped `siglake
subscribe` CLI installs no recorder and serves no `/metrics`, so no siglake
process exports `siglake_subscription_rewrite_commits_skipped_total` or
`siglake_subscription_history_gap_total` and no shipped chart rule alerts on
them. They are there for a consumer that embeds the crate and installs its own
recorder and exporter — every alerting suggestion on this page means "in your
own monitoring". If you run the CLI instead, its operator signals are the WARN
above and a poll that fails outright.

**Snapshot expiry can break that chain.** siglake's compactor drops old
snapshots from table metadata on a timer — `SIGLAKE_SNAPSHOT_RETAIN_LAST`
(default 100), swept every 60s — because the `snapshots` array is re-read on
every commit. If you are down for more than `retain_last` commits, the ancestors
between your cursor and the current snapshot can be gone. Those commits cannot
be enumerated, so `poll()` **refuses**: it returns a `HistoryGap` error
(downcastable from `anyhow`) and advances *neither* cursor. It does not deliver
the reachable suffix, because doing so would move the cursor past commits you
never saw — routine retention silently turning into data loss.

Your cursor's *own* snapshot expiring is not a gap: a retained child whose
parent link names it proves the chain is intact, and delivery continues
normally. Only a hole *between* the two ends stops the subscription.

**Recovering from a gap.** The interval is genuinely unrecoverable from table
metadata, so pick deliberately — the subscription will keep failing until you
do:

- **Re-bootstrap** with a fresh subscription (`IcebergSubscription::new`, or
  `resume` with `snapshot_id: None`, or `siglake subscribe --since`). The next
  poll is a full scan filtered to `time_column > cursor`, so you get the missed
  rows whose timestamps are newer than your cursor — and permanently miss any
  late-arriving row inside the gap that was timestamped *before* it.
- **Backfill by query.** `POST /api/v1/sql` over the interval you missed is the
  complete answer, and the only one that recovers late-arriving rows. Then
  resume from the current snapshot id.
- **Prevent it.** Keep `SIGLAKE_SNAPSHOT_RETAIN_LAST` comfortably above the
  commits your longest expected consumer outage spans (compaction and
  reclustering commits count too), and alert on the
  `siglake_subscription_history_gap_total` counter — it increments once per
  refused poll, per table. Same caveat as above: that counter only exists in a
  process that installed its own recorder and exporter, which the `siglake
  subscribe` CLI does not; there a gap surfaces as the command failing with the
  `HistoryGap` error.

The WAL consumer has the stronger property here: its watermark holds retention
open (bounded, see above), while a table subscription publishes nothing that
snapshot expiry consults. If losing an interval is unacceptable, consume the
WAL.

## Deployment

The consumer needs read access to the WAL directory. In Kubernetes that means
mounting the same volume the ingester writes to:

- The chart's `wal.persistence` claim must be **ReadWriteMany** (EFS or similar)
  for any pod other than the ingester to mount it.
- Mount it **read-only** except for `<wal>/consumers/`, which is where
  watermarks are written. In practice mount read-write and rely on the consumer
  only writing there.
- Give the consumer its own small **ReadWriteOnce** volume for `state_dir`.

Nothing about a consumer is privileged: it is an ordinary pod with a volume
mount.

## Building against siglake

`siglake-wal` is a normal Rust crate. Note that if you also link
`siglake-storage` — e.g. to write results into Iceberg tables — you must patch
the `iceberg` crates the same way siglake does, because siglake builds against
**vendored forks** under `third_party/`:

```toml
[patch.crates-io]
iceberg = { path = "../siglake/third_party/iceberg" }
iceberg-catalog-sql = { path = "../siglake/third_party/iceberg-catalog-sql" }
iceberg-storage-opendal = { path = "../siglake/third_party/iceberg-storage-opendal" }
```

Without this you get the upstream crates and a wall of unresolved imports.

## This interface is not speculative

A four-tier semantic detection pipeline — streaming detectors, episode
correlation, webhook dispatch — shipped *inside* siglake until 2026-08-29. It
was moved out and now runs entirely on top of this interface, consuming the WAL
through these four calls and nothing else.

That pipeline is maintained as the reference consumer on purpose: if the
interface is not enough to build it, the interface is wrong. Everything on this
page exists because building it that way demanded it — the durable cursor, the
retention that waits, the bounded wait so a stuck consumer cannot fill the disk,
and reads that survive the compactor's renames.

## History

siglake used to contain its own detection pipeline, which read the WAL through
the raw primitives (`list_visible`, `read_segment`,
`publish_consumer_watermark`) and carried its own cursor. That made siglake a
storage engine with one particular detector welded to it, and left the
guarantees above as things the in-tree consumer happened to do correctly rather
than things any consumer could rely on.

Moving the pipeline out forced the guarantees to become an interface. The
consumer also gained a correctness fix in the process: the in-tree version
*skipped* a segment it could not read while still advancing its cursor past it,
so a single bad read silently dropped data and counted it as processed. The
interface makes the safe thing the easy thing — stop, and it is re-delivered.

### Nothing is still coupled

siglake used to keep the detection pipeline's output tables too — `candidates`,
`episodes`, `episode_events`, `detector_runs`, `webhook_dlq` — with built-in
schemas, `ensure_*`/`append_*` methods, DataFusion registration hooks, and
reserved names so nobody could create an index that collided. A storage engine
provisioning one particular consumer's tables, on every warehouse open.

Those are gone. The detection tables are now declared by the consumer that
writes them, through the same `IndexConfig` any caller uses:

```rust
ice.create_index(&my_output_table_config()).await?;
ice.append_to_table(&ice.index_table_ident("my_table"), batch, &tag_columns).await?;
```

Everything downstream follows from being an index, with no special case
anywhere: `SELECT … FROM candidates` registers through the generic index path,
`siglake subscribe --table episodes` tails it (subscriptions used to be a closed
list of five names; any index works now), per-index retention applies, and
`siglake sql-direct` sees it.

A fresh siglake warehouse now contains `events` and `query_audit` and nothing
else. The only reserved index id is `query_audit`.

`siglake_core::shard` went too — consistent `(host, sourcetype)` partitioning
for replicated consumers. It had no callers inside siglake; sharding a consumer
is the consumer's business, and the chart's `siglake.shardEnv` helper still
supplies `SIGLAKE_SHARD_INDEX`/`_COUNT` from StatefulSet ordinals for anyone who
wants it.

## Writing your own output tables

If your consumer produces results, declare them the same way — there is no
privileged path:

1. Build an `IndexConfig` (one `timestamp_field`, typed `field_mappings`,
   `tag_fields` for the columns you will filter on — those become the pruning
   blooms).
2. `create_index` once, idempotently, on startup.
3. `append_to_table` your Arrow batches.

Two things worth knowing. A user index always carries a trailing nullable
`attributes` column for residual fields; your batches do not need it, because
appends null-pad missing columns **by name**. And the field vocabulary is
`text`/`long`/`double`/`bool`/`datetime`/`bytes`/`json` — there is one integer
width, so an `Int32` column becomes `Long`.

The technique that keeps this honest: derive your `IndexConfig`s FROM the Arrow
schemas your code builds batches against, rather than writing both by hand, and
assert in a test that the two agree. Hand-written pairs drift, and the failure is
quiet — an append against a shifted schema can succeed. The reference consumer
does exactly this, after hand-written field lists got two of its tables wrong.
