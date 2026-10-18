# Bounded staging for v1 index rebuilds

Production rebuilds retain the v1 KIDX encoding and Zstd Puffin codec. They read
one projected column at a time in the existing 8192-row Parquet batches, preserve
physical row ordinals (including empty and NULL values), and spill sorted posting
runs instead of retaining a whole file's posting vectors. The spill builder merges
runs with bounded fan-in and emits the same uncompressed bytes as `IndexBuilder`.

The storage caller gives each builder an 8 MiB work budget and a 1 GiB scratch
budget. A single over-budget input row and scratch exhaustion are explicit errors;
there is no token truncation, partial index, row omission, or alternate encoding.
The compressed spool has a separate 1 GiB limit. The complete staged Puffin file,
including its footer, has a 2 GiB limit. Only one file-column builder and compressed
spool exist at once. Scratch files are private, owned temporary files and removed
on drop. Deployments must place temporary files on a suitable disk filesystem and
reserve scratch across concurrent rebuilds; these per-rebuild limits are not a
global reservation system.

This bounds posting construction and serialized payload staging. It does not bound
Parquet row-group decoding, the input batch's strings, allocator-retained arenas,
Puffin footer metadata proportional to the number of indexed columns/files, or the
existing parsed query cache. Synchronous spill/merge/compression work is not a
scheduler preemption guarantee. The original file/chunk/row-group geometry and
memory acceptance thresholds remain unchanged and require actual qualification.

Compression uses the existing level, content-size pledge, and checksum. Compressed
bytes are copied in chunks of at most 64 KiB into a local Puffin writer. A failed,
short, long, or cancelled streamed blob poisons that writer: it cannot produce a
valid footer or accept another blob. File-size limits include the footer.

Only after every blob is complete does rebuild check the original caller-table
`snapshot_has_statistics` guard. If already covered, it records the existing
deferral with no upload. Otherwise it streams the complete local sidecar to its
unique remote path and uses the same `RegisterFirstStatisticsAction` transaction
as inline indexing. A racing registration still wins first and leaves the new
object unreferenced for existing orphan cleanup; no second registration attempt,
snapshot redirect, or #5425 implementation is introduced. Failed rebuilds retain
the existing error path; a previously committed rewrite remains committed.
