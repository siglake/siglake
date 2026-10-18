# Timestamp scratch in page-bounded merges

Page-bounded merge planning writes each projected timestamp column to a private
fixed-width temporary file instead of retaining every column as `Vec<i64>`.
Each census or execution pass opens its own file descriptions and holds one
64 KiB forward buffer per input, plus a heap and cursor positions. Runs coalesce
across scratch-page boundaries. The uncut-run copy census and the configured
merge chunk sizes, fetch ranges, output order, and row-group sizes are unchanged.

Scratch uses exactly eight bytes per input row. The 64 × 235,349-row production
fixture therefore needs 120,498,688 scratch bytes, with 4 MiB of timestamp cursor
buffers per pass. Census buffers are dropped before execution buffers are opened.
This is a timestamp-residency bound, **not** a total compactor memory bound or
proof that timestamps caused the retained 1 GiB failure. Parquet decoders,
encoders, input chunks, metadata, and later v1 index rebuilds have separate costs.

`SIGLAKE_MERGE_TIMESTAMP_SPOOL_DIR` optionally selects the scratch directory;
otherwise the platform temporary directory is used. Choose a disk-backed volume.
A tmpfs directory charges scratch bytes to memory and defeats this separation.
Page cache can also be charged to the cgroup; it is reclaimable, not free.

`SIGLAKE_MERGE_TIMESTAMP_SPOOL_MAX_BYTES` is an exact per-merge admission ceiling,
defaulting to 8 GiB. A malformed setting, arithmetic overflow, insufficient disk,
short/long input, unexpected file length, or read/write error refuses the merge.
There is no fallback to full in-memory timestamps and no successful partial EOF.
Concurrent merges require the sum of their admitted scratch allowances; this
setting does not implement a global disk reservation. Required bytes, configured
limit, and scratch location are logged at admission.

Temporary files use private 0600 permissions on Unix. Ordinary success, error,
and cancellation remove them through RAII; an abrupt process kill can leave
scratch files in the selected directory. An error requests cancellation of outstanding owned merge fetch tasks and
never registers partial output. Abort-on-drop does not synchronously await
termination or prove all decoder buffers are released before return. Existing orphan-data recovery
rules still apply to incomplete Parquet output.

The new planner uses an extended signed key so both i64 timestamp extrema sort
correctly without negating i64::MIN. Lowest input index continues to break ties,
and row ordinals within each input remain unchanged.
