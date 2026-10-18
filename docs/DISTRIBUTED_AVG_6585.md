# Bounded distributed integer AVG

This source candidate adds a two-state AVG path to the coordinator. It has not
been compiled, formatted, tested or performance-qualified as of this source
freeze. Task 6585 still requires its original frozen-S3 off/on qualification.

Eligibility is intentionally narrow: a pure scan with column group keys,
integer-column AVG, optional COUNT, and a pass-through output projection. A
final ORDER BY over output columns and LIMIT are applied only after merging.
Signed 8/16/32/64 and unsigned 8/16/32 inputs are supported. Floating, decimal,
duration, unsigned64, computed AVG operands, DISTINCT/FILTER/ordered aggregate
arguments, HAVING and subquery aggregates retain the original local fallback.

Each AVG ships SUM(CAST(value AS DOUBLE)) and COUNT(value), plus MIN/MAX of the
**original integer** as range witnesses. Casting the witnesses would be unsafe:
2^53+1 rounds to 2^53 and could otherwise pass the guard. NULL input values do
not increment the AVG denominator; count(*) remains a separate output. Workers
never apply the final group order or LIMIT. Internal aliases do not escape in
the response, and the original final column names, order and types are restored.

The coordinator checks count sign/overflow, state schema and NULL consistency,
then sums max(abs(min),abs(max))*non_null_count across all states per AVG. Both
this conservative absolute bound and total non-null count must be <=2^53.
Consequently all underlying integers, all partial and intermediate sums, and
the final count are exactly representable in binary64. Division occurs once,
as in the original AVG. Out-of-bound execution refuses instead of returning a
rounded answer or silently retrying a whole-table query. This may refuse some
otherwise representable workloads; it does not introduce an epsilon oracle or
promise exact generic floating-point distributed summation.

Remote shards, local shard failover and the WAL-buffer partial use one worker
SQL accessor. WAL admission retains its existing bounded-expression checks and
DecodedBuffer lifetime. The extra cast workspace is charged on the decoded
input lease, and output admission uses the wider partial-state schema rather
than the smaller public schema. No WAL size, deadline, memory or tenant guard
is relaxed.

Added source regressions execute actual worker SQL on unequal populations,
NULL/all-NULL/empty inputs, repeated AVG aliases, global group top-N and a WAL
extra partial. State mutations cover wrong schema, nonfinite/null sums,
negative counts and original-integer range boundaries. A managed-index test
starts three real HTTP worker routers over persisted files plus an uncommitted
WAL tail, submits the unchanged registered httplogs AVG SQL, compares every
answer/order against an independently built MemTable, and requires one actual
shard request per worker plus distributed phases. These tests still require
execution in a coordinated build slot, followed by independent source review,
full exact-source gates and qualification; source presence is not acceptance.

WAL admission is mandatory for this new AVG path. A selected tail that cannot
be decoded, bounded, reserved or attached returns an explicit error before
worker fan-out; it must not be replaced by empty partials. Cast workspace uses
the conservative Arrow startup/rounding/fragmentation bound, and the attached
input's full physical payload is charged separately. Detached Arrow children
retain both decoded and workspace reservations. Other distribution variants
keep their existing refusal policy. These source regressions still require
execution under the separately owned compiler lease.
