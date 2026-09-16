#!/usr/bin/env bash
# Verify that external Iceberg readers can open a siglake warehouse and that
# `timestamp_ns` round-trips the OTLP nanosecond through them.
#
# The 2026-09-06 timestamp contract ("Timestamp contract" in
# docs/DESIGN_time_ordered_storage.md) exists so that Spark, DuckDB and
# PyIceberg can read siglake tables at all: `timestamp` is a microsecond
# `timestamptz` and tables are format version 2. This is that claim's
# regression check.
#
# Two halves:
#   1. `siglake iceberg-demo` writes the fixture and asserts the siglake-side
#      contract itself (format version, Iceberg field types, the nanosecond
#      round-trip, no nulls, the total order) and prints the values -- including
#      the microsecond bounds -- the engines are then held to. That half always
#      runs.
#   2. Each external engine reads the same warehouse and must agree on the row
#      count, the nanosecond bounds AND the decoded microsecond `timestamp`:
#      its type, its bounds, that no row of either column is null, and that
#      every row's decoded microsecond is floor(timestamp_ns / 1000). Reading
#      and typing `timestamp` is part of each engine's single query, so an
#      engine that cannot decode the column fails instead of passing on its
#      nanosecond sibling. An engine that is not installed is
#      SKIPPED, not failed — the engines are heavy and are expected only in the
#      nightly/heavy gate. `--require-engines` turns a skip into a failure, and
#      that is how the heavy gate should invoke this.
#
# Engines are located as: `duckdb` on PATH, `spark-sql` on PATH, and pyiceberg
# importable by `python3`. Spark additionally needs a SQLite JDBC driver, since
# it reads the fixture through the same SQLite catalog siglake writes: set
# `SIGLAKE_SQLITE_JDBC_JAR` to a local `sqlite-jdbc-*.jar` for an offline gate,
# or let spark-sql resolve `org.xerial:sqlite-jdbc` from Maven.

set -euo pipefail

cd "$(dirname "$0")/.."

require_engines=0
rows=2000
for arg in "$@"; do
  case "$arg" in
    --require-engines) require_engines=1 ;;
    --rows=*) rows="${arg#--rows=}" ;;
    *)
      echo "usage: $0 [--require-engines] [--rows=N]" >&2
      exit 2
      ;;
  esac
done

work=$(mktemp -d "${TMPDIR:-/tmp}/siglake-timestamp-contract.XXXXXX")
trap 'rm -rf -- "$work"' EXIT

target_dir="${CARGO_TARGET_DIR:-$PWD/target}"
siglake="${SIGLAKE_BIN:-}"
if [ -z "$siglake" ]; then
  # BUILD rather than pick up whatever is warm. CARGO_TARGET_DIR is shared and
  # long-lived here, so a `release/siglake` left by some older checkout is both
  # present and wrong -- and a check that silently tests a stale binary is worse
  # than no check. `cargo build` is a no-op when the tree is already built.
  echo "building siglake-cli (set SIGLAKE_BIN to test a specific binary)"
  if ! cargo build -q -p siglake-cli >&2; then
    echo "FAIL: cargo build -p siglake-cli failed" >&2
    exit 1
  fi
  siglake="$target_dir/debug/siglake"
fi
if [ ! -x "$siglake" ]; then
  echo "FAIL: no siglake binary at $siglake" >&2
  echo "      (set SIGLAKE_BIN to the binary to test)" >&2
  exit 1
fi
echo "siglake binary: $siglake"

failures=0
skips=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}
skip() {
  if [ "$require_engines" -eq 1 ]; then
    fail "$1 not installed and --require-engines was given"
  else
    echo "SKIP: $1 not installed"
    skips=$((skips + 1))
  fi
}

# --- half 1: write the fixture and assert the siglake-side contract ----------

echo "== siglake iceberg-demo ($rows rows) =="
if ! RUST_LOG=warn "$siglake" --data-dir "$work" iceberg-demo \
  --n "$rows" --reset >"$work/demo.log" 2>&1; then
  sed 's/^/  /' "$work/demo.log" >&2
  fail "iceberg-demo did not satisfy the siglake-side contract"
  exit 1
fi
grep -E '^(contract ok|external assertions):' "$work/demo.log" | sed 's/^/  /' || true

# The fixture prints the values the external engines must agree on. A binary too
# old to print them is the likeliest cause here (a stale release build in a warm
# target directory), so say so rather than dying on an empty grep.
assertions=$(grep '^external assertions:' "$work/demo.log" || true)
if [ -z "$assertions" ]; then
  fail "$siglake printed no 'external assertions:' line; rebuild it (cargo build -p siglake-cli) or set SIGLAKE_BIN"
  exit 1
fi
field() { sed -n "s/.*$1=\([0-9-]*\).*/\1/p" <<<"$assertions"; }
expect_rows=$(field rows)
expect_min_ns=$(field min_timestamp_ns)
expect_max_ns=$(field max_timestamp_ns)
expect_min_us=$(field min_timestamp_us)
expect_max_us=$(field max_timestamp_us)
if [ -z "$expect_rows" ] || [ -z "$expect_min_ns" ] || [ -z "$expect_max_ns" ] ||
  [ -z "$expect_min_us" ] || [ -z "$expect_max_us" ]; then
  fail "could not parse the fixture's expected values from: $assertions"
  exit 1
fi
# The microsecond expectations are what the engines' decoded `timestamp` is
# diffed against, so they have to be floor(timestamp_ns / 1000) themselves --
# otherwise a fixture bug would be handed to every engine as the contract.
if [ "$expect_min_us" != "$((expect_min_ns / 1000))" ] ||
  [ "$expect_max_us" != "$((expect_max_ns / 1000))" ]; then
  fail "fixture expects timestamp [$expect_min_us, $expect_max_us] us, which is not timestamp_ns [$expect_min_ns, $expect_max_ns] floored to the microsecond"
  exit 1
fi

# `iceberg-demo` writes a SQLite-backed catalog; the table's metadata.json is
# what the file-based external readers are pointed at.
metadata=$(find "$work" -path '*/events/metadata/*.metadata.json' | sort | tail -1)
if [ -z "$metadata" ]; then
  fail "no events metadata.json under $work"
  exit 1
fi
echo "  table metadata: ${metadata#"$work"/}"

# The engine's spelling of the `timestamp` column's type. Each engine names the
# microsecond UTC timestamp differently and none of them spells a nanosecond or
# zone-naive type this way, so an exact match per engine is the check: a
# `TIMESTAMP_NS`, a `timestamp_ntz` or a bare `BIGINT` all fall through.
timestamp_type_ok() { # <engine> <type as the engine printed it>
  case "$1:$2" in
    # DuckDB's typeof() for a timestamptz column.
    'duckdb:TIMESTAMP WITH TIME ZONE' | duckdb:TIMESTAMPTZ) return 0 ;;
    # Spark maps Iceberg timestamptz to TimestampType (instant semantics,
    # microseconds); typeof() prints it lowercase.
    spark:timestamp) return 0 ;;
    # PyIceberg answers with the pyarrow type it materialised.
    'pyiceberg:timestamp[us, tz=UTC]' | 'pyiceberg:timestamp[us, tz=+00:00]') return 0 ;;
    *) return 1 ;;
  esac
}

# Every engine answers the same eight values, and every one of them is asserted:
# the row count and `timestamp_ns` bounds, the decoded `timestamp` as
# microseconds since the epoch (bounds, plus the per-row count of rows where it
# is not floor(timestamp_ns / 1000)), the null count over both columns, and the
# engine's name for the column's type. A correct nanosecond triple therefore
# cannot carry a reader whose `timestamp` is unreadable, of the wrong type, or
# off by any amount. Comparisons are string comparisons on purpose: a garbled or
# missing field is a mismatch, not a shell arithmetic error.
check_reader() { # <engine> <rows> <min_ns> <max_ns> <min_us> <max_us> <nulls> <mismatched> <type>
  local engine=$1 got_rows=$2 got_min=$3 got_max=$4
  local got_min_us=$5 got_max_us=$6 got_nulls=$7 got_bad=$8 got_type=$9
  local problems=()
  if [ "$got_rows" != "$expect_rows" ] ||
    [ "$got_min" != "$expect_min_ns" ] ||
    [ "$got_max" != "$expect_max_ns" ]; then
    problems+=("read rows=$got_rows timestamp_ns=[$got_min, $got_max], expected rows=$expect_rows timestamp_ns=[$expect_min_ns, $expect_max_ns]")
  fi
  if [ "$got_min_us" != "$expect_min_us" ] || [ "$got_max_us" != "$expect_max_us" ]; then
    problems+=("decoded timestamp [$got_min_us, $got_max_us] us, expected [$expect_min_us, $expect_max_us]")
  fi
  if [ "$got_bad" != 0 ]; then
    problems+=("$got_bad row(s) where the decoded timestamp is not floor(timestamp_ns / 1000)")
  fi
  if [ "$got_nulls" != 0 ]; then
    problems+=("$got_nulls null timestamp/timestamp_ns value(s); both columns are required")
  fi
  if ! timestamp_type_ok "$engine" "$got_type"; then
    problems+=("timestamp is of type '$got_type', not a microsecond UTC timestamp")
  fi
  if [ "${#problems[@]}" -eq 0 ]; then
    # Exactly one `  ok:` line per reader: ci-local-external-readers.sh counts
    # them to report how many readers agreed.
    echo "  ok: $engine read $got_rows rows, timestamp_ns [$got_min, $got_max], timestamp [$got_min_us, $got_max_us] us as $got_type"
  else
    # One FAIL line per reader, however many assertions it broke: the reporting
    # in ci-local-external-readers.sh counts FAIL lines, and a reader that fails
    # three ways is still one reader that disagreed.
    local joined
    joined=$(printf '%s; ' "${problems[@]}")
    fail "$engine ${joined%; }"
  fi
}

# --- half 2: the external engines --------------------------------------------

echo "== DuckDB iceberg_scan =="
if command -v duckdb >/dev/null 2>&1; then
  duckdb_version='version unknown'
  if duckdb_version_output=$(duckdb --version 2>&1); then
    duckdb_version=$(awk 'NF { sub(/^[[:space:]]+/, ""); print; exit }' \
      <<<"$duckdb_version_output")
    [ -n "$duckdb_version" ] || duckdb_version='version unknown'
  fi
  echo "engine: DuckDB $duckdb_version"

  # iceberg_scan over the metadata.json needs no catalog. Both the nanoseconds
  # and the decoded microseconds exceed DuckDB's default integer display, so
  # cast to VARCHAR before reading back. `epoch_us` is the decoded instant in
  # microseconds since the epoch and `//` is integer division.
  #
  # The timestamp reads are in the SAME query as the counts, deliberately: until
  # 2026-09-07 the type probe ran separately with `|| true`, so a DuckDB that
  # could not decode `timestamp` at all -- the exact failure the old nanosecond
  # type caused -- still passed on its nanosecond triple.
  duckdb -noheader -list -c "
    INSTALL iceberg; LOAD iceberg;
    SELECT count(*)::VARCHAR,
           min(timestamp_ns)::VARCHAR,
           max(timestamp_ns)::VARCHAR,
           min(epoch_us(\"timestamp\"))::VARCHAR,
           max(epoch_us(\"timestamp\"))::VARCHAR,
           count(*) FILTER (WHERE \"timestamp\" IS NULL OR timestamp_ns IS NULL),
           count(*) FILTER (WHERE epoch_us(\"timestamp\") != timestamp_ns // 1000),
           min(typeof(\"timestamp\"))
    FROM iceberg_scan('$metadata');
  " >"$work/duckdb.out" 2>"$work/duckdb.err" && duckdb_ran=1 || duckdb_ran=0
  if [ "$duckdb_ran" -eq 0 ]; then
    sed 's/^/  /' "$work/duckdb.err" >&2
    fail "duckdb iceberg_scan errored (reading or typing the microsecond timestamp is part of this query)"
  elif [ ! -s "$work/duckdb.out" ]; then
    # A silent empty answer would otherwise pass the whole check.
    fail "duckdb iceberg_scan printed nothing"
  fi
  if [ "$duckdb_ran" -eq 1 ] && [ -s "$work/duckdb.out" ]; then
    IFS='|' read -r d_rows d_min d_max d_min_us d_max_us d_nulls d_bad d_type \
      <"$work/duckdb.out"
    check_reader duckdb "$d_rows" "$d_min" "$d_max" \
      "$d_min_us" "$d_max_us" "$d_nulls" "$d_bad" "$d_type"
  fi
else
  skip duckdb
fi

echo "== Spark Iceberg reader =="
if command -v spark-sql >/dev/null 2>&1; then
  spark_command=$(command -v spark-sql)
  spark_home=${SPARK_HOME:-$(dirname "$(dirname "$spark_command")")}
  spark_version='version unknown'
  if spark_version_output=$(spark-sql --version 2>&1); then
    spark_version=$(awk '
      {
        line = tolower($0)
        if (match(line, /version[[:space:]]+[^[:space:]]+/)) {
          print substr($0, RSTART, RLENGTH)
          exit
        }
      }
    ' <<<"$spark_version_output")
    [ -n "$spark_version" ] || spark_version='version unknown'
  fi
  iceberg_runtime='version unknown'
  for jar in "$spark_home"/jars/iceberg-spark-runtime-*.jar; do
    [ -f "$jar" ] || continue
    jar=${jar##*/}
    if [ "$iceberg_runtime" = 'version unknown' ]; then
      iceberg_runtime=$jar
    else
      iceberg_runtime="$iceberg_runtime,$jar"
    fi
  done
  echo "engine: Spark $spark_version; Iceberg runtime $iceberg_runtime"

  # Spark reads the fixture through the catalog siglake actually wrote: the
  # SQLite one at `{warehouse}/_catalog.db` (`IcebergContext::open`), which is
  # Iceberg's JDBC catalog schema. A Hadoop catalog cannot open this table at
  # all -- it resolves metadata by `vN.metadata.json` + `version-hint.text`,
  # while the SQL catalog writes `00002-<uuid>.metadata.json` and no hint.
  #
  # Two names have to line up with the fixture, not with our taste:
  #   - the warehouse root is the directory holding `_catalog.db`, not the
  #     table's grandparent (that is `{warehouse}/{namespace}`);
  #   - the Spark catalog must be *named* `siglake`, because Iceberg's
  #     JdbcCatalog keys every lookup on `iceberg_tables.catalog_name` and
  #     siglake loads its catalog under the name `siglake`. Hence the
  #     `siglake.siglake.events` below: catalog, namespace, table.
  catalog_db=$(find "$work" -name '_catalog.db' | sort | head -1)
  if [ -z "$catalog_db" ]; then
    fail "no _catalog.db under $work; the fixture's catalog layout changed"
    catalog_db=/nonexistent
  fi
  warehouse=$(dirname "$catalog_db")

  # Iceberg's JdbcCatalog opens that file over JDBC, and no Spark distribution
  # ships a SQLite driver. Prefer a caller-supplied jar (an offline heavy gate
  # has no Maven), then one already in $SPARK_HOME/jars, then a coordinate
  # spark-sql resolves itself.
  spark_jar_opts=()
  if [ -n "${SIGLAKE_SQLITE_JDBC_JAR:-}" ]; then
    if [ ! -f "$SIGLAKE_SQLITE_JDBC_JAR" ]; then
      fail "SIGLAKE_SQLITE_JDBC_JAR=$SIGLAKE_SQLITE_JDBC_JAR is not a file"
    fi
    spark_jar_opts=(--jars "$SIGLAKE_SQLITE_JDBC_JAR")
  else
    if ! compgen -G "$spark_home/jars/sqlite-jdbc-*.jar" >/dev/null; then
      spark_jar_opts=(--packages "org.xerial:sqlite-jdbc:${SIGLAKE_SQLITE_JDBC_VERSION:-3.50.3.0}")
    fi
  fi

  # Run from $work: spark-sql drops a Derby `metastore_db/` and `derby.log` in
  # $PWD, which is the repo root here.
  (
    cd "$work" &&
      spark-sql --silent \
        ${spark_jar_opts[@]+"${spark_jar_opts[@]}"} \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.siglake=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.siglake.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
        --conf "spark.sql.catalog.siglake.uri=jdbc:sqlite:$catalog_db" \
        --conf "spark.sql.catalog.siglake.warehouse=file://$warehouse" \
        -e 'SELECT count(*), min(timestamp_ns), max(timestamp_ns),
                   min(unix_micros(`timestamp`)), max(unix_micros(`timestamp`)),
                   count(CASE WHEN `timestamp` IS NULL OR timestamp_ns IS NULL THEN 1 END),
                   count(CASE WHEN unix_micros(`timestamp`) <> timestamp_ns DIV 1000 THEN 1 END),
                   min(typeof(`timestamp`))
            FROM siglake.siglake.events;'
  ) >"$work/spark.out" 2>"$work/spark.err" && spark_ran=1 || spark_ran=0
  # `unix_micros` is the decoded instant in microseconds since the epoch (UTC
  # regardless of the session time zone) and `DIV` is integer division --
  # `timestamp_ns / 1000` would go through a double and lose the low digits of a
  # 19-digit nanosecond. Reading and typing `timestamp` is part of this one
  # query, so a Spark that cannot map the column fails the reader outright.
  if [ "$spark_ran" -eq 0 ]; then
    sed 's/^/  /' "$work/spark.err" >&2
    fail "spark-sql errored (schema conversion is what the old contract failed on; a missing SQLite driver is the other likely cause -- set SIGLAKE_SQLITE_JDBC_JAR)"
  elif [ -s "$work/spark.out" ]; then
    IFS=$'\t' read -r s_rows s_min s_max s_min_us s_max_us s_nulls s_bad s_type \
      <"$work/spark.out"
    check_reader spark "$s_rows" "$s_min" "$s_max" \
      "$s_min_us" "$s_max_us" "$s_nulls" "$s_bad" "$s_type"
  else
    # A silent empty answer would otherwise pass the whole check.
    fail "spark-sql printed nothing for siglake.siglake.events"
  fi
else
  skip spark-sql
fi

echo "== PyIceberg =="
# pyarrow too: `to_arrow()` is how the reader decodes both columns, so a
# pyiceberg without it cannot check the contract at all and is a skip, not a
# red.
if python3 -c 'import pyiceberg, pyarrow' >/dev/null 2>&1; then
  pyiceberg_version='version unknown'
  pyarrow_version='version unknown'
  if python_versions=$(python3 -c \
    'import pyarrow, pyiceberg; print(f"{pyiceberg.__version__}|{pyarrow.__version__}")' \
    2>&1); then
    IFS='|' read -r pyiceberg_version pyarrow_version <<<"$python_versions"
    [ -n "$pyiceberg_version" ] || pyiceberg_version='version unknown'
    [ -n "$pyarrow_version" ] || pyarrow_version='version unknown'
  fi
  echo "engine: PyIceberg $pyiceberg_version; PyArrow $pyarrow_version"

  # Reads BOTH columns and reports; the shell asserts, like the other two
  # readers. Any read or type failure raises out of here and fails the reader.
  cat >"$work/pyiceberg_read.py" <<'PY'
import sys

import pyarrow as pa
from pyiceberg.table import StaticTable

metadata = sys.argv[1]
table = StaticTable.from_metadata(metadata)
arrow = table.scan(selected_fields=("timestamp", "timestamp_ns")).to_arrow()
ts, ns = arrow.column("timestamp"), arrow.column("timestamp_ns")

# Casting the timestamp column to int64 is its stored microsecond value; a
# column of any other unit would not compare equal to floor(ns / 1000) below,
# and the type spelling the harness checks is reported verbatim.
micros = ts.cast(pa.int64()).to_pylist()
nanos = ns.to_pylist()
nulls = sum(1 for v in micros if v is None) + sum(1 for v in nanos if v is None)
mismatched = sum(
    1 for u, n in zip(micros, nanos) if u is None or n is None or u != n // 1000
)


def bounds(values):
    # 0 for an empty or all-null column: the harness compares against the
    # fixture's own bounds, so a placeholder is a mismatch rather than a pass.
    present = [v for v in values if v is not None]
    return (min(present), max(present)) if present else (0, 0)


min_ns, max_ns = bounds(nanos)
min_us, max_us = bounds(micros)
print(
    "%d|%d|%d|%d|%d|%d|%d|%s"
    % (len(nanos), min_ns, max_ns, min_us, max_us, nulls, mismatched, ts.type)
)
PY
  if python3 "$work/pyiceberg_read.py" "$metadata" \
    >"$work/pyiceberg.out" 2>"$work/pyiceberg.err"; then
    IFS='|' read -r p_rows p_min p_max p_min_us p_max_us p_nulls p_bad p_type \
      <"$work/pyiceberg.out"
    check_reader pyiceberg "$p_rows" "$p_min" "$p_max" \
      "$p_min_us" "$p_max_us" "$p_nulls" "$p_bad" "$p_type"
  else
    sed 's/^/  /' "$work/pyiceberg.err" >&2
    fail "pyiceberg could not read the table"
  fi
else
  skip pyiceberg
fi

echo
if [ "$failures" -gt 0 ]; then
  echo "FAIL: $failures external-reader check(s) failed" >&2
  exit 1
fi
if [ "$skips" -gt 0 ]; then
  echo "ok (siglake-side contract asserted; $skips engine(s) skipped)"
else
  echo "ok (siglake-side contract asserted; every engine agreed)"
fi
