#!/usr/bin/env bash
# Exercise ci-local.sh's external-readers reporting with no engine installed
# and no Rust compiled: a stub SIGLAKE_BIN writes the fixture layout, stub
# `duckdb` / `spark-sql` / `python3` stand in for the engines, and the real
# check-external-timestamp-contract.sh runs against them under a PATH that
# holds nothing but the utilities it needs -- so the missing-engine arms are
# still missing-engine arms on a box that has duckdb.
#
# The arms are the four the reporting has to tell apart: every engine agreed,
# some skipped, all skipped, and an engine that ran and disagreed. Each runs in
# both modes, because the bug being pinned here (2026-09-07, task #1701) was a
# `--strict --all` run reporting `ok (0 engine(s) agreed, 3 skipped)`.
#
# A second set of arms (2026-09-07, task #1702) pins the other half of the
# claim: a reader that answers the nanosecond triple correctly must still fail
# when its decoded microsecond `timestamp` is wrong, of the wrong type, null, or
# unreadable — and the fixture must not be able to hand the engines a
# microsecond expectation that is not floor(timestamp_ns / 1000), or none at all.

set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/ci-local-external-readers.sh

work=$(mktemp -d "${TMPDIR:-/tmp}/siglake-external-readers.XXXXXX")
trap 'rm -rf -- "$work"' EXIT

bash_bin=$(command -v bash)
# The stub PATH carries only what the checker itself runs. Anything missing
# here would fail the arm for the wrong reason, so say so instead.
for tool in mktemp rm mkdir sed grep awk find sort head tail cat dirname; do
  tool_path=$(command -v "$tool" || true)
  if [ -z "$tool_path" ]; then
    echo "FAIL: $tool is not on PATH; the stub environment cannot be built" >&2
    exit 1
  fi
  ln -sf "$tool_path" "$work/$tool"
done
mkdir -p "$work/stubs"

# The numbers the fixture claims and the engines must agree on: the nanosecond
# triple and the decoded microsecond bounds, which are the nanosecond bounds
# floored (`123` and `9876` nanoseconds are inside the same microseconds as
# their floors, so a reader that truncated to milliseconds would miss).
fixture_rows=7
fixture_min=1757000000000000123
fixture_max=1757000000000009876
fixture_min_us=1757000000000000
fixture_max_us=1757000000000009

stub() { # <dir> <name> ; body on stdin
  local path="$1/$2"
  printf '#!%s\n' "$bash_bin" >"$path"
  cat >>"$path"
  chmod +x "$path"
}

# What one stub reports, per arm: the fixture's own values unless the arm
# overrode them. Set by `set_stub_values` and read by the stub writers, because
# a stub's body is generated once and has to bake its answer in.
s_rows= s_min= s_max= s_min_us= s_max_us= s_nulls= s_bad= s_type= s_exit= s_omit_us= s_version_exit=

# Each engine spells the microsecond UTC timestamp type its own way, and the
# checker holds each to its own spelling; these are the correct ones.
default_type() { # <engine>
  case $1 in
    duckdb) printf 'TIMESTAMP WITH TIME ZONE\n' ;;
    spark-sql) printf 'timestamp\n' ;;
    pyiceberg) printf 'timestamp[us, tz=UTC]\n' ;;
  esac
}

set_stub_values() { # <engine> [key=value ...]
  local engine=$1 kv
  shift
  s_rows=$fixture_rows
  s_min=$fixture_min
  s_max=$fixture_max
  s_min_us=$fixture_min_us
  s_max_us=$fixture_max_us
  s_nulls=0
  s_bad=0
  s_type=$(default_type "$engine")
  s_exit=0
  s_omit_us=0
  s_version_exit=0
  # `bad` is the reader's own count of rows whose decoded microsecond is not
  # floor(timestamp_ns / 1000); `exit` makes the reader's query fail; `omit_us`
  # is the fixture printing no microsecond expectations at all.
  for kv in "$@"; do
    case $kv in
      rows=*) s_rows=${kv#*=} ;;
      min=*) s_min=${kv#*=} ;;
      max=*) s_max=${kv#*=} ;;
      min_us=*) s_min_us=${kv#*=} ;;
      max_us=*) s_max_us=${kv#*=} ;;
      nulls=*) s_nulls=${kv#*=} ;;
      bad=*) s_bad=${kv#*=} ;;
      type=*) s_type=${kv#*=} ;;
      exit=*) s_exit=${kv#*=} ;;
      omit_us=*) s_omit_us=${kv#*=} ;;
      version_exit=*) s_version_exit=${kv#*=} ;;
      *)
        echo "FAIL: unknown stub override $engine:$kv" >&2
        exit 1
        ;;
    esac
  done
}

# `siglake --data-dir D iceberg-demo --n N --reset`: the fixture half. Writes
# the SQLite-catalog layout the checker locates (a metadata.json under
# `*/events/metadata/`, a `_catalog.db` beside the namespace) and prints the
# `external assertions:` line the engines are then held to.
make_siglake_stub() { # <dir> ; uses s_*
  local us_fields="min_timestamp_us=$s_min_us max_timestamp_us=$s_max_us "
  if [ "$s_omit_us" = 1 ]; then
    # A binary predating the microsecond assertions: the checker must refuse it
    # rather than silently check nanoseconds only.
    us_fields=
  fi
  stub "$1" siglake <<STUB
set -eu
data_dir=
while [ \$# -gt 0 ]; do
  case \$1 in
    --data-dir) data_dir=\$2; shift ;;
  esac
  shift
done
metadata="\$data_dir/warehouse/siglake/events/metadata"
mkdir -p "\$metadata"
: >"\$data_dir/warehouse/_catalog.db"
printf '{"format-version":2}\n' >"\$metadata/00001-stub.metadata.json"
printf 'contract ok: format_version=2 timestamp=timestamptz(us) timestamp_ns=long\n'
printf 'external assertions: rows=%s min_timestamp_ns=%s max_timestamp_ns=%s %sdistinct_timestamp=%s distinct_timestamp_ns=%s\n' \\
  '$s_rows' '$s_min' '$s_max' '$us_fields' '$s_rows' '$s_rows'
exit $s_exit
STUB
}

# The eight values every reader answers with, in the checker's field order.
stub_fields() { # <separator>
  printf "%s$1%s$1%s$1%s$1%s$1%s$1%s$1%s\\\\n" \
    "$s_rows" "$s_min" "$s_max" "$s_min_us" "$s_max_us" "$s_nulls" "$s_bad" "$s_type"
}

# `duckdb -noheader -list -c SQL`: one pipe-separated row, or a failure -- the
# counts and the timestamp reads are one query now, so an engine that cannot
# decode `timestamp` fails the whole reader.
make_duckdb_stub() { # <dir> ; uses s_*
  stub "$1" duckdb <<STUB
set -eu
if [ "\${1:-}" = --version ]; then
  [ $s_version_exit -eq 0 ] || exit $s_version_exit
  printf 'v1.4.1-stub\\n'
  exit 0
fi
if [ $s_exit -ne 0 ]; then
  echo 'Binder Error: No function matches epoch_us(TIMESTAMP_NS)' >&2
  exit $s_exit
fi
printf '$(stub_fields '|')'
STUB
}

# `spark-sql ... -e SELECT ...`: tab-separated, which is what the checker reads.
make_spark_stub() { # <dir> ; uses s_*
  stub "$1" spark-sql <<STUB
set -eu
if [ "\${1:-}" = --version ]; then
  [ $s_version_exit -eq 0 ] || exit $s_version_exit
  printf 'version 3.5.7-stub\\n' >&2
  exit 0
fi
if [ $s_exit -ne 0 ]; then
  echo 'AnalysisException: cannot resolve unix_micros(timestamp)' >&2
  exit $s_exit
fi
printf '$(stub_fields '\t')'
STUB
}

# `python3 -c 'import pyiceberg, pyarrow'` then `python3 read.py METADATA`. The
# read script now reports and the checker asserts, so the stub reports too.
make_pyiceberg_stub() { # <dir> ; uses s_*
  stub "$1" python3 <<STUB
set -eu
if [ "\${1:-}" = -c ]; then
  case \${2:-} in
    *__version__*)
      [ $s_version_exit -eq 0 ] || exit $s_version_exit
      printf '0.10.0-stub|21.0.0-stub\\n'
      ;;
  esac
  exit 0
fi
if [ $s_exit -ne 0 ]; then
  echo 'pyarrow.lib.ArrowInvalid: cannot convert timestamp[ns]' >&2
  exit $s_exit
fi
printf '$(stub_fields '|')'
STUB
}

# An arm's stub directory: the coreutils, the fixture, and the engines named.
# Sets `arm_dir`; a failure here has to be able to exit the script, which it
# could not do from a command substitution.
#
# A spec is `<name>[:key=value[;key=value]]`, e.g. `duckdb:rows=6` for a reader
# that disagrees on the row count or `duckdb:min_us=...;type=BIGINT` for one
# whose nanoseconds are right and whose timestamp is not. `;` separates, not
# `,`, because a pyarrow type spelling contains a comma. The fixture's own spec
# is `<exit code>[:key=value...]`.
arm_dir=
split_spec() { # <spec> ; sets spec_name and spec_over[]
  spec_name=${1%%:*}
  spec_over=()
  if [ "$1" != "$spec_name" ]; then
    IFS=';' read -r -a spec_over <<<"${1#*:}"
  fi
}
spec_name=
spec_over=()
make_arm() { # <name> <fixture spec> [engine spec ...]
  local dir="$work/stubs/$1" engine
  mkdir -p "$dir"
  mkdir -p "$dir/spark-home/jars"
  : >"$dir/spark-home/jars/iceberg-spark-runtime-3.5_2.12-1.11.0-stub.jar"
  for tool in "$work"/*; do
    [ -L "$tool" ] || continue
    ln -sf "$(readlink "$tool")" "$dir/$(basename "$tool")"
  done
  split_spec "$2"
  local fixture_exit=$spec_name
  set_stub_values siglake "exit=$fixture_exit" ${spec_over[@]+"${spec_over[@]}"}
  make_siglake_stub "$dir"
  shift 2
  for engine in "$@"; do
    split_spec "$engine"
    set_stub_values "$spec_name" ${spec_over[@]+"${spec_over[@]}"}
    case $spec_name in
      duckdb) make_duckdb_stub "$dir" ;;
      spark-sql) make_spark_stub "$dir" ;;
      pyiceberg) make_pyiceberg_stub "$dir" ;;
      *)
        echo "FAIL: unknown stub engine $engine" >&2
        exit 1
        ;;
    esac
  done
  arm_dir=$dir
}

failures=0
expect() { # <label> <expected> <actual>
  if [ "$2" != "$3" ]; then
    echo "FAIL $1: expected '$2', got '$3'" >&2
    failures=$((failures + 1))
  fi
}

# One run of the real checker through the stub environment, reported the way
# ci-local.sh reports it: same argument helper, same status function, same log.
status_for() { # <arm dir> <strict> <log>
  local rc=0 opts=()
  mapfile -t opts < <(external_readers_args "$2")
  env -u SIGLAKE_SQLITE_JDBC_VERSION \
    PATH="$1" TMPDIR="$work" SPARK_HOME="$1/spark-home" SIGLAKE_BIN="$1/siglake" \
    SIGLAKE_SQLITE_JDBC_JAR="$1/sqlite-jdbc-stub.jar" \
    "$bash_bin" scripts/check-external-timestamp-contract.sh \
    ${opts[@]+"${opts[@]}"} >"$3" 2>&1 || rc=$?
  external_readers_status "$2" "$3" "$rc"
}

# Sets `arm_log` rather than printing it: a command substitution would run the
# assertions in a subshell and lose every failure it counted.
arm_log=
arm() { # <label> <arm dir> <strict> <expected status>
  local log="$work/$1.$3.log"
  : >"$2/sqlite-jdbc-stub.jar"
  expect "$1 (strict=$3)" "$4" "$(status_for "$2" "$3" "$log")"
  # A strict run hands the checker its own --require-engines, so nothing may
  # remain a SKIP: the reporting must not be the only thing that noticed.
  if [ "$3" = 1 ] && grep -q '^SKIP: ' "$log"; then
    echo "FAIL $1: a --strict run left SKIP lines in $log" >&2
    failures=$((failures + 1))
  fi
  arm_log=$log
}

# --- every engine agreed -----------------------------------------------------
make_arm complete 0 duckdb spark-sql pyiceberg
complete=$arm_dir
arm complete "$complete" 0 'ok (3 reader(s) agreed, none skipped)'
arm complete "$complete" 1 'ok (3 reader(s) agreed, none skipped)'
# Agreement is about the decoded timestamp too, and the log has to say so --
# one `ok:` line per reader (the reporting counts them) naming both columns.
for engine in duckdb spark pyiceberg; do
  if ! grep -Fq \
    "ok: $engine read $fixture_rows rows, timestamp_ns [$fixture_min, $fixture_max], timestamp [$fixture_min_us, $fixture_max_us] us as " \
    "$arm_log"; then
    echo "FAIL complete: $engine's ok line does not report the decoded timestamp" >&2
    failures=$((failures + 1))
  fi
done
for version_line in \
  'engine: DuckDB v1.4.1-stub' \
  'engine: Spark version 3.5.7-stub; Iceberg runtime iceberg-spark-runtime-3.5_2.12-1.11.0-stub.jar' \
  'engine: PyIceberg 0.10.0-stub; PyArrow 21.0.0-stub'; do
  if ! grep -Fqx "$version_line" "$arm_log"; then
    echo "FAIL complete: missing version line '$version_line'" >&2
    failures=$((failures + 1))
  fi
done

# Version probes are diagnostic: an installed reader still runs when its
# version command fails, and the report says that the version is unknown.
make_arm unknown_versions 0 \
  duckdb:version_exit=1 spark-sql:version_exit=1 pyiceberg:version_exit=1
arm unknown_versions "$arm_dir" 1 'ok (3 reader(s) agreed, none skipped)'
for version_line in \
  'engine: DuckDB version unknown' \
  'engine: Spark version unknown; Iceberg runtime iceberg-spark-runtime-3.5_2.12-1.11.0-stub.jar' \
  'engine: PyIceberg version unknown; PyArrow version unknown'; do
  if ! grep -Fqx "$version_line" "$arm_log"; then
    echo "FAIL unknown_versions: missing version line '$version_line'" >&2
    failures=$((failures + 1))
  fi
done

# --- partially skipped: permissive says INCOMPLETE, strict says NOT RUN ------
make_arm partial 0 duckdb
partial=$arm_dir
arm partial "$partial" 0 \
  'ok (INCOMPLETE: 1 reader(s) agreed, 2 not installed: spark-sql, pyiceberg)'
arm partial "$partial" 1 \
  'skipped (external reader(s) not installed: spark-sql, pyiceberg)'
# The engine names have to survive into the retained log, not just the line.
for engine in spark-sql pyiceberg; do
  if ! grep -Fq "FAIL: $engine not installed and --require-engines was given" \
    "$arm_log"; then
    echo "FAIL partial: the strict log does not name $engine" >&2
    failures=$((failures + 1))
  fi
done

# --- fully skipped: the run that used to read as green -----------------------
make_arm none 0
none=$arm_dir
arm none "$none" 0 \
  'ok (INCOMPLETE: 0 reader(s) agreed, 3 not installed: duckdb, spark-sql, pyiceberg)'
arm none "$none" 1 \
  'skipped (external reader(s) not installed: duckdb, spark-sql, pyiceberg)'

# --- an engine that ran and disagreed: red in both modes ---------------------
make_arm disagree 0 duckdb:rows=6 spark-sql pyiceberg
disagree=$arm_dir
arm disagree "$disagree" 0 'FAIL (1 check(s) failed, 2 reader(s) agreed)'
arm disagree "$disagree" 1 'FAIL (1 check(s) failed, 2 reader(s) agreed)'

# --- a correct nanosecond triple cannot mask the timestamp (task #1702) -------
#
# Every arm below reports rows, min_timestamp_ns and max_timestamp_ns exactly as
# the fixture printed them, and fails only on the microsecond `timestamp`. The
# other two readers agree, so the count in the status also shows the failure is
# attributed to the one reader that broke the contract.
ts_arm() { # <label> <engine spec> <expected FAIL substrings...>
  local label=$1 spec=$2 name=${2%%:*} others=() engine want
  shift 2
  # The other two engines answer correctly, and only once: stubbing the
  # overridden engine again with its defaults would silently undo the arm.
  for engine in duckdb spark-sql pyiceberg; do
    [ "$engine" = "$name" ] || others+=("$engine")
  done
  make_arm "$label" 0 "$spec" "${others[@]}"
  arm "$label" "$arm_dir" 1 'FAIL (1 check(s) failed, 2 reader(s) agreed)'
  for want in "$@"; do
    if ! grep -Fq "$want" "$arm_log"; then
      echo "FAIL $label: the log does not say '$want'" >&2
      failures=$((failures + 1))
    fi
  done
}

# One microsecond off in either bound. Off-by-one is the whole point: it is what
# a millisecond-truncating or nanosecond-misreading engine looks like once the
# nanosecond column is read correctly.
ts_arm ts_value "duckdb:min_us=$((fixture_min_us + 1))" \
  "FAIL: duckdb decoded timestamp [$((fixture_min_us + 1)), $fixture_max_us] us, expected [$fixture_min_us, $fixture_max_us]"
ts_arm ts_value_max "spark-sql:max_us=$((fixture_max_us - 1))" \
  "FAIL: spark decoded timestamp [$fixture_min_us, $((fixture_max_us - 1))] us, expected"
ts_arm ts_value_py "pyiceberg:min_us=0;max_us=0" \
  'FAIL: pyiceberg decoded timestamp [0, 0] us, expected'

# Right bounds, but rows inside the range that do not floor to their nanosecond.
ts_arm ts_rowwise 'duckdb:bad=3' \
  'FAIL: duckdb 3 row(s) where the decoded timestamp is not floor(timestamp_ns / 1000)'

# A null in either column: both are required.
ts_arm ts_nulls 'pyiceberg:nulls=2' \
  'FAIL: pyiceberg 2 null timestamp/timestamp_ns value(s)'

# The wrong type, with correct values: a nanosecond column, a zone-naive one, or
# a plain integer all read as numbers that happen to agree.
ts_arm ts_type_ns 'duckdb:type=TIMESTAMP_NS' \
  "FAIL: duckdb timestamp is of type 'TIMESTAMP_NS', not a microsecond UTC timestamp"
ts_arm ts_type_ntz 'spark-sql:type=timestamp_ntz' \
  "FAIL: spark timestamp is of type 'timestamp_ntz', not a microsecond UTC timestamp"
ts_arm ts_type_long 'pyiceberg:type=int64' \
  "FAIL: pyiceberg timestamp is of type 'int64', not a microsecond UTC timestamp"

# The read itself failing. Until 2026-09-07 DuckDB's timestamp probe was a
# separate query with `|| true`, so this arm was green on its triple alone.
ts_arm ts_read_duckdb 'duckdb:exit=1' \
  'FAIL: duckdb iceberg_scan errored'
ts_arm ts_read_py 'pyiceberg:exit=1' \
  'FAIL: pyiceberg could not read the table'

# --- and the fixture cannot hand the engines a wrong expectation -------------
#
# The microsecond expectation the readers are diffed against has to be the
# nanosecond bounds floored. A fixture that says otherwise, or that predates the
# microsecond assertions entirely, is refused before any engine runs -- so no
# engine can "agree" with it.
make_arm fixture_us "0:min_us=$fixture_max_us" duckdb spark-sql pyiceberg
arm fixture_us "$arm_dir" 1 'FAIL (1 check(s) failed, 0 reader(s) agreed)'
if ! grep -Fq \
  "FAIL: fixture expects timestamp [$fixture_max_us, $fixture_max_us] us, which is not timestamp_ns [$fixture_min, $fixture_max] floored" \
  "$arm_log"; then
  echo "FAIL fixture_us: the log does not name the inconsistent expectation" >&2
  failures=$((failures + 1))
fi

make_arm fixture_old '0:omit_us=1' duckdb spark-sql pyiceberg
arm fixture_old "$arm_dir" 1 'FAIL (1 check(s) failed, 0 reader(s) agreed)'
if ! grep -Fq "FAIL: could not parse the fixture's expected values" "$arm_log"; then
  echo "FAIL fixture_old: a fixture without microsecond expectations was accepted" >&2
  failures=$((failures + 1))
fi

# A disagreement alongside strict skips is red, not "not run": the counts have
# to stay separable or a real failure hides behind a missing engine.
make_arm mixed 0 duckdb:rows=6
mixed=$arm_dir
arm mixed "$mixed" 1 \
  'FAIL (1 check(s) failed, 0 reader(s) agreed, 2 not installed: spark-sql, pyiceberg)'

# --- the siglake half itself failing is red, engines or not ------------------
make_arm broken 1 duckdb spark-sql pyiceberg
broken=$arm_dir
arm broken "$broken" 1 'FAIL (1 check(s) failed, 0 reader(s) agreed)'

# --- the PyIceberg reader at least compiles ----------------------------------
#
# It is a heredoc inside the checker, so on a box without pyiceberg nothing
# reads it at all and a typo would surface only on the heavy gate. Compiling it
# needs neither pyiceberg nor pyarrow.
sed -n "/<<'PY'/,/^PY\$/p" scripts/check-external-timestamp-contract.sh |
  sed '1d;$d' >"$work/pyiceberg_read.py"
if [ ! -s "$work/pyiceberg_read.py" ]; then
  echo "FAIL: could not lift the PyIceberg reader out of the checker" >&2
  failures=$((failures + 1))
elif ! python3 -c \
  'import py_compile, sys; py_compile.compile(sys.argv[1], doraise=True)' \
  "$work/pyiceberg_read.py" >"$work/pyiceberg_compile.log" 2>&1; then
  sed 's/^/  /' "$work/pyiceberg_compile.log" >&2
  echo "FAIL: the checker's PyIceberg reader does not compile" >&2
  failures=$((failures + 1))
fi

# --- a checker that died before printing anything attributable ---------------
printf 'siglake binary: /nonexistent\n' >"$work/truncated.log"
expect 'truncated' 'FAIL (checker exited 137 with no FAIL line to attribute it to)' \
  "$(external_readers_status 1 "$work/truncated.log" 137)"
expect 'missing log' "FAIL (no external-readers log at $work/absent.log)" \
  "$(external_readers_status 1 "$work/absent.log" 0)"
# Exit 0 with every engine present and no engine result is not a pass.
printf '== DuckDB iceberg_scan ==\n' >"$work/silent.log"
expect 'silent' 'FAIL (no external reader result in the log)' \
  "$(external_readers_status 0 "$work/silent.log" 0)"

# --- and what ci-local.sh's report() then does with those statuses -----------
#
# The status strings above are only half the claim: `skipped (...)` has to
# become a FAIL line that counts as a job NOT RUN, which is what keeps the
# summary off ALL CHECKED JOBS GREEN (fail=1) while not claiming main is red
# (red=0). report() lives inline in ci-local.sh, so lift the function out by
# name and drive it.
awk '/^report\(\) \{$/,/^\}$/' scripts/ci-local.sh >"$work/report.sh"
if [ ! -s "$work/report.sh" ]; then
  echo "FAIL: could not lift report() out of scripts/ci-local.sh" >&2
  exit 1
fi

report_line() { # <strict> <status> ; prints "<line>|fail=<n> red=<n> skips=<n>"
  (
    # shellcheck source=/dev/null
    . "$work/report.sh"
    STRICT=$1 LOG_DIR=$work job_started=$SECONDS fail=0 red=0 strict_skips=0
    # In a subshell, not a command substitution: report()'s whole point is the
    # counters it sets, and $(...) would throw them away with the subshell.
    report external-readers "$2" >"$work/report.out" 2>/dev/null
    printf '%s|fail=%s red=%s skips=%s\n' \
      "$(sed 's/ ([0-9]*s)$//' "$work/report.out")" "$fail" "$red" "$strict_skips"
  )
}
expect 'report of a strict skip' \
  'external-readers   FAIL|fail=1 red=0 skips=1' \
  "$(report_line 1 'skipped (external reader(s) not installed: duckdb)')"
expect 'report of a permissive skip' \
  'external-readers   ok (INCOMPLETE: 0 reader(s) agreed, 3 not installed: duckdb)|fail=0 red=0 skips=0' \
  "$(report_line 0 'ok (INCOMPLETE: 0 reader(s) agreed, 3 not installed: duckdb)')"
expect 'report of a disagreement' \
  'external-readers   FAIL (1 check(s) failed, 2 reader(s) agreed)|fail=1 red=1 skips=0' \
  "$(report_line 1 'FAIL (1 check(s) failed, 2 reader(s) agreed)')"

# The defect was not in either function but in the job forgetting to use them:
# ci-local.sh invoked the checker bare and printed `ok (...)` unconditionally.
for wiring in 'external_readers_args "$STRICT"' 'external_readers_status "$STRICT"'; do
  if ! grep -Fq "$wiring" scripts/ci-local.sh; then
    echo "FAIL: scripts/ci-local.sh no longer calls $wiring" >&2
    failures=$((failures + 1))
  fi
done

if [ "$failures" -gt 0 ]; then
  echo "FAIL: $failures external-readers reporting check(s) failed" >&2
  exit 1
fi
echo "ok (complete, partially skipped, fully skipped, disagreeing and broken arms;"
echo "    timestamp value, row-wise, null, type, read-failure and fixture arms)"
