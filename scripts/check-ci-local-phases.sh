#!/usr/bin/env bash
# Exercise ci-local.sh's docker phase accounting without a daemon: the real
# helpers from ci-local-phases.sh run against a writable log, with $SECONDS
# assigned between calls so an arm can be 471 seconds long and take none.
#
# The arms are the ones a wrong answer would hide: a phase that fails still
# leaves a record (a job that dies inside compose must not read as a job that
# measured nothing), the status a phase was handed comes back unchanged (the
# whole block is wired as `ci_local_phase_end ... || dk_ok=0`, so a swallowed
# status is a green gate over a red suite), nested records are not summed
# twice, and the residual names the part of the job total no phase claimed
# rather than being quietly folded into the last one.
#
# The wiring half matters as much as the helpers: a begin without its end
# leaves the stack open and every later phase nested, which is exactly the
# shape that reports a plausible total and attributes nothing.

set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/ci-local-phases.sh

work=$(mktemp -d "${TMPDIR:-/tmp}/siglake-ci-local-phases.XXXXXX")
trap 'rm -rf -- "$work"' EXIT

failures=0
expect() { # <label> <expected> <actual>
  if [ "$2" != "$3" ]; then
    echo "FAIL $1: expected '$2', got '$3'" >&2
    failures=$((failures + 1))
  fi
}
expect_log() { # <label> <log> <substring>
  if ! grep -Fq -- "$3" "$2"; then
    echo "FAIL $1: the log does not say '$3'" >&2
    sed 's/^/    /' "$2" >&2
    failures=$((failures + 1))
  fi
}
expect_not_log() { # <label> <log> <substring>
  if grep -Fq -- "$3" "$2"; then
    echo "FAIL $1: the log says '$3' and should not" >&2
    failures=$((failures + 1))
  fi
}

# `phase <name> <elapsed>s rc=<rc>` with the padding collapsed, so an arm can
# compare the record it expects without counting spaces.
records() { # <log>
  sed -n 's/^\(  \)\{0,1\}phase /phase /p' "$1" | tr -s ' '
}

# The same records with the elapsed number replaced by `N`. The arms that run
# the real block spend real time in it, and a $SECONDS tick landing inside one
# of sixteen stub steps is not a defect to fail on; which steps ran, in which
# order, nested or not, and with which status is.
shapes() { # <log>
  records "$1" | sed 's/ [0-9][0-9]*s rc=/ Ns rc=/'
}

# `<accounted> <total> <residual>` from the last accounting line in <log>.
accounting() { # <log>
  sed -n 's/^phases accounted \([0-9]*\)s of \([0-9]*\)s docker total; residual \(-*[0-9]*\)s.*/\1 \2 \3/p' \
    "$1" | tail -1
}

# --- a green run: every step named, the total fully attributed -------------
#
# The shape nightly run #171 could not produce: 471s of docker job, and a
# reader able to say where it went.
green="$work/green.log"
: >"$green"
ci_local_phases_reset "$green"
SECONDS=0
ci_local_phase_begin preflight
SECONDS=3
ci_local_phase_end "$green" 0
ci_local_phase_begin image-build
SECONDS=121
ci_local_phase_end "$green" 0
ci_local_phase_begin compose-up
SECONDS=168
ci_local_phase_end "$green" 0
ci_local_phase_begin s3-pagination
SECONDS=199
ci_local_phase_end "$green" 0
ci_local_phase_begin teardown
SECONDS=207
ci_local_phase_end "$green" 0
SECONDS=210
ci_local_phases_report "$green" 210
expect 'green records' \
  'phase preflight 3s rc=0
phase image-build 118s rc=0
phase compose-up 47s rc=0
phase s3-pagination 31s rc=0
phase teardown 8s rc=0' \
  "$(records "$green")"
expect_log 'green accounting' "$green" \
  'phases accounted 207s of 210s docker total; residual 3s (1.4%)'
expect_not_log 'a green run is balanced' "$green" 'UNBALANCED'

# --- a failing phase is still a record --------------------------------------
#
# compose never came up: the job stops there, and the retained log has to show
# how long it spent trying before it did.
red="$work/red.log"
: >"$red"
ci_local_phases_reset "$red"
SECONDS=0
ci_local_phase_begin preflight
SECONDS=2
ci_local_phase_end "$red" 0
ci_local_phase_begin image-build
SECONDS=140
ci_local_phase_end "$red" 0
ci_local_phase_begin compose-up
SECONDS=201
compose_rc=0
ci_local_phase_end "$red" 1 || compose_rc=$?
expect 'a failed phase hands its status back' 1 "$compose_rc"
ci_local_phase_begin teardown
SECONDS=205
ci_local_phase_end "$red" 0
SECONDS=206
ci_local_phases_report "$red" 206
expect 'red records' \
  'phase preflight 2s rc=0
phase image-build 138s rc=0
phase compose-up 61s rc=1
phase teardown 4s rc=0' \
  "$(records "$red")"
expect_log 'red accounting' "$red" \
  'phases accounted 205s of 206s docker total; residual 1s (0.5%)'

# --- the status is the caller's, in both directions -------------------------
#
# ci-local.sh writes `ci_local_phase_end "$dlog" "$rc" || dk_ok=0`; a helper
# that normalised the status would turn every red suite green.
status_log="$work/status.log"
: >"$status_log"
ci_local_phases_reset "$status_log"
for rc in 0 1 2 42 101 124; do
  ci_local_phase_begin "exit-$rc"
  got=0
  ci_local_phase_end "$status_log" "$rc" || got=$?
  expect "status $rc survives the phase" "$rc" "$got"
done
# And a green `dk_ok` gate is only taken on a zero status.
dk_ok=1
ci_local_phase_begin gate
ci_local_phase_end "$status_log" 0 || dk_ok=0
expect 'a green phase leaves the verdict alone' 1 "$dk_ok"
ci_local_phase_begin gate
ci_local_phase_end "$status_log" 7 || dk_ok=0
expect 'a red phase turns the verdict red' 0 "$dk_ok"

# --- nesting: recorded, not summed ------------------------------------------
#
# Five of the Postgres-only suites share one test binary, so the group is the
# phase and each suite is detail inside it. Summing both would attribute 2x
# the group's wall clock and turn the residual negative.
nested="$work/nested.log"
: >"$nested"
ci_local_phases_reset "$nested"
SECONDS=0
ci_local_phase_begin compose-up
SECONDS=40
ci_local_phase_end "$nested" 0
ci_local_phase_begin postgres-suites
SECONDS=55
ci_local_phase_begin jobs-postgres-ownership
SECONDS=75
ci_local_phase_end "$nested" 0
ci_local_phase_begin wal-ledger-postgres
SECONDS=90
ci_local_phase_end "$nested" 1 || true
SECONDS=95
ci_local_phase_end "$nested" 1 || true
SECONDS=100
ci_local_phases_report "$nested" 100
expect 'nested records' \
  'phase compose-up 40s rc=0
phase jobs-postgres-ownership 20s rc=0 (nested)
phase wal-ledger-postgres 15s rc=1 (nested)
phase postgres-suites 55s rc=1' \
  "$(records "$nested")"
# 40 + 55, not 40 + 55 + 20 + 15.
expect_log 'nested children are not summed' "$nested" \
  'phases accounted 95s of 100s docker total; residual 5s (5.0%)'
# The children are still in the log: excluded from the sum is not dropped.
expect_log 'nested children are recorded' "$nested" 'jobs-postgres-ownership'
expect_log 'a nested record says so' "$nested" '(nested)'

# --- an unbalanced block says so rather than reporting a plausible total ----
open="$work/open.log"
: >"$open"
ci_local_phases_reset "$open"
SECONDS=0
ci_local_phase_begin compose-up
SECONDS=30
ci_local_phases_report "$open" 60
expect_log 'an open phase is named' "$open" \
  'phases UNBALANCED: 1 phase(s) never closed: compose-up'
# Nothing was recorded, so nothing is claimed: the residual is the whole job.
expect_log 'an open phase claims nothing' "$open" \
  'phases accounted 0s of 60s docker total; residual 60s (100.0%)'
ci_local_phase_end "$open" 0

# A close with nothing open is a wiring bug, not a silent no-op, and it still
# hands the status back.
stray_rc=0
ci_local_phase_end "$open" 9 || stray_rc=$?
expect 'a stray end hands its status back' 9 "$stray_rc"
expect_log 'a stray end is named' "$open" 'phase UNBALANCED end with no phase open (rc=9)'

# --- a stale block in a caller-named log cannot be summed into this run -----
#
# `--log-dir` points the manager's runs at a directory it keeps with the run
# record, and a retried job writes into a log that already holds a block.
stale="$work/stale.log"
cat >"$stale" <<'STALE'
--- docker phases ---
phase preflight                            3s rc=0
phase image-build                        400s rc=0
STALE
ci_local_phases_reset "$stale"
SECONDS=0
ci_local_phase_begin preflight
SECONDS=4
ci_local_phase_end "$stale" 0
SECONDS=10
ci_local_phases_report "$stale" 10
expect_log 'a stale block is not this run' "$stale" \
  'phases accounted 4s of 10s docker total; residual 6s (60.0%)'

# --- derived cargo subdurations ---------------------------------------------
#
# cargo prints the compile and the run inside one `cargo test`; the block adds
# them up instead of pretending a phase measured them apart. The lines here are
# verbatim from nightly run #171's docker.log.
derived="$work/derived.log"
cat >"$derived" <<'CARGO'
   Compiling siglake-storage v0.2.1 (/x/crates/siglake-storage)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 21.37s
     Running tests/s3_mirror_pagination.rs (/x/debug/deps/s3_mirror_pagination-f7ae)
test native_s3_pages_resume_wrap_and_repair_behind_cursor ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 7.07s
    Finished `test` profile [unoptimized + debuginfo] target(s) in 4.63s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.50s
CARGO
expect 'derived cargo subdurations' \
  'cargo compilation 26.0s over 2 Finished line(s); test execution 19.6s over 2 result line(s)' \
  "$(ci_local_phase_derived "$derived")"
expect 'derived over a log with no cargo output' \
  'cargo compilation 0.0s over 0 Finished line(s); test execution 0.0s over 0 result line(s)' \
  "$(ci_local_phase_derived "$work/absent.log")"

# --- the docker job's own block, lifted and driven against stand-ins --------
#
# Wrapping sixteen steps in phases rearranged the job's control flow, and the
# only place that flow runs is a nightly `--all` gate with a daemon. Lift the
# block out of ci-local.sh and drive it over stub `docker`, `cargo`, up.sh and
# down.sh: the verdict on each path has to be the one it was before, and the
# records have to say which steps the run reached.
root=$PWD
block="$work/docker-block.sh"
awk '
  /^  # docker: the images CI publishes/ { inside = 1 }
  inside {
    print
    if (seen && $0 == "  fi") exit
    if ($0 ~ /skipped \(no docker daemon/) seen = 1
  }
' scripts/ci-local.sh >"$block"
if [ "$(wc -l <"$block")" -lt 100 ]; then
  echo "FAIL: could not lift the docker job out of scripts/ci-local.sh" >&2
  exit 1
fi

sandbox="$work/sandbox"
mkdir -p "$sandbox/scripts" "$sandbox/bin"
cat >"$sandbox/scripts/up.sh" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = --preflight-only ]; then
  echo "stub preflight"
  exit "${STUB_PREFLIGHT_RC:-0}"
fi
echo "==> docker compose up (build + start)"
exit "${STUB_UP_RC:-0}"
STUB
cat >"$sandbox/scripts/down.sh" <<'STUB'
#!/usr/bin/env bash
echo "stub down"
exit "${STUB_DOWN_RC:-0}"
STUB
cat >"$sandbox/scripts/compose-common.bash" <<'STUB'
SIGLAKE_OBJECT_STORE=${SIGLAKE_OBJECT_STORE:-minio}
SIGLAKE_S3_HOST_ENDPOINT=http://localhost:19000
SIGLAKE_S3_ACCESS_KEY=minioadmin
SIGLAKE_S3_SECRET_KEY=minioadmin
SIGLAKE_S3_REGION=us-east-1
STUB
cat >"$sandbox/scripts/ci-local-conditional-write-probe.sh" <<'STUB'
#!/usr/bin/env bash
echo "stub conditional-write probe"
exit "${STUB_PROBE_RC:-0}"
STUB
cat >"$sandbox/scripts/ci-local-conditional-write-agreement.sh" <<'STUB'
#!/usr/bin/env bash
echo "stub conditional-write agreement"
exit "${STUB_AGREEMENT_RC:-0}"
STUB
cat >"$sandbox/bin/docker" <<'STUB'
#!/usr/bin/env bash
case "${1:-}" in
  version) exit 0 ;;
  build) exit "${STUB_BUILD_RC:-0}" ;;
  *) exit 0 ;;
esac
STUB
# Enough of cargo's output for the conditional-write arm's 3-passed /
# one-result-line check, and for the derived subduration line.
cat >"$sandbox/bin/cargo" <<'STUB'
#!/usr/bin/env bash
printf '    Finished `test` profile [unoptimized + debuginfo] target(s) in 1.00s\n'
passed=3
# A conditional-write run that exits 0 having run fewer cases than the job
# requires: the arm the count check exists for.
if [ -n "${STUB_CARGO_SHORT_RESULTS:-}" ] && [[ "$*" == *conditional_write_live* ]]; then
  passed=1
fi
printf 'test result: ok. %s passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.50s\n' "$passed"
if [ -n "${STUB_CARGO_FAIL:-}" ] && [[ "$*" == *"$STUB_CARGO_FAIL"* ]]; then
  exit 101
fi
exit 0
STUB
chmod +x "$sandbox/scripts"/*.sh "$sandbox/bin"/*

awk '/^report\(\) \{$/,/^\}$/' scripts/ci-local.sh >"$sandbox/report.sh"
if [ ! -s "$sandbox/report.sh" ]; then
  echo "FAIL: could not lift report() out of scripts/ci-local.sh" >&2
  exit 1
fi

cat >"$sandbox/driver.sh" <<DRIVER
set -uo pipefail
. "$root/scripts/ci-local-phases.sh"
. "$sandbox/report.sh"
# Covered by check-image-sizes-report.sh against its own stub daemon; here it
# only has to leave the line and the log entry a real one would.
docker_image_sizes() {
  echo 'image sizes: stub' >>"\$1"
  printf 'siglake 1MB, operator 1MB; store driver overlay2'
}
docker_job_status() {
  local sizes=\${3:-}
  if [ "\${1:-0}" = 1 ]; then printf 'ok (s3_mirror_pagination%s)\n' "\${sizes:+; \$sizes}"
  elif [ "\${2:-1}" = 0 ]; then printf 'FAIL (port preflight%s)\n' "\${sizes:+; \$sizes}"
  else printf 'FAIL%s\n' "\${sizes:+ (\$sizes)}"; fi
}
ci_local_choose_compose_ingest_port() {
  echo 'stub port selection' >>"\$1"
  return \${STUB_PORT_RC:-0}
}
LOG_DIR=\$PWD/logs
mkdir -p "\$LOG_DIR"
STRICT=0 fail=0 red=0 strict_skips=0
. "$block"
printf 'VERDICT fail=%s red=%s dk_ok=%s preflight_ok=%s\n' \\
  "\$fail" "\$red" "\$dk_ok" "\$preflight_ok"
DRIVER

# One run of the lifted block. Sets `job_line`, `job_verdict` and
# `job_log`; every stub knob is passed as an environment assignment.
job_line= job_verdict= job_log=
run_docker_block() { # <label> [VAR=value ...]
  local label=$1 dir="$sandbox/run-$1"
  shift
  rm -rf -- "$dir"
  mkdir -p "$dir"
  cp -r "$sandbox/scripts" "$dir/scripts"
  job_log="$dir/logs/docker.log"
  local out
  out=$(cd "$dir" && env PATH="$sandbox/bin:$PATH" "$@" bash "$sandbox/driver.sh" 2>/dev/null)
  job_line=$(grep '^docker ' <<<"$out" | sed 's/ ([0-9]*s)$//')
  job_verdict=$(grep '^VERDICT ' <<<"$out")
}

run_docker_block green
expect 'green docker verdict' \
  'docker             ok (s3_mirror_pagination; siglake 1MB, operator 1MB; store driver overlay2)' \
  "$job_line"
expect 'green docker counters' 'VERDICT fail=0 red=0 dk_ok=1 preflight_ok=1' "$job_verdict"
expect 'green docker phases' \
  'phase preflight Ns rc=0
phase image-build Ns rc=0
phase image-sizes Ns rc=0
phase compose-up Ns rc=0
phase s3-pagination Ns rc=0
phase conditional-probe Ns rc=0
phase conditional-live Ns rc=0
phase conditional-agreement Ns rc=0
phase jobs-postgres-ownership Ns rc=0 (nested)
phase local-commit-mark-postgres Ns rc=0 (nested)
phase wal-ledger-postgres Ns rc=0 (nested)
phase eligible-claim-postgres Ns rc=0 (nested)
phase sharded-claim-postgres Ns rc=0 (nested)
phase watermark-transaction-postgres Ns rc=0 (nested)
phase postgres-suites Ns rc=0
phase teardown Ns rc=0' \
  "$(shapes "$job_log")"
expect_not_log 'a green docker job is balanced' "$job_log" 'UNBALANCED'
# The card's acceptance, on a job whose steps are stubs: nearly all of the
# total is attributed. Over sixteen instant steps the residual is the report
# call itself, so the bound is a second rather than a percentage.
read -r acc tot res <<<"$(accounting "$job_log")"
if [ -z "${res:-}" ]; then
  echo "FAIL: the green docker job wrote no accounting line" >&2
  failures=$((failures + 1))
elif [ "$res" -lt 0 ] || [ "$res" -gt 1 ] || [ "$((tot - acc))" -ne "$res" ]; then
  echo "FAIL: accounted ${acc}s of ${tot}s leaves ${res}s unattributed" >&2
  failures=$((failures + 1))
fi

# A port this box cannot publish: the job stops before up.sh is asked for
# anything, and the preflight phase is the only record.
run_docker_block port STUB_PORT_RC=1
expect 'port-preflight verdict' 'docker             FAIL (port preflight)' "$job_line"
expect 'port-preflight counters' 'VERDICT fail=1 red=1 dk_ok=0 preflight_ok=0' "$job_verdict"
expect 'port-preflight phases' 'phase preflight Ns rc=1' "$(shapes "$job_log")"
expect_not_log 'a rejected port never ran preflight-only' "$job_log" 'stub preflight'

# up.sh's own preflight refused: same verdict, and the record says the phase
# reached it.
run_docker_block preflight STUB_PREFLIGHT_RC=1
expect 'preflight verdict' 'docker             FAIL (port preflight)' "$job_line"
expect 'preflight phases' 'phase preflight Ns rc=1' "$(shapes "$job_log")"
expect_log 'preflight ran up.sh' "$job_log" 'stub preflight'

# A failed image build stops before compose and before the sizes, and still
# tears down.
run_docker_block build STUB_BUILD_RC=1
expect 'build verdict' 'docker             FAIL' "$job_line"
expect 'build counters' 'VERDICT fail=1 red=1 dk_ok=0 preflight_ok=1' "$job_verdict"
expect 'build phases' \
  'phase preflight Ns rc=0
phase image-build Ns rc=1
phase compose-up Ns rc=0
phase s3-pagination Ns rc=0
phase conditional-probe Ns rc=0
phase conditional-live Ns rc=0
phase conditional-agreement Ns rc=0
phase jobs-postgres-ownership Ns rc=0 (nested)
phase local-commit-mark-postgres Ns rc=0 (nested)
phase wal-ledger-postgres Ns rc=0 (nested)
phase eligible-claim-postgres Ns rc=0 (nested)
phase sharded-claim-postgres Ns rc=0 (nested)
phase watermark-transaction-postgres Ns rc=0 (nested)
phase postgres-suites Ns rc=0
phase teardown Ns rc=0' \
  "$(shapes "$job_log")"

# The stack never came up: no suite ran, teardown still did, and the records
# say exactly that rather than leaving 400 unexplained seconds.
run_docker_block compose STUB_UP_RC=1
expect 'compose verdict' \
  'docker             FAIL (siglake 1MB, operator 1MB; store driver overlay2)' "$job_line"
expect 'compose counters' 'VERDICT fail=1 red=1 dk_ok=0 preflight_ok=1' "$job_verdict"
expect 'compose phases' \
  'phase preflight Ns rc=0
phase image-build Ns rc=0
phase image-sizes Ns rc=0
phase compose-up Ns rc=1
phase teardown Ns rc=0' \
  "$(shapes "$job_log")"
expect_log 'a failed compose still tore down' "$job_log" 'stub down'

# One Postgres suite red: the suite's own record is red, its group's record is
# red, and the suites after it still ran — the job reports every failure it
# can see in one run, as it did before phases existed.
run_docker_block suite STUB_CARGO_FAIL=wal_ledger_postgres
expect 'red suite verdict' 'docker             FAIL (siglake 1MB, operator 1MB; store driver overlay2)' \
  "$job_line"
expect 'red suite counters' 'VERDICT fail=1 red=1 dk_ok=0 preflight_ok=1' "$job_verdict"
expect 'red suite records' \
  'phase wal-ledger-postgres Ns rc=101 (nested)' \
  "$(shapes "$job_log" | grep wal-ledger)"
expect 'the group carries its child failure' \
  'phase postgres-suites Ns rc=1' \
  "$(shapes "$job_log" | grep postgres-suites)"
expect 'the suites after a red one still ran' \
  'phase eligible-claim-postgres Ns rc=0 (nested)
phase sharded-claim-postgres Ns rc=0 (nested)
phase watermark-transaction-postgres Ns rc=0 (nested)' \
  "$(shapes "$job_log" | grep -E 'eligible-claim|sharded-claim|watermark')"

# cargo exited 0 and ran one case where the job requires three. The job was
# already red on that count; the phase record has to be red with it, or the
# log shows a green conditional-write phase inside a red job.
run_docker_block short_results STUB_CARGO_SHORT_RESULTS=1
expect 'short conditional-write verdict' \
  'docker             FAIL (siglake 1MB, operator 1MB; store driver overlay2)' "$job_line"
expect 'short conditional-write record' 'phase conditional-live Ns rc=1' \
  "$(shapes "$job_log" | grep conditional-live)"
expect_log 'short conditional-write reason' "$job_log" \
  'conditional-write live tests FAIL (1 passed, 0 failed, 1 result lines)'

# Teardown is part of the verdict, and its phase is the record of it.
run_docker_block teardown STUB_DOWN_RC=1
expect 'red teardown verdict' \
  'docker             FAIL (siglake 1MB, operator 1MB; store driver overlay2)' "$job_line"
expect 'red teardown record' 'phase teardown Ns rc=1' "$(shapes "$job_log" | grep teardown)"

# --- the wiring, which is where this kind of defect actually lives ----------
for wiring in \
  '. scripts/ci-local-phases.sh' \
  'ci_local_phases_reset "$dlog"' \
  'ci_local_phases_report "$dlog" "$((SECONDS - job_started))"'; do
  if ! grep -Fq "$wiring" scripts/ci-local.sh; then
    echo "FAIL: scripts/ci-local.sh no longer has: $wiring" >&2
    failures=$((failures + 1))
  fi
done

# Every begin has an end. An unmatched begin nests everything after it, which
# reports a believable total and attributes almost none of it.
begins=$(grep -c 'ci_local_phase_begin ' scripts/ci-local.sh || true)
ends=$(grep -c 'ci_local_phase_end "\$dlog"' scripts/ci-local.sh || true)
if [ "$begins" -lt 10 ]; then
  echo "FAIL: only $begins phases in the docker job; the steps it runs are not covered" >&2
  failures=$((failures + 1))
fi
if [ "$begins" -ne "$ends" ]; then
  echo "FAIL: $begins ci_local_phase_begin against $ends ci_local_phase_end" >&2
  failures=$((failures + 1))
fi

# The steps the card names. A phase per step, by the name the reader of the
# retained log will look for.
for phase in preflight image-build image-sizes compose-up s3-pagination \
  conditional-probe conditional-live postgres-suites teardown; do
  if ! grep -Fq "ci_local_phase_begin $phase" scripts/ci-local.sh; then
    echo "FAIL: the docker job has no '$phase' phase" >&2
    failures=$((failures + 1))
  fi
done

# Teardown and the report are outside the `compose came up` branch: a job that
# failed earlier still tears down and still reports what it measured.
if ! awk '/ci_local_phase_begin teardown/ { t = NR }
          /ci_local_phases_report/ { r = NR }
          /scripts\/down.sh >>"\$dlog"/ { d = NR }
          END { exit !(t && d && r && t < d && d < r) }' scripts/ci-local.sh; then
  echo "FAIL: teardown is no longer a phase that precedes the phase report" >&2
  failures=$((failures + 1))
fi

# scripts/up.sh: the compose step's own elapsed line on both paths, and the
# loadgen command labelled as guidance. The printed command was read as
# something the script had run.
for up_line in \
  'compose up (build + start) took $((SECONDS - compose_started))s' \
  'compose up (build + start) failed after $((SECONDS - compose_started))s' \
  'Drive load — NOT RUN by this script'; do
  if ! grep -Fq "$up_line" scripts/up.sh; then
    echo "FAIL: scripts/up.sh no longer has: $up_line" >&2
    failures=$((failures + 1))
  fi
done
if grep -Fq 'Drive load (next phase):' scripts/up.sh; then
  echo "FAIL: scripts/up.sh still presents loadgen as a phase it runs" >&2
  failures=$((failures + 1))
fi
# Guidance stays guidance: nothing in up.sh executes loadgen.
if grep -En '^[^#]*loadgen\.sh' scripts/up.sh | grep -qv 'scripts/loadgen.sh --eps'; then
  echo "FAIL: scripts/up.sh looks like it now runs loadgen" >&2
  failures=$((failures + 1))
fi

if [ "$failures" -gt 0 ]; then
  echo "FAIL: $failures phase-accounting check(s) failed" >&2
  exit 1
fi
echo "ok (green and failing records, status preservation, nesting excluded from"
echo "    the sum, residual, unbalanced blocks, stale block, derived cargo"
echo "    subdurations; the lifted docker job over stand-ins on eight paths;"
echo "    its wiring, and up.sh's elapsed lines and loadgen labelling)"
