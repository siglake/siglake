#!/usr/bin/env bash
# Exercise ci-local.sh's docker size reporting without a daemon: a stub `docker`
# on PATH answers `info`, `image inspect` and `image ls`, and the real
# docker_image_sizes / image_sizes_status / docker_job_status run against it.
#
# The arms are the ones the reporting has to tell apart (2026-09-07, task
# #1638): both halves measured, a daemon that printed no image store driver
# line, one whose driver is empty, one or both `image ls` sizes missing, and a
# log that already carried an older block. A size on a containerd image store is
# ~4x below the same image's size on overlay2, so a size recorded without its
# driver is not comparable to anything — it used to vanish from the line with
# nothing saying it had.
#
# The verdict half is pinned too: the measurement, present or UNRECORDED, never
# turns an ok docker job red and never hides a red one (decision #1322).

set -euo pipefail

cd "$(dirname "$0")/.."
. scripts/ci-local-image-sizes.sh

work=$(mktemp -d "${TMPDIR:-/tmp}/siglake-image-sizes.XXXXXX")
trap 'rm -rf -- "$work"' EXIT

bash_bin=$(command -v bash)

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
    failures=$((failures + 1))
  fi
}

# A `docker` that answers the three subcommands the block runs. `driver` is the
# `docker info --format` output (`-` for a daemon that fails without printing
# one), `ls` the pipe-separated `image ls` answers per image (`-` for none).
make_docker_stub() { # <dir> <driver> <ls sizes>
  local dir=$1 driver=$2 ls=$3
  mkdir -p "$dir"
  cat >"$dir/docker" <<STUB
#!$bash_bin
case "\$1 \$2" in
  'info --format')
    if [ '$driver' = - ]; then
      echo 'Cannot connect to the Docker daemon at unix:///var/run/docker.sock.' >&2
      exit 1
    fi
    printf 'image store driver: %s\n' '$driver'
    ;;
  'image inspect')
    printf 'siglake:ci-local 41000000 bytes (inspect)\n'
    printf 'siglake-operator:ci-local 29000000 bytes (inspect)\n'
    ;;
  'image ls')
    if [ '$ls' = - ]; then
      echo 'Error response from daemon: no such image' >&2
      exit 1
    fi
    case "\$*" in
      *siglake-operator:ci-local) printf '%s\n' "\$(cut -d'|' -f2 <<<'$ls')" ;;
      *) printf '%s\n' "\$(cut -d'|' -f1 <<<'$ls')" ;;
    esac
    ;;
  *) echo "stub docker: unexpected \$*" >&2; exit 2 ;;
esac
STUB
  chmod +x "$dir/docker"
}

# One run of the real block against a stub daemon. Sets `arm_log` and
# `arm_status`; the caller's own verdict variable is passed in so an arm can
# show the measurement did not touch it.
arm_log= arm_status= arm_dk_ok=
run_block() { # <label> <driver> <ls sizes> [seed line]
  local dir="$work/$1"
  arm_log="$work/$1.log"
  make_docker_stub "$dir" "$2" "$3"
  : >"$arm_log"
  [ $# -lt 4 ] || printf '%s\n' "$4" >>"$arm_log"
  arm_dk_ok=1
  arm_status=$(PATH="$dir:$PATH" docker_image_sizes "$arm_log")
}

# --- both halves measured ----------------------------------------------------
run_block complete overlay2 '41.2MB|29.4MB'
expect complete 'siglake 41.2MB, operator 29.4MB; store driver overlay2' "$arm_status"
expect 'complete leaves the verdict alone' 1 "$arm_dk_ok"
# The line is the short form; the log keeps the detail the line cannot carry.
expect_log complete "$arm_log" '--- image sizes ---'
expect_log complete "$arm_log" 'image store driver: overlay2'
expect_log complete "$arm_log" 'siglake:ci-local 41000000 bytes (inspect)'
expect_log complete "$arm_log" 'siglake-operator:ci-local 29000000 bytes (inspect)'
# The log and the line agree, so a summary read later can be checked against it.
expect_log complete "$arm_log" \
  'image sizes: siglake 41.2MB, operator 29.4MB; store driver overlay2'

# A containerd store reports a different driver and much smaller numbers for the
# same images; the driver is what makes the two comparable at all.
run_block containerd overlayfs '11.9MB|8.1MB'
expect containerd 'siglake 11.9MB, operator 8.1MB; store driver overlayfs' "$arm_status"

# --- no driver line: the regression this file exists for ---------------------
run_block no_driver - '41.2MB|29.4MB'
expect no_driver \
  'siglake 41.2MB, operator 29.4MB; store driver UNRECORDED (docker info printed no image store driver line)' \
  "$arm_status"
expect 'a missing driver leaves the verdict alone' 1 "$arm_dk_ok"
# Why it is missing has to reach the retained log, or the line names a gap
# nothing explains.
expect_log no_driver "$arm_log" 'Cannot connect to the Docker daemon'

# An `--format` the daemon answered with nothing is the same gap: a line that
# parses and carries no driver must not read as a recorded one.
run_block empty_driver '' '41.2MB|29.4MB'
expect empty_driver \
  'siglake 41.2MB, operator 29.4MB; store driver UNRECORDED (docker info printed no image store driver line)' \
  "$arm_status"

# --- the sizes themselves missing --------------------------------------------
run_block no_sizes overlay2 -
expect no_sizes \
  'image sizes UNRECORDED (docker image ls gave no size for one or both images); store driver overlay2' \
  "$arm_status"
expect_log no_sizes "$arm_log" 'Error response from daemon: no such image'

run_block one_size overlay2 '41.2MB|'
expect one_size \
  'image sizes UNRECORDED (docker image ls gave no size for one or both images); store driver overlay2' \
  "$arm_status"

run_block nothing - -
expect nothing \
  'image sizes UNRECORDED (docker image ls gave no size for one or both images); store driver UNRECORDED (docker info printed no image store driver line)' \
  "$arm_status"

# --- an older block in the same log cannot supply this run's driver ----------
#
# A caller that names its own --log-dir (the manager, keeping the block with the
# run record) can hand the job a log that already holds a block from a previous
# attempt. The status describes THIS block.
run_block stale_driver - '41.2MB|29.4MB' 'image store driver: overlay2'
expect stale_driver \
  'siglake 41.2MB, operator 29.4MB; store driver UNRECORDED (docker info printed no image store driver line)' \
  "$arm_status"

# And the pure reader on its own, over a log the block never wrote to.
expect 'no log at all' \
  'image sizes UNRECORDED (docker image ls gave no size for one or both images); store driver UNRECORDED (docker info printed no image store driver line)' \
  "$(image_sizes_status "$work/absent.log" '' '')"

# --- the verdict is the build's, never the measurement's ---------------------
sizes='siglake 41.2MB, operator 29.4MB; store driver overlay2'
unrecorded='siglake 41.2MB, operator 29.4MB; store driver UNRECORDED (docker info printed no image store driver line)'
expect 'green with a measurement' \
  "ok (s3_mirror_pagination; $sizes)" "$(docker_job_status 1 1 "$sizes")"
expect 'green with an UNRECORDED half' \
  "ok (s3_mirror_pagination; $unrecorded)" "$(docker_job_status 1 1 "$unrecorded")"
expect 'green before the block existed' \
  'ok (s3_mirror_pagination)' "$(docker_job_status 1 1 '')"
expect 'red keeps the sizes it did measure' \
  "FAIL ($sizes)" "$(docker_job_status 0 1 "$sizes")"
expect 'red with nothing measured' 'FAIL' "$(docker_job_status 0 1 '')"
expect 'preflight red' 'FAIL (port preflight)' "$(docker_job_status 0 0 '')"

# --- and what ci-local.sh's report() does with those statuses ----------------
#
# An UNRECORDED half must not spend the exit code: report() lives inline in
# ci-local.sh, so lift it by name and drive it, the way
# check-external-readers-report.sh does.
awk '/^report\(\) \{$/,/^\}$/' scripts/ci-local.sh >"$work/report.sh"
if [ ! -s "$work/report.sh" ]; then
  echo "FAIL: could not lift report() out of scripts/ci-local.sh" >&2
  exit 1
fi
report_line() { # <status> ; prints "<line>|fail=<n> red=<n>"
  (
    # shellcheck source=/dev/null
    . "$work/report.sh"
    STRICT=0 LOG_DIR=$work job_started=$SECONDS fail=0 red=0 strict_skips=0
    # In a subshell, not a command substitution: report()'s whole point is the
    # counters it sets, and $(...) would throw them away with the subshell.
    report docker "$1" >"$work/report.out" 2>/dev/null
    printf '%s|fail=%s red=%s\n' \
      "$(sed 's/ ([0-9]*s)$//' "$work/report.out")" "$fail" "$red"
  )
}
expect 'report of an UNRECORDED driver' \
  "docker             ok (s3_mirror_pagination; $unrecorded)|fail=0 red=0" \
  "$(report_line "$(docker_job_status 1 1 "$unrecorded")")"
expect 'report of a red build that measured' \
  "docker             FAIL ($sizes)|fail=1 red=1" \
  "$(report_line "$(docker_job_status 0 1 "$sizes")")"

# --- the wiring, which is where this kind of defect actually lives -----------
for wiring in \
  '. scripts/ci-local-image-sizes.sh' \
  'image_sizes=$(docker_image_sizes "$dlog")' \
  'report docker "$(docker_job_status "$dk_ok" "$preflight_ok" "$image_sizes")"'; do
  if ! grep -Fq "$wiring" scripts/ci-local.sh; then
    echo "FAIL: scripts/ci-local.sh no longer has: $wiring" >&2
    failures=$((failures + 1))
  fi
done

# The block has to land in the run's own log directory, which survives EXIT and
# which a caller can point at its own run record with --log-dir: the card this
# file comes from was filed because the measurement was nearly lost to a log
# that the next run overwrote. Cleanup may remove the run's scratch directory
# and active-run pointer, but nothing else from the caller-owned log directory.
if ! grep -Fq 'dlog="$LOG_DIR/docker.log"' scripts/ci-local.sh; then
  echo "FAIL: the docker job no longer writes its log into \$LOG_DIR" >&2
  failures=$((failures + 1))
fi
expected_cleanup='cleanup_ci_local() {
  rm -rf -- "$LOG_DIR/tmp"
  [ -z "$run_pointer" ] || rm -f -- "$run_pointer"
}'
actual_cleanup=$(sed -n '/^cleanup_ci_local() {$/,/^}$/p' scripts/ci-local.sh)
if [ "$actual_cleanup" != "$expected_cleanup" ] \
  || ! grep -Fqx 'trap cleanup_ci_local EXIT' scripts/ci-local.sh; then
  echo "FAIL: the EXIT trap no longer removes only \$LOG_DIR/tmp and the run pointer" >&2
  failures=$((failures + 1))
fi

if [ "$failures" -gt 0 ]; then
  echo "FAIL: $failures image-size reporting check(s) failed" >&2
  exit 1
fi
echo "ok (measured, no driver line, empty driver, missing sizes, stale block;"
echo "    verdict composition and report() arms)"
