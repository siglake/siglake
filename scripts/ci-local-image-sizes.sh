#!/usr/bin/env bash
# Reporting for ci-local.sh's `docker` job: the two image sizes and the daemon's
# image store driver, and how the job's verdict and that measurement become one
# status line.
#
# Sourced by ci-local.sh and exercised without a daemon (a stub `docker` on
# PATH, fabricated log blocks) by check-image-sizes-report.sh.
#
# The sizes exist for the launch checklist's post-strip item (task #173):
# nightly run #7 built both stripped images and nobody could read the size
# afterwards, because the docker job reported only ok/FAIL. Full detail goes to
# the job's log, which survives the run in $LOG_DIR; the short form is echoed
# into the report line, so the number survives in a summary read the next
# morning.
#
# `image inspect`'s `.Size` is compressed content on a containerd image store
# (~4x below `image ls` there) and the same number on overlay2, so THE DRIVER IS
# PART OF THE MEASUREMENT: without it the recorded size is not comparable to any
# other run's. Until 2026-09-07 (task #1638) a `docker info` that printed no
# driver line — a daemon that answered `version` and then went away, an
# `--format` the daemon does not know — left the sizes in the line and the
# driver silently absent, and the reader had no way to tell that from a run
# where nobody looked. Both halves are now named in the line, each either with
# its value or as UNRECORDED with the reason.
#
# Informational only, in both directions: a missing driver line does not fail
# the docker job (decision #1322 — the verdict belongs to the build and to
# s3_mirror_pagination), and a red job still reports whatever the block did
# measure.

# The size block's opening line in the job log. The status is read back out of
# the log rather than kept in a variable, so the line and the retained log
# cannot disagree about what was measured.
IMAGE_SIZES_MARKER='--- image sizes ---'

# The image store driver the last size block in this log recorded, empty when
# `docker info` printed no driver line at all or printed an empty one.
image_sizes_driver() { # <log>
  [ -f "$1" ] || return 0
  awk -v marker="$IMAGE_SIZES_MARKER" -v prefix='image store driver: ' '
    $0 == marker { driver = ""; next }
    index($0, prefix) == 1 { driver = substr($0, length(prefix) + 1) }
    END { print driver }
  ' "$1"
}

# `<log> <siglake size> <operator size>` -> the short form for the report line.
# Never empty: a block that measured nothing says so.
image_sizes_status() { # <log> <siglake size> <operator size>
  local driver sizes
  driver=$(image_sizes_driver "$1")
  if [ -n "${2:-}" ] && [ -n "${3:-}" ]; then
    sizes=$(printf 'siglake %s, operator %s' "$2" "$3")
  else
    sizes='image sizes UNRECORDED (docker image ls gave no size for one or both images)'
  fi
  if [ -n "$driver" ]; then
    printf '%s; store driver %s\n' "$sizes" "$driver"
  else
    printf '%s; store driver UNRECORDED (docker info printed no image store driver line)\n' \
      "$sizes"
  fi
}

# Measure the two images this run built, appending the detail to the job log and
# printing the short form. One `docker image ls` per image on purpose: it takes
# at most one positional repository, and the two-image form is a usage error
# that cost one measurement.
docker_image_sizes() { # <log>
  local log=$1 siglake_size operator_size status
  {
    echo "$IMAGE_SIZES_MARKER"
    docker info --format 'image store driver: {{.Driver}}'
    docker image inspect --format '{{index .RepoTags 0}} {{.Size}} bytes (inspect)' \
      siglake:ci-local siglake-operator:ci-local
  } >>"$log" 2>&1
  siglake_size=$(docker image ls --format '{{.Size}}' siglake:ci-local 2>>"$log" | head -1)
  operator_size=$(docker image ls --format '{{.Size}}' siglake-operator:ci-local 2>>"$log" | head -1)
  status=$(image_sizes_status "$log" "$siglake_size" "$operator_size")
  echo "image sizes: $status" >>"$log"
  printf '%s' "$status"
}

# `<dk_ok> <preflight_ok> <size summary>` -> the docker job's status string.
# The measurement is appended to the verdict and never decides it; an empty
# summary is a run that never got as far as building both images.
docker_job_status() { # <dk_ok> <preflight_ok> <size summary>
  local sizes=${3:-}
  if [ "${1:-0}" = 1 ]; then
    printf 'ok (s3_mirror_pagination%s)\n' "${sizes:+; $sizes}"
  elif [ "${2:-1}" = 0 ]; then
    printf 'FAIL (port preflight%s)\n' "${sizes:+; $sizes}"
  else
    printf 'FAIL%s\n' "${sizes:+ ($sizes)}"
  fi
}
