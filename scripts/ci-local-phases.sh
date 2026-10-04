#!/usr/bin/env bash
# Elapsed-phase accounting for ci-local.sh's `docker` job.
#
# The job reports one number — `docker ok (...) (471s)` — and the nightly run
# that spent 471s there (run #171, 2026-10-03) left nobody able to say whether
# the time went into the two image builds, into compose coming up, into cargo
# compiling the live suites, or into the suites themselves. Each of those has a
# different answer, and three of them are not regressions at all.
#
# A phase is a wall-clock interval around one step of the job, named when it
# starts and recorded when it ends. The record goes into the job's own log as
# it happens, so a run killed mid-phase still shows how far it got, and the
# summary at the end is read BACK OUT of those records rather than kept in a
# variable: the log and the summary cannot then disagree about what was
# measured, the same rule ci-local-image-sizes.sh follows.
#
# Phases nest one level: a parent around a group (the five Postgres-only
# storage suites share a test binary) and a child around each member. Only
# top-level records are summed, so a child never counts twice, and the summary
# names the residual — the part of the job's total no phase claimed — instead
# of implying the named phases are the whole of it.
#
# What this does NOT measure directly is cargo's own split between compiling a
# test binary and running it: both happen inside one `cargo test` invocation,
# and splitting them would mean adding a `--no-run` build, which is a change to
# what the job runs. cargo already prints both (`Finished ... in 21.37s`,
# `test result: ... finished in 7.07s`), so those are summed from the log as a
# derived line, labelled as detail nested inside the test phases rather than as
# another phase.
#
# Sourced by ci-local.sh and exercised without a daemon by
# check-ci-local-phases.sh.
#
# Informational only: a phase helper never changes a verdict.
# ci_local_phase_end returns the status it was handed, so
# `ci_local_phase_end "$log" "$rc" || dk_ok=0` is the same gate the bare
# command was.

# The phase block's opening line in the job log. Records that follow the LAST
# one of these belong to this run; a caller-named log directory can carry a
# block from a previous attempt.
CI_LOCAL_PHASES_MARKER='--- docker phases ---'

# Stack of open phases: names and their $SECONDS at entry. $SECONDS is what
# report() measures the job with, so the parts and the total share a clock.
CI_LOCAL_PHASE_NAMES=()
CI_LOCAL_PHASE_STARTS=()

# Open a block in <log> and drop any phase left open by an earlier one.
ci_local_phases_reset() { # <log>
  CI_LOCAL_PHASE_NAMES=()
  CI_LOCAL_PHASE_STARTS=()
  printf '%s\n' "$CI_LOCAL_PHASES_MARKER" >>"$1"
}

ci_local_phase_begin() { # <name>
  CI_LOCAL_PHASE_NAMES+=("$1")
  CI_LOCAL_PHASE_STARTS+=("$SECONDS")
}

# Close the innermost open phase, record it, and hand the caller's status back
# unchanged. A close with nothing open is a wiring bug in the caller, not a
# silent no-op: it says so in the log and still preserves the status.
ci_local_phase_end() { # <log> <rc>
  local log=$1 rc=${2:-0} depth name started elapsed
  depth=${#CI_LOCAL_PHASE_NAMES[@]}
  if [ "$depth" -eq 0 ]; then
    printf 'phase UNBALANCED end with no phase open (rc=%s)\n' "$rc" >>"$log"
    return "$rc"
  fi
  name=${CI_LOCAL_PHASE_NAMES[depth - 1]}
  started=${CI_LOCAL_PHASE_STARTS[depth - 1]}
  # Reassign rather than `unset` the last element: unset leaves a sparse array
  # whose length no longer names the top of the stack.
  CI_LOCAL_PHASE_NAMES=("${CI_LOCAL_PHASE_NAMES[@]:0:depth - 1}")
  CI_LOCAL_PHASE_STARTS=("${CI_LOCAL_PHASE_STARTS[@]:0:depth - 1}")
  elapsed=$((SECONDS - started))
  if [ "$depth" -gt 1 ]; then
    # Inside a parent: recorded for the reader, excluded from the sum.
    printf '  phase %-34s %5ss rc=%s (nested)\n' "$name" "$elapsed" "$rc" >>"$log"
  else
    printf 'phase %-36s %5ss rc=%s\n' "$name" "$elapsed" "$rc" >>"$log"
  fi
  return "$rc"
}

# cargo's own subdurations, summed over the whole log: how much of the test
# phases was compilation and how much was the tests running. Both already
# printed by cargo before this file existed; this only adds them up.
ci_local_phase_derived() { # <log>
  local log=$1
  # A job that never reached a cargo invocation reports zeros, not nothing:
  # "no line said so" and "nobody looked" have to read differently.
  [ -f "$log" ] || log=/dev/null
  awk '
    /^ *Finished .* target\(s\) in [0-9.]+s$/ {
      sub(/s$/, "", $NF); compile += $NF; compiles++
    }
    /^test result: .* finished in [0-9.]+s$/ {
      sub(/s$/, "", $NF); run += $NF; runs++
    }
    END {
      printf "cargo compilation %.1fs over %d Finished line(s); ", compile, compiles
      printf "test execution %.1fs over %d result line(s)", run, runs
    }
  ' "$log"
}

# What the records in this log's last block add up to, against the job total.
# Top-level records only; `(nested)` ones are detail inside one of them.
ci_local_phases_report() { # <log> <docker total seconds>
  local log=$1 total=${2:-0} accounted residual pct leaked
  accounted=$(awk -v marker="$CI_LOCAL_PHASES_MARKER" '
    $0 == marker { sum = 0; next }
    /^phase [a-z0-9-]+ +[0-9]+s rc=/ { s = $3; sub(/s$/, "", s); sum += s }
    END { print sum + 0 }
  ' "$log")
  residual=$((total - accounted))
  if [ "$total" -gt 0 ]; then
    pct=$(awk -v r="$residual" -v t="$total" 'BEGIN { printf "%.1f", 100 * r / t }')
  else
    pct=0.0
  fi
  leaked=${#CI_LOCAL_PHASE_NAMES[@]}
  {
    printf 'phases accounted %ss of %ss docker total; residual %ss (%s%%)\n' \
      "$accounted" "$total" "$residual" "$pct"
    printf 'phases derived (nested in the test phases, not summed): %s\n' \
      "$(ci_local_phase_derived "$log")"
    if [ "$leaked" -gt 0 ]; then
      printf 'phases UNBALANCED: %s phase(s) never closed: %s\n' \
        "$leaked" "${CI_LOCAL_PHASE_NAMES[*]}"
    fi
  } >>"$log"
}
