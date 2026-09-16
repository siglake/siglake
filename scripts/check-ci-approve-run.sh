#!/usr/bin/env bash
# Drive scripts/ci-approve-run.sh against a stand-in `gh`: no network, no
# token, no repository.
#
# The script is the last gate between a maintainer's label and a fork's code
# running on a runner, and its job is to REFUSE. So the arms here are mostly
# refusals: the head moved between the label and the call, the head repository
# changed, the queued run never appeared, the approval itself was rejected.
# The two that release something prove it releases exactly one run id -- the
# one whose head commit and head repository were reviewed -- and that a run
# already past "waiting for approval" is reported rather than re-approved.
#
# A stand-in `gh` records every call, so "nothing was released" is checked by
# the absence of the POST rather than by the exit status alone: a refusal that
# approves first and exits 1 afterwards would pass the status check.

set -uo pipefail

cd "$(dirname "$0")/.."

work=$(mktemp -d "${TMPDIR:-/tmp}/siglake-ci-approve.XXXXXX")
trap 'rm -rf -- "$work"' EXIT

bash_bin=$(command -v bash)
mkdir -p "$work/bin"

SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
OTHER=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
FORK=contributor/siglake

# The stand-in. `pulls` answers from $GH_PULL, the runs query from $GH_RUNS
# (one TSV row per line, `-` for none), the approve POST exits $GH_APPROVE_RC.
# Every argument list is appended to $GH_CALLS.
cat >"$work/bin/gh" <<STUB
#!$bash_bin
printf '%s\n' "\$*" >>"\$GH_CALLS"
case "\$*" in
  *"/pulls/"*)
    case "\$*" in
      *".head.sha"*) ;;
      *) echo "the pulls call no longer asks for .head.sha" >&2; exit 3 ;;
    esac
    printf '%s\n' "\$GH_PULL"
    ;;
  *"-X POST"*"/approve")
    exit "\${GH_APPROVE_RC:-0}"
    ;;
  *"/actions/runs?"*)
    [ "\$GH_RUNS" = - ] || printf '%s\n' "\$GH_RUNS"
    ;;
  *) echo "unexpected gh call: \$*" >&2; exit 4 ;;
esac
STUB
chmod +x "$work/bin/gh"

failures=0
calls=""

# <label> <expected rc> <gh pull answer> <gh runs answer> [approve rc]
arm() {
  local label=$1 want_rc=$2 pull=$3 runs=$4 approve_rc=${5:-0}
  calls="$work/calls.$$"
  : >"$calls"
  out="$work/out.$$"
  PATH="$work/bin:$PATH" \
  GH_CALLS="$calls" GH_PULL="$pull" GH_RUNS="$runs" GH_APPROVE_RC="$approve_rc" \
  GH_REPO=limnion-ai/siglake PR_NUMBER=77 AUTHORIZED_SHA="$SHA" HEAD_REPO="$FORK" \
  CI_APPROVE_POLL_TRIES=2 CI_APPROVE_POLL_SLEEP=0 \
    scripts/ci-approve-run.sh >"$out" 2>&1
  local rc=$?
  if [ "$rc" -ne "$want_rc" ]; then
    echo "FAIL $label: exit $rc, expected $want_rc" >&2
    sed 's/^/    /' "$out" >&2
    failures=$((failures + 1))
  fi
}

expect_out() { # <label> <substring>
  if ! grep -Fq -- "$2" "$out"; then
    echo "FAIL $1: the output does not say '$2'" >&2
    sed 's/^/    /' "$out" >&2
    failures=$((failures + 1))
  fi
}
expect_approved() { # <label> <run id>
  if ! grep -Fq -- "/actions/runs/$2/approve" "$calls"; then
    echo "FAIL $1: run $2 was not approved" >&2
    sed 's/^/    /' "$calls" >&2
    failures=$((failures + 1))
  fi
}
expect_nothing_approved() { # <label>
  if grep -Fq -- "/approve" "$calls"; then
    echo "FAIL $1: something was approved anyway" >&2
    sed 's/^/    /' "$calls" >&2
    failures=$((failures + 1))
  fi
}

# The reviewed commit, queued and waiting. One POST, for that run id.
arm "the reviewed head" 0 "$(printf '%s\t%s' "$SHA" "$FORK")" \
  "$(printf '101\taction_required\tci\t%s\t.github/workflows/ci.yml' "$FORK")"
expect_out "the reviewed head" "approved run 101"
# The record says which workflow file was released, not just its display name:
# a fork's run uses the fork's copy of .github/workflows, so "ci" is a name it
# could have chosen.
expect_out "the reviewed head" "(ci, .github/workflows/ci.yml)"
expect_approved "the reviewed head" 101
expect_out "the reviewed head" "released 1 of 1 run(s)"

# A push landed between the label and this call. The label named a commit that
# is no longer on offer, so nothing runs.
arm "the head moved" 1 "$(printf '%s\t%s' "$OTHER" "$FORK")" \
  "$(printf '101\taction_required\tci\t%s\t.github/workflows/ci.yml' "$FORK")"
expect_out "the head moved" "moved from aaaaaaaaaaaa to bbbbbbbbbbbb"
expect_out "the head moved" "re-add ci:run"
expect_nothing_approved "the head moved"

# The pull request now points at a different fork.
arm "the head repository changed" 1 "$(printf '%s\t%s' "$SHA" "someone-else/siglake")" \
  "$(printf '101\taction_required\tci\t%s\t.github/workflows/ci.yml' "$FORK")"
expect_out "the head repository changed" "head repository is now 'someone-else/siglake'"
expect_nothing_approved "the head repository changed"

# The same commit under a second pull request from a different fork. Only the
# reviewed head repository's run is released.
arm "a same-commit run from another fork" 0 "$(printf '%s\t%s' "$SHA" "$FORK")" \
  "$(printf '101\taction_required\tci\t%s\t.github/workflows/ci.yml\n202\taction_required\tci\tmallory/siglake\t.github/workflows/ci.yml' "$FORK")"
expect_approved "a same-commit run from another fork" 101
expect_out "a same-commit run from another fork" "skipping run 202"
if grep -Fq -- "/actions/runs/202/approve" "$calls"; then
  echo "FAIL a same-commit run from another fork: run 202 was approved" >&2
  failures=$((failures + 1))
fi

# GitHub's fork-approval setting did not hold this run (an organization
# member's fork). Nothing to release, and that is not a failure.
arm "a run already past approval" 0 "$(printf '%s\t%s' "$SHA" "$FORK")" \
  "$(printf '101\tin_progress\tci\t%s\t.github/workflows/ci.yml' "$FORK")"
expect_out "a run already past approval" "is in_progress: nothing to release"
expect_nothing_approved "a run already past approval"

# The label arrived before the run did. A refusal with a remedy beats a pass.
arm "no queued run at all" 1 "$(printf '%s\t%s' "$SHA" "$FORK")" -
expect_out "no queued run at all" "no pull_request run for $FORK"
expect_nothing_approved "no queued run at all"

# The approval itself was refused -- the 403 this design has to survive.
arm "the approval is refused" 1 "$(printf '%s\t%s' "$SHA" "$FORK")" \
  "$(printf '101\taction_required\tci\t%s\t.github/workflows/ci.yml' "$FORK")" 1
expect_out "the approval is refused" "CI_AUTHORIZE_TOKEN"

# A pull request the API cannot answer for.
arm "the head cannot be re-read" 1 "" "$(printf '101\taction_required\tci\t%s\t.github/workflows/ci.yml' "$FORK")"
expect_nothing_approved "the head cannot be re-read"

if [ "$failures" -ne 0 ]; then
  echo "FAIL $failures ci-approve-run arm(s)" >&2
  exit 1
fi
echo "ok   8 ci-approve-run arms (2 releases, 5 refusals, 1 nothing-to-do)"
