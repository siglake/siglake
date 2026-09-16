#!/usr/bin/env bash
# Release the queued CI run for ONE reviewed commit, and refuse anything else.
#
# Called by .github/workflows/ci-authorize.yml after
# scripts/check-ci-authorization.py has decided that a trusted login labelled
# a fork's head. Everything this script does is a narrowing:
#
#   1. re-read the pull request's head from the API. The labelling event is a
#      snapshot; if the fork pushed between the label and this call, the
#      commit a maintainer read is no longer the commit on offer, and this
#      exits red without approving anything. The `synchronize` arm of the
#      controller then removes the label, so the next release is a deliberate
#      act again.
#   2. approve by HEAD COMMIT, never by pull request number. A queued run is
#      pinned to its commit, so an approval cannot be inherited by a push that
#      lands a second later.
#   3. approve only runs whose head repository is the one that was reviewed --
#      two pull requests can carry the same commit.
#
# A run that is already past "waiting for approval" needs nothing: the
# repository's fork-approval setting did not hold it (an organization member's
# fork, say), and this reports that and exits 0 rather than inventing a
# failure.
#
# Env: GH_TOKEN GH_REPO PR_NUMBER AUTHORIZED_SHA HEAD_REPO.
# scripts/check-ci-approve-run.sh drives every arm above against a stand-in
# `gh`, and ci.yml's `shell` job runs it.
set -uo pipefail

: "${GH_REPO:?GH_REPO is required}"
: "${PR_NUMBER:?PR_NUMBER is required}"
: "${AUTHORIZED_SHA:?AUTHORIZED_SHA is required}"
: "${HEAD_REPO:?HEAD_REPO is required}"

# The queued run can trail the labelling event by a few seconds; a label added
# before the run appeared at all is a real state and gets a readable refusal
# rather than a silent pass.
tries="${CI_APPROVE_POLL_TRIES:-12}"
sleep_s="${CI_APPROVE_POLL_SLEEP:-5}"

die() {
  echo "::error::$*" >&2
  echo "FAIL $*" >&2
  exit 1
}

# `gh api --jq` prints one field per line; keep every expression trivial enough
# that the canned answer in scripts/check-ci-approve-run.sh means the same
# thing a live one would.
current="$(gh api "repos/$GH_REPO/pulls/$PR_NUMBER" \
  --jq '[.head.sha, (.head.repo.full_name // "")] | @tsv')" ||
  die "could not re-read #$PR_NUMBER's head before approving it"

IFS=$'\t' read -r current_sha current_repo <<<"$current"

if [ "$current_sha" != "$AUTHORIZED_SHA" ]; then
  die "#$PR_NUMBER moved from ${AUTHORIZED_SHA:0:12} to ${current_sha:0:12} before this" \
    "approval: nothing released. Read the new diff and re-add ci:run."
fi
if [ "$current_repo" != "$HEAD_REPO" ]; then
  die "#$PR_NUMBER's head repository is now '$current_repo', not the reviewed" \
    "'$HEAD_REPO': nothing released."
fi

approved=0
seen=0
attempt=1
while [ "$attempt" -le "$tries" ]; do
  # Every `pull_request` run for this exact commit. Filtered in the shell
  # rather than in the query so that the head repository and the status are
  # compared here, next to the refusals they justify.
  runs="$(gh api "repos/$GH_REPO/actions/runs?event=pull_request&head_sha=$AUTHORIZED_SHA&per_page=100" \
    --jq '.workflow_runs[] | [.id, .status, .name, (.head_repository.full_name // ""), .path] | @tsv')" ||
    die "could not list the queued runs for ${AUTHORIZED_SHA:0:12}"

  # EVERY queued run for that commit, which is also what the "Approve and
  # run" button in the UI releases -- including a workflow the pull request
  # itself added, since a fork's `pull_request` run uses the fork's copy of
  # .github/workflows. That is what reading the diff is for, and the path of
  # each released run is logged below so the record says what ran.
  while IFS=$'\t' read -r id status name head_repo path; do
    [ -n "${id:-}" ] || continue
    if [ "$head_repo" != "$HEAD_REPO" ]; then
      echo "skipping run $id ($name, ${path:-?}): head repository '$head_repo' is not '$HEAD_REPO'"
      continue
    fi
    seen=$((seen + 1))
    case "$status" in
      action_required | waiting)
        gh api -X POST "repos/$GH_REPO/actions/runs/$id/approve" >/dev/null ||
          die "could not approve run $id for ${AUTHORIZED_SHA:0:12} -- if this is a" \
            "403, set the CI_AUTHORIZE_TOKEN secret (see the workflow) or approve" \
            "the run from the Actions tab"
        echo "approved run $id ($name, ${path:-?}) for $HEAD_REPO@${AUTHORIZED_SHA:0:12}"
        approved=$((approved + 1))
        ;;
      *)
        echo "run $id ($name, ${path:-?}) is $status: nothing to release"
        ;;
    esac
  done <<<"$runs"

  [ "$seen" -gt 0 ] && break
  attempt=$((attempt + 1))
  [ "$attempt" -le "$tries" ] && sleep "$sleep_s"
done

if [ "$seen" -eq 0 ]; then
  die "no pull_request run for $HEAD_REPO@${AUTHORIZED_SHA:0:12} after" \
    "$((tries * sleep_s))s. Nothing was released. If the run appears later," \
    "remove and re-add ci:run."
fi

echo "released $approved of $seen run(s) for $HEAD_REPO@${AUTHORIZED_SHA:0:12}"
