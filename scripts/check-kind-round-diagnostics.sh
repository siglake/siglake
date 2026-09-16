#!/usr/bin/env bash
# Verify that scripts/kind-round.sh's failure diagnostics dump a pod's logs
# exactly when that pod's Job was not dumped above it -- and that the membership
# test deciding this does not depend on the exit status of a writer whose reader
# may have gone away.
#
# The failure this guards is task #1958's, the same one fixed one site over in
# scripts/check-kind-round-scale.sh (see its `contains` at line 50): under
# `set -o pipefail`, `printf '%s\n' "${jobs[@]}" | grep -qx -- "$pod_job"` can
# report a SUCCESSFUL match as a failed one, because `grep -q` exits at its
# first match and the printf then dies of SIGPIPE. Negated, that reads as "this
# pod's job is not in the list" and the round dumps a second copy of the pod's
# logs into an already-long failed-round log.
#
# The round script is not runnable here -- it builds a kind cluster -- so this
# sources the prefix of it that is constants and function definitions (up to,
# but not including, the `trap cleanup` line) into a sandbox whose `kubectl` is
# a stand-in reading fixture files. Nothing here reaches a cluster, a container
# runtime or the network.
#
# A guard whose failure mode is a green run is worse than no guard, so the last
# arm redefines the membership helper as the pipe form the fix removed and
# requires this to be caught. That mutation is also what pins the CALL SITE to
# the helper: were the pipe inlined there again, redefining the helper would
# change nothing, the mutation would go uncaught, and this guard would go red.
#
# The same stand-in-PATH sandbox also exercises kind-up.sh and kind-down.sh's
# cluster-existence checks. Their fixtures pin exact-name matches and misses,
# including a list larger than a pipe buffer with the match first: neither
# decision may depend on a producer surviving an early-exiting reader.

set -euo pipefail

cd "$(dirname "$0")/.."

ROUND_SCRIPT=scripts/kind-round.sh
# The line the sourced prefix stops before: everything after it runs the round.
TRAP_LINE='trap cleanup EXIT INT TERM'
# The namespace kind-round.sh dumps Jobs from, and the marker its per-pod log
# section prints. Both are read out of the script rather than restated, so a
# rename there cannot leave this guard asserting about a section nobody prints.
NAMESPACE=$(sed -n 's/^NAMESPACE=\([A-Za-z0-9-]*\)$/\1/p' "$ROUND_SCRIPT")

fail() {
  echo "FAIL $*" >&2
  exit 1
}

[ -n "$NAMESPACE" ] || fail "$ROUND_SCRIPT has no single \`NAMESPACE=\` assignment"
grep -qxF "$TRAP_LINE" "$ROUND_SCRIPT" ||
  fail "$ROUND_SCRIPT no longer has a \`$TRAP_LINE\` line -- this guard cuts the sourceable prefix there"
grep -qF 'dump_section "logs pod ' "$ROUND_SCRIPT" ||
  fail "$ROUND_SCRIPT no longer prints a \`logs pod <ns>/<name>\` section -- the assertions below name nothing"

sandbox=$(mktemp -d "${TMPDIR:-/tmp}/siglake-kind-round-diagnostics.XXXXXX")
trap 'rm -rf -- "$sandbox"' EXIT
mkdir -p "$sandbox/bin" "$sandbox/scripts" "$sandbox/tmp"

# The prefix of the round script that defines things without doing any: the
# `trap cleanup` line and everything below it is the round itself.
sed -n "1,/^${TRAP_LINE}\$/p" "$ROUND_SCRIPT" | sed '$d' >"$sandbox/scripts/prelude.bash"
grep -q '^dump_cluster_state()' "$sandbox/scripts/prelude.bash" ||
  fail "the sourceable prefix of $ROUND_SCRIPT does not define dump_cluster_state"
grep -q '^cleanup()' "$sandbox/scripts/prelude.bash" ||
  fail "the sourceable prefix of $ROUND_SCRIPT does not define cleanup"
# `source "$ROOT/scripts/kind-common.bash"` resolves against the prelude's own
# directory, so the sandbox carries a copy; kind-down.sh is a stand-in, so the
# cleanup arm can run the whole function without deleting anything.
cp scripts/kind-common.bash "$sandbox/scripts/kind-common.bash"
cat >"$sandbox/scripts/kind-down.sh" <<'EOF'
#!/usr/bin/env bash
echo "STUB kind-down ${KIND_CLUSTER_NAME:-}"
[ -z "${KIND_DOWN_CALLS:-}" ] || printf '%s\t%s\n' \
  "${KIND_CLUSTER_NAME:-}" "${KIND_EXPECTED_CONTROL_PLANE_ID:-}" >>"$KIND_DOWN_CALLS"
exit "${KIND_DOWN_RC:-0}"
EOF
chmod +x "$sandbox/scripts/kind-down.sh"

# The stand-in kubectl. Only the reads dump_cluster_state makes are answered;
# anything else prints a marker and succeeds, which is what the round's
# best-effort `|| true` commands would see from a real cluster.
cat >"$sandbox/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
args="$*"
case "$args" in
*"get --raw /readyz"*) exit 0 ;;
*"get pods -A -o json"*) cat "$FIXTURE_PODS"; exit 0 ;;
*"get jobs -o name"*) cat "$FIXTURE_JOBS"; exit 0 ;;
*"-o jsonpath="*) exit 0 ;;
esac
printf 'STUB kubectl %s\n' "$args"
EOF
chmod +x "$sandbox/bin/kubectl"

# Stand-ins for kind-up.sh and kind-down.sh. `kind get clusters` fully writes a
# fixture; mutating either script back to `kind ... | grep -q` makes the large
# fixture's cat lose SIGPIPE and the script choose the wrong arm under
# pipefail. Docker discovery and inspection also come from fixture files.
# Every Docker command is recorded for assertions below.
cat >"$sandbox/bin/kind" <<'EOF'
#!/usr/bin/env bash
if [ "$*" = "get clusters" ]; then
  cat "$FIXTURE_CLUSTERS"
else
  printf 'kind %s\n' "$*" >>"$CALLS"
fi
EOF
cat >"$sandbox/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf 'docker %s\n' "$*" >>"$CALLS"
case "${1:-}" in
ps)
  [ "${FIXTURE_DOCKER_PS_RC:-0}" = 0 ] || exit "$FIXTURE_DOCKER_PS_RC"
  cat "$FIXTURE_DOCKER_PS"
  ;;
inspect)
  [ "${FIXTURE_DOCKER_INSPECT_RC:-0}" = 0 ] || exit "$FIXTURE_DOCKER_INSPECT_RC"
  cat "$FIXTURE_DOCKER_INSPECT"
  ;;
esac
EOF
cat >"$sandbox/bin/helm" <<'EOF'
#!/usr/bin/env bash
printf 'helm %s\n' "$*" >>"$CALLS"
EOF
chmod +x "$sandbox/bin/kind" "$sandbox/bin/docker" "$sandbox/bin/helm"

# Two not-Ready pods per fixture: `job` is the value of the pod's job-name
# label, empty for a pod that belongs to no Job.
write_pods() {
  local out=$1 name job
  {
    printf '{"items":['
    local first=1
    shift
    for name in "$@"; do
      job=${name#*=}
      name=${name%%=*}
      [ "$first" = 1 ] || printf ','
      first=0
      printf '{"metadata":{"namespace":"%s","name":"%s","labels":{"job-name":"%s"}},' \
        "$NAMESPACE" "$name" "$job"
      printf '"status":{"phase":"Running","conditions":[{"type":"Ready","status":"False"}]}}'
    done
    printf ']}\n'
  } >"$out"
}

# Run dump_cluster_state(1) against a fixture. $1 = jobs file, $2 = pods file,
# $3 = extra bash evaluated after sourcing the prelude (the mutation), stdout =
# the diagnostics. Every arm runs in its own subshell: the prelude is a prefix
# of a script written to be sourced once.
run_diagnostics() {
  local jobs_file=$1 pods_file=$2 mutation=${3:-}
  (
    PATH="$sandbox/bin:$PATH"
    TMPDIR="$sandbox/tmp"
    export PATH TMPDIR
    FIXTURE_JOBS=$jobs_file FIXTURE_PODS=$pods_file
    export FIXTURE_JOBS FIXTURE_PODS
    # shellcheck disable=SC1091
    source "$sandbox/scripts/prelude.bash"
    [ -z "$mutation" ] || eval "$mutation"
    dump_cluster_state 1
  ) 2>/dev/null
}

# Does the diagnostics output carry a `logs pod <ns>/<name>` section? The `case`
# is the point of the whole exercise: no pipe, no reader to go away.
dumped_logs() {
  case "$1" in
  *"--- logs pod ${NAMESPACE}/$2"$'\n'*) return 0 ;;
  *) return 1 ;;
  esac
}

# $1 = case name, $2 = output, $3.. = `pod:yes|no` expectations.
expect_logs() {
  local name=$1 out=$2 spec pod want
  shift 2
  for spec in "$@"; do
    pod=${spec%%:*}
    want=${spec#*:}
    case "$out" in
    *"--- describe pod ${NAMESPACE}/${pod}"$'\n'*) ;;
    *) fail "$name: pod $pod was not described at all -- the fixture never reached the per-pod loop" ;;
    esac
    if dumped_logs "$out" "$pod"; then
      [ "$want" = yes ] ||
        fail "$name: pod $pod's logs were dumped, but its Job was dumped above -- a second copy of the same logs"
    else
      [ "$want" = no ] ||
        fail "$name: pod $pod's logs were NOT dumped, and nothing above them carries them"
    fi
  done
}

cases=0

# --- kind-up/down cluster existence -----------------------------------------
run_kind_script() {
  local script=$1 fixture=$2 calls=$3
  local docker_ps=${4:-$sandbox/docker-none}
  local docker_inspect=${5:-$sandbox/docker-inspect-unused}
  local inspect_rc=${6:-0} ownership_file=${7:-} expected_id=${8:-}
  : >"$calls"
  (
    PATH="$sandbox/bin:$PATH"
    FIXTURE_CLUSTERS="$fixture" CALLS="$calls" KIND_CLUSTER_NAME=siglake-test
    FIXTURE_DOCKER_PS="$docker_ps" FIXTURE_DOCKER_INSPECT="$docker_inspect"
    FIXTURE_DOCKER_INSPECT_RC="$inspect_rc"
    export PATH FIXTURE_CLUSTERS CALLS KIND_CLUSTER_NAME FIXTURE_DOCKER_PS
    export FIXTURE_DOCKER_INSPECT FIXTURE_DOCKER_INSPECT_RC
    if [[ -n "$ownership_file" ]]; then
      KIND_CLUSTER_OWNERSHIP_FILE=$ownership_file
      export KIND_CLUSTER_OWNERSHIP_FILE
    else
      unset KIND_CLUSTER_OWNERSHIP_FILE
    fi
    if [[ -n "$expected_id" ]]; then
      KIND_EXPECTED_CONTROL_PLANE_ID=$expected_id
      export KIND_EXPECTED_CONTROL_PLANE_ID
    else
      unset KIND_EXPECTED_CONTROL_PLANE_ID
    fi
    "$script"
  )
}

recorded_call() {
  local calls=$1 expected=$2 call
  while IFS= read -r call; do
    [[ "$call" == "$expected" ]] && return 0
  done <"$calls"
  return 1
}

assert_cluster_arms() {
  local name=$1 fixture=$2 calls
  calls="$sandbox/${name}-calls"

  run_kind_script scripts/kind-up.sh "$fixture" "$calls" >/dev/null 2>&1
  if recorded_call "$calls" "kind create cluster --name siglake-test --config $PWD/deploy/kind/cluster.yaml"; then
    [[ "$name" == missing ]] || fail "$name: kind-up.sh created an existing cluster"
  else
    [[ "$name" != missing ]] || fail "$name: kind-up.sh skipped creation of a missing cluster"
  fi

  run_kind_script scripts/kind-down.sh "$fixture" "$calls" >/dev/null 2>&1
  if recorded_call "$calls" "kind delete cluster --name siglake-test"; then
    [[ "$name" != missing ]] || fail "$name: kind-down.sh deleted a missing cluster"
  else
    [[ "$name" == missing ]] || fail "$name: kind-down.sh skipped deletion of an existing cluster"
  fi
}

printf 'other\nsiglake-test\nanother\n' >"$sandbox/clusters-exists"
printf 'other\nsiglake-testing\nanother\n' >"$sandbox/clusters-missing"
: >"$sandbox/docker-none"
: >"$sandbox/docker-inspect-unused"
assert_cluster_arms exists "$sandbox/clusters-exists"
cases=$((cases + 2))
assert_cluster_arms missing "$sandbox/clusters-missing"
cases=$((cases + 2))

{
  printf 'siglake-test\n'
  filler=$(printf 'x%.0s' $(seq 1 1024))
  for i in $(seq 1 100); do printf '%s-%s\n' "$filler" "$i"; done
} >"$sandbox/clusters-big"
[[ $(wc -c <"$sandbox/clusters-big") -gt 65536 ]] ||
  fail "the cluster SIGPIPE fixture is under a pipe buffer"
assert_cluster_arms big-list "$sandbox/clusters-big"
cases=$((cases + 2))

# An exact-name container is removable only after Docker identifies it as this
# cluster's exited kind control plane. Removal is by inspected ID and is never
# forced; kind-up may then create the cluster, while kind-down finishes the
# interrupted teardown without asking kind to delete an unregistered cluster.
container_id=bef96318d13c6e77693729fd34ee4e00b4e49d839c59bb801fc6cbdef2f0533b
printf '%s\n' "$container_id" >"$sandbox/docker-owned-exited"
cat >"$sandbox/docker-inspect-owned-exited" <<'EOF'
state=exited
cluster=siglake-test
role=control-plane
EOF
calls="$sandbox/owned-exited-up-calls"
marker="$sandbox/owned-exited-marker"
run_kind_script scripts/kind-up.sh "$sandbox/clusters-missing" "$calls" \
  "$sandbox/docker-owned-exited" "$sandbox/docker-inspect-owned-exited" 0 "$marker" \
  >/dev/null 2>&1
recorded_call "$calls" "docker ps -a --no-trunc --filter name=^/siglake-test-control-plane$ --format {{.ID}}" ||
  fail "owned exited: kind-up.sh did not use exact-name, non-truncated Docker discovery"
recorded_call "$calls" "docker rm $container_id" ||
  fail "owned exited: kind-up.sh did not remove the inspected container ID"
recorded_call "$calls" "kind create cluster --name siglake-test --config $PWD/deploy/kind/cluster.yaml" ||
  fail "owned exited: kind-up.sh did not create the cluster after recovery"
[[ $(<"$marker") == "$container_id" ]] ||
  fail "owned exited: kind-up.sh did not record the created control-plane container ID"
grep -q '^docker rm -' "$calls" &&
  fail "owned exited: cleanup passed an option to docker rm instead of relying on non-forced removal"
cases=$((cases + 1))

calls="$sandbox/owned-exited-down-calls"
run_kind_script scripts/kind-down.sh "$sandbox/clusters-missing" "$calls" \
  "$sandbox/docker-owned-exited" "$sandbox/docker-inspect-owned-exited" >/dev/null 2>&1
recorded_call "$calls" "docker rm $container_id" ||
  fail "owned exited: kind-down.sh did not remove the inspected container ID"
recorded_call "$calls" "kind delete cluster --name siglake-test" &&
  fail "owned exited: kind-down.sh asked kind to delete an unregistered cluster"
cases=$((cases + 1))

expect_container_refusal() {
  local name=$1 inspect_file=$2 inspect_rc=${3:-0} script calls out rc fragment
  shift 3 || true
  for script in scripts/kind-up.sh scripts/kind-down.sh; do
    calls="$sandbox/${name}-$(basename "$script")-calls"
    rc=0
    out=$(run_kind_script "$script" "$sandbox/clusters-missing" "$calls" \
      "$sandbox/docker-owned-exited" "$inspect_file" "$inspect_rc" 2>&1) || rc=$?
    [[ "$rc" -ne 0 ]] || fail "$name: $script accepted an ambiguous container"
    recorded_call "$calls" "docker rm $container_id" &&
      fail "$name: $script removed a container whose ownership or state was not safe"
    recorded_call "$calls" "kind create cluster --name siglake-test --config $PWD/deploy/kind/cluster.yaml" &&
      fail "$name: $script created a cluster without resolving its container-name collision"
    for fragment in "$@"; do
      case "$out" in
      *"$fragment"*) ;;
      *) fail "$name: $script diagnostic omitted: $fragment" ;;
      esac
    done
    cases=$((cases + 1))
  done
}

cat >"$sandbox/docker-inspect-active" <<'EOF'
state=running
cluster=siglake-test
role=control-plane
EOF
expect_container_refusal active "$sandbox/docker-inspect-active" 0 \
  "$container_id" 'io.x-k8s.kind.cluster=siglake-test' 'state=running'

cat >"$sandbox/docker-inspect-foreign" <<'EOF'
state=exited
cluster=somebody-else
role=control-plane
EOF
expect_container_refusal foreign "$sandbox/docker-inspect-foreign" 0 \
  "$container_id" 'io.x-k8s.kind.cluster=somebody-else' 'state=exited'

cat >"$sandbox/docker-inspect-unlabeled" <<'EOF'
state=exited
cluster=
role=
EOF
expect_container_refusal unlabeled "$sandbox/docker-inspect-unlabeled" 0 \
  "$container_id" 'io.x-k8s.kind.cluster=<missing>' 'state=exited'

expect_container_refusal inspect-failure "$sandbox/docker-inspect-unused" 17 \
  "could not inspect container $container_id" 'refusing cleanup'

# A round supplies an ownership marker path. Seeing a registered cluster before
# creation must fail without writing that marker, so the round cannot reuse and
# later tear down another invocation's cluster.
calls="$sandbox/existing-round-calls"
marker="$sandbox/existing-round-marker"
rc=0
out=$(run_kind_script scripts/kind-up.sh "$sandbox/clusters-exists" "$calls" \
  "$sandbox/docker-none" "$sandbox/docker-inspect-unused" 0 "$marker" 2>&1) || rc=$?
[[ "$rc" -ne 0 ]] || fail "existing round: kind-up.sh reused a cluster this round did not create"
[[ ! -e "$marker" ]] || fail "existing round: kind-up.sh claimed ownership of an existing cluster"
case "$out" in
*'already exists and was not created by this round'*) ;;
*) fail "existing round: refusal did not explain why the cluster cannot be reused" ;;
esac
cases=$((cases + 1))

# The round's teardown carries the container ID observed after creation. A
# replacement cluster with the same kind name has a different ID and must not
# be deleted by the older round's EXIT trap.
calls="$sandbox/expected-id-match-calls"
run_kind_script scripts/kind-down.sh "$sandbox/clusters-exists" "$calls" \
  "$sandbox/docker-owned-exited" "$sandbox/docker-inspect-unused" 0 '' "$container_id" \
  >/dev/null 2>&1
recorded_call "$calls" "kind delete cluster --name siglake-test" ||
  fail "expected ID: kind-down.sh did not delete the cluster created by this round"
cases=$((cases + 1))

calls="$sandbox/expected-id-mismatch-calls"
rc=0
out=$(run_kind_script scripts/kind-down.sh "$sandbox/clusters-exists" "$calls" \
  "$sandbox/docker-owned-exited" "$sandbox/docker-inspect-unused" 0 '' deadbeef 2>&1) || rc=$?
[[ "$rc" -ne 0 ]] || fail "expected ID: kind-down.sh deleted a replacement cluster"
recorded_call "$calls" "kind delete cluster --name siglake-test" &&
  fail "expected ID: kind-down.sh issued delete despite a replacement container ID"
case "$out" in
*"now uses control-plane container $container_id"*'this round created deadbeef'*) ;;
*) fail "expected ID: refusal omitted the observed and owned container IDs" ;;
esac
cases=$((cases + 1))

# --- match, miss and no-Job, with a Job list of ordinary size -----------------
# `migrate-schema` is dumped as a Job, so its pod's logs are already in the
# output; `orphan` is not a Job this round dumped, and `nojob-pod` has no
# job-name label at all. Both of the latter need their own logs.
printf 'job.batch/migrate-schema\njob.batch/other-job\n' >"$sandbox/jobs-small"
write_pods "$sandbox/pods-mixed" \
  "migrate-schema-abcde=migrate-schema" "orphan-pod=orphan" "nojob-pod="
out=$(run_diagnostics "$sandbox/jobs-small" "$sandbox/pods-mixed")
expect_logs "mixed" "$out" \
  "migrate-schema-abcde:no" "orphan-pod:yes" "nojob-pod:yes"
cases=$((cases + 1))

# A prefix of a dumped Job's name is not that Job: `migrate` must not match
# `migrate-schema`, which a substring test would let through.
write_pods "$sandbox/pods-prefix" "prefix-pod=migrate" "suffix-pod=schema"
out=$(run_diagnostics "$sandbox/jobs-small" "$sandbox/pods-prefix")
expect_logs "prefix" "$out" "prefix-pod:yes" "suffix-pod:yes"
cases=$((cases + 1))

# --- no Jobs at all ----------------------------------------------------------
# The round can fail before any Job exists, which leaves the array empty. Under
# `set -u` that is a shape a membership test can die on; every pod's logs are
# needed, because nothing above carries them.
: >"$sandbox/jobs-empty"
out=$(run_diagnostics "$sandbox/jobs-empty" "$sandbox/pods-mixed")
expect_logs "no-jobs" "$out" \
  "migrate-schema-abcde:yes" "orphan-pod:yes" "nojob-pod:yes"
cases=$((cases + 1))

# --- the SIGPIPE hazard, forced -----------------------------------------------
# The bug is a race, and at three job names it is nearly unlosable -- which is
# why it was left alone when it was found. Made deterministic: the match is the
# first line and the rest of the list is larger than a 64 KiB pipe buffer, so a
# reader that exits at the first match leaves the writer with a full buffer and
# nowhere to put the rest. The fixed code has no reader to lose.
{
  printf 'job.batch/migrate-schema\n'
  filler=$(printf 'x%.0s' $(seq 1 1024))
  for i in $(seq 1 100); do printf 'job.batch/%s-%s\n' "$filler" "$i"; done
} >"$sandbox/jobs-big"
[ "$(wc -c <"$sandbox/jobs-big")" -gt 65536 ] ||
  fail "the SIGPIPE fixture's job list is under a pipe buffer -- it would not force the race"
write_pods "$sandbox/pods-one" "migrate-schema-abcde=migrate-schema"

out=$(run_diagnostics "$sandbox/jobs-big" "$sandbox/pods-one")
expect_logs "big-list" "$out" "migrate-schema-abcde:no"
cases=$((cases + 1))

# The mutation: the membership helper put back the way it was. It must be caught
# on the same fixture the real code just passed.
MUTATION='array_contains() { local n=$1; shift; printf "%s\n" "$@" | grep -qx -- "$n"; }'
out=$(run_diagnostics "$sandbox/jobs-big" "$sandbox/pods-one" "$MUTATION")
if ! dumped_logs "$out" "migrate-schema-abcde"; then
  fail "fixture: the pipe-form membership test was not caught. Either it no longer loses the SIGPIPE race on a $(wc -c <"$sandbox/jobs-big")-byte list, or the call site no longer goes through array_contains and redefining it changes nothing"
fi
cases=$((cases + 1))

# --- the status cleanup exits with -------------------------------------------
# The dump is best-effort from end to end: kind round #2 lost a failed hook's
# stderr to the teardown, and the fix for that must never become a dump that
# changes what the round reports. Both a failing and a succeeding round, with
# the diagnostics half reached only by the first.
for status in 0 1 7; do
  rc=0
  run_diagnostics "$sandbox/jobs-empty" "$sandbox/pods-mixed" \
    "dump_cluster_state() { :; }; dump_cluster_state $status" >/dev/null || rc=$?
  [ "$rc" = 0 ] || fail "sourcing the prelude for status=$status exited $rc"
done
for status in 0 3; do
  rc=0
  (
    PATH="$sandbox/bin:$PATH"
    TMPDIR="$sandbox/tmp"
    export PATH TMPDIR
    FIXTURE_JOBS=$sandbox/jobs-small FIXTURE_PODS=$sandbox/pods-mixed
    export FIXTURE_JOBS FIXTURE_PODS
    # shellcheck disable=SC1091
    source "$sandbox/scripts/prelude.bash"
    trap - EXIT INT TERM
    ROOT=$sandbox
    (exit "$status")
    cleanup
  ) >/dev/null 2>&1 || rc=$?
  [ "$rc" = "$status" ] ||
    fail "cleanup turned a round that exited $status into $rc -- the pre-teardown dump is not allowed to change the status"
  cases=$((cases + 1))
done

run_cleanup_ownership() {
  local marker_value=$1 calls=$2 status=${3:-3} rc=0
  : >"$calls"
  (
    PATH="$sandbox/bin:$PATH"
    TMPDIR="$sandbox/tmp"
    FIXTURE_JOBS=$sandbox/jobs-small FIXTURE_PODS=$sandbox/pods-mixed
    KIND_DOWN_CALLS=$calls KIND_CLUSTER_NAME=siglake-test
    export PATH TMPDIR FIXTURE_JOBS FIXTURE_PODS KIND_DOWN_CALLS KIND_CLUSTER_NAME
    # shellcheck disable=SC1091
    source "$sandbox/scripts/prelude.bash"
    trap - EXIT INT TERM
    ROOT=$sandbox
    dump_cluster_state() { :; }
    if [[ -n "$marker_value" ]]; then
      printf '%s\n' "$marker_value" >"$KIND_CLUSTER_OWNERSHIP_FILE"
    fi
    (exit "$status")
    cleanup
  ) >/dev/null 2>&1 || rc=$?
  printf '%s\n' "$rc"
}

calls="$sandbox/cleanup-unowned-calls"
rc=$(run_cleanup_ownership '' "$calls")
[[ "$rc" == 3 ]] || fail "unowned cleanup: changed failed-round status 3 to $rc"
[[ ! -s "$calls" ]] || fail "unowned cleanup: called kind-down for a cluster this round did not create"
cases=$((cases + 1))

calls="$sandbox/cleanup-owned-calls"
rc=$(run_cleanup_ownership "$container_id" "$calls")
[[ "$rc" == 3 ]] || fail "owned cleanup: changed failed-round status 3 to $rc"
recorded_call "$calls" $'siglake-test\t'"$container_id" ||
  fail "owned cleanup: did not call kind-down for the cluster this round created"
cases=$((cases + 1))

calls="$sandbox/cleanup-invalid-calls"
rc=$(run_cleanup_ownership '' "$calls" 0)
[[ "$rc" == 0 ]] || fail "unowned successful cleanup: changed status 0 to $rc"
[[ ! -s "$calls" ]] || fail "unowned successful cleanup: called kind-down without an ownership marker"
cases=$((cases + 1))

echo "ok ($ROUND_SCRIPT: per-pod log dumps follow exact Job membership, cleanup preserves its status; $cases cases)"
