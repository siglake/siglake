#!/usr/bin/env bash
# Hermetic regression coverage for deploy/aws/down.sh's destroy exit status.
#
# down.sh ran without errexit and ended with `log "down complete"`, so its
# status was that log call: a failed `terraform destroy` left RDS, the
# warehouse bucket and the IAM role billing while the caller read the teardown
# as finished (#5309). The Helm and kubectl steps before it are best-effort on
# purpose, so the cleanup-failure case below is here to keep them that way.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/siglake-aws-down-destroy.XXXXXX")"
trap 'rm -rf "$TEST_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

write_stubs() {
  local stub_dir=$1
  mkdir -p "$stub_dir"

  cat > "$stub_dir/terraform" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
printf 'terraform' >> "$STUB_LOG"
printf ' <%s>' "$@" >> "$STUB_LOG"
printf '\n' >> "$STUB_LOG"

case "${1:-}" in
  output)
    case "${3:-}" in
      warehouse_bucket) printf 'stub-bucket\n' ;;
      *) printf 'unexpected terraform output: %s\n' "${3:-}" >&2; exit 2 ;;
    esac
    ;;
  destroy)
    exit "$STUB_DESTROY_RC"
    ;;
  *)
    printf 'unexpected terraform command: %s\n' "$*" >&2
    exit 2
    ;;
esac
STUB

  cat > "$stub_dir/aws" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
printf 'aws' >> "$STUB_LOG"
printf ' <%s>' "$@" >> "$STUB_LOG"
printf '\n' >> "$STUB_LOG"

case "${1:-} ${2:-}" in
  "s3 rm")
    exit "$STUB_CLEANUP_RC"
    ;;
  "s3api list-object-versions")
    [[ "$STUB_CLEANUP_RC" -eq 0 ]] || exit "$STUB_CLEANUP_RC"
    printf '{"Versions":[{"Key":"warehouse/obj","VersionId":"v1"}]}\n'
    ;;
  "s3api delete-objects")
    exit "$STUB_CLEANUP_RC"
    ;;
  *)
    printf 'unexpected aws command: %s\n' "$*" >&2
    exit 2
    ;;
esac
STUB

  cat > "$stub_dir/kubectl" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
printf 'kubectl' >> "$STUB_LOG"
printf ' <%s>' "$@" >> "$STUB_LOG"
printf '\n' >> "$STUB_LOG"

# The namespace exists, so the best-effort cleanup steps all run.
[[ " $* " != *" get ns "* ]] || exit 0
exit "$STUB_CLEANUP_RC"
STUB

  cat > "$stub_dir/helm" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
printf 'helm' >> "$STUB_LOG"
printf ' <%s>' "$@" >> "$STUB_LOG"
printf '\n' >> "$STUB_LOG"
exit "$STUB_CLEANUP_RC"
STUB

  chmod +x "$stub_dir/terraform" "$stub_dir/aws" "$stub_dir/kubectl" "$stub_dir/helm"
}

# run_case <name> <SIGLAKE_DOWN_MODE> <destroy rc> <cleanup rc>; echoes the case dir.
run_case() {
  local name=$1 mode=$2 destroy_rc=$3 cleanup_rc=$4
  local case_dir="$TEST_ROOT/$name"
  local stub_dir="$case_dir/bin"
  local rc=0

  mkdir -p "$case_dir/tf"
  : > "$case_dir/commands.log"
  write_stubs "$stub_dir"

  STUB_LOG="$case_dir/commands.log" \
  STUB_DESTROY_RC="$destroy_rc" \
  STUB_CLEANUP_RC="$cleanup_rc" \
  SIGLAKE_DOWN_MODE="$mode" \
  TF_DIR="$case_dir/tf" \
  PATH="$stub_dir:$PATH" \
    "$ROOT/deploy/aws/down.sh" > "$case_dir/output" 2>&1 || rc=$?

  printf '%s\n' "$rc" > "$case_dir/rc"
  printf '%s\n' "$case_dir"
}

case_rc() { cat "$1/rc"; }

assert_reported_complete() {
  local name=$1 case_dir=$2
  grep -q 'down complete' "$case_dir/output" \
    || fail "$name: a successful destroy did not report 'down complete'"
}

assert_silent_on_failure() {
  local name=$1 case_dir=$2
  if grep -q 'down complete' "$case_dir/output"; then
    fail "$name: a failed destroy still reported 'down complete'"
  fi
}

assert_destroyed() {
  local name=$1 case_dir=$2
  grep -q '^terraform <destroy>' "$case_dir/commands.log" \
    || fail "$name: terraform destroy never ran"
}

# --- cluster mode ------------------------------------------------------------
# The default mode is targeted, and the targets are what keeps EKS/VPC/EFS/ECR
# warm; assert two of them so a destroy that silently widened or narrowed its
# selection is caught here and not on a bill.
cluster_ok=$(run_case cluster-destroy-succeeds cluster 0 0)
[[ "$(case_rc "$cluster_ok")" -eq 0 ]] \
  || fail "cluster-destroy-succeeds: down.sh exited $(case_rc "$cluster_ok")"
assert_destroyed cluster-destroy-succeeds "$cluster_ok"
assert_reported_complete cluster-destroy-succeeds "$cluster_ok"
grep -q '<-target=aws_db_instance.rds>' "$cluster_ok/commands.log" \
  || fail "cluster-destroy-succeeds: the RDS target left the targeted destroy"
grep -q '<-target=aws_s3_bucket.warehouse>' "$cluster_ok/commands.log" \
  || fail "cluster-destroy-succeeds: the warehouse target left the targeted destroy"

cluster_fail=$(run_case cluster-destroy-fails cluster 7 0)
[[ "$(case_rc "$cluster_fail")" -eq 7 ]] \
  || fail "cluster-destroy-fails: down.sh exited $(case_rc "$cluster_fail"), want terraform's 7"
assert_silent_on_failure cluster-destroy-fails "$cluster_fail"
grep -q 'terraform destroy failed' "$cluster_fail/output" \
  || fail "cluster-destroy-fails: the failure was not reported"

# --- all mode ----------------------------------------------------------------
all_ok=$(run_case all-destroy-succeeds all 0 0)
[[ "$(case_rc "$all_ok")" -eq 0 ]] \
  || fail "all-destroy-succeeds: down.sh exited $(case_rc "$all_ok")"
assert_destroyed all-destroy-succeeds "$all_ok"
assert_reported_complete all-destroy-succeeds "$all_ok"
if grep -q '<-target=' "$all_ok/commands.log"; then
  fail "all-destroy-succeeds: the full destroy was targeted"
fi

all_fail=$(run_case all-destroy-fails all 3 0)
[[ "$(case_rc "$all_fail")" -eq 3 ]] \
  || fail "all-destroy-fails: down.sh exited $(case_rc "$all_fail"), want terraform's 3"
assert_silent_on_failure all-destroy-fails "$all_fail"

# --- cleanup stays best-effort -----------------------------------------------
# Helm, kubectl and the warehouse sweep all fail here. Every one of them is
# expected to keep going, and the run's status is still the destroy's.
cleanup_fail=$(run_case cluster-cleanup-fails cluster 0 9)
[[ "$(case_rc "$cleanup_fail")" -eq 0 ]] \
  || fail "cluster-cleanup-fails: failing cleanup changed the exit status to $(case_rc "$cleanup_fail")"
assert_destroyed cluster-cleanup-fails "$cleanup_fail"
assert_reported_complete cluster-cleanup-fails "$cleanup_fail"
for expected in '^helm <uninstall>' '^kubectl .*<secret>' '^kubectl .*<pvc>' \
                '^kubectl <delete> <namespace>' '^aws <s3> <rm>'; do
  grep -qE "$expected" "$cleanup_fail/commands.log" \
    || fail "cluster-cleanup-fails: a best-effort step matching $expected did not run"
done

cleanup_then_destroy_fail=$(run_case cluster-cleanup-and-destroy-fail cluster 5 9)
[[ "$(case_rc "$cleanup_then_destroy_fail")" -eq 5 ]] \
  || fail "cluster-cleanup-and-destroy-fail: exited $(case_rc "$cleanup_then_destroy_fail"), want 5"
assert_silent_on_failure cluster-cleanup-and-destroy-fail "$cleanup_then_destroy_fail"

# --- an unknown mode destroys nothing ----------------------------------------
bad_mode=$(run_case unknown-mode sideways 0 0)
[[ "$(case_rc "$bad_mode")" -ne 0 ]] \
  || fail "unknown-mode: down.sh accepted SIGLAKE_DOWN_MODE=sideways"
if grep -q '^terraform <destroy>' "$bad_mode/commands.log"; then
  fail "unknown-mode: terraform destroy ran for an unknown mode"
fi
assert_silent_on_failure unknown-mode "$bad_mode"

printf 'ok: aws down exits nonzero on a failed destroy and reports completion only on success\n'
