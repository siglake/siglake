#!/usr/bin/env bash
# Hermetic regression coverage for deploy/aws/up.sh's kubeconfig isolation.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/siglake-aws-up-kubeconfig.XXXXXX")"
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
set -euo pipefail
printf 'terraform' >> "$STUB_LOG"
printf ' <%s>' "$@" >> "$STUB_LOG"
printf '\n' >> "$STUB_LOG"

case "${1:-}" in
  version)
    printf '{"terraform_version":"1.6.0"}\n'
    ;;
  init|apply)
    touch "$STUB_STATE/applied"
    ;;
  output)
    [[ -f "$STUB_STATE/applied" ]] || exit 0
    case "${3:-}" in
      region) printf 'us-east-1\n' ;;
      cluster_name) printf 'stub-cluster\n' ;;
      rds_secret_arn) printf 'arn:aws:secretsmanager:us-east-1:123:secret:stub\n' ;;
      ecr_repository_url) printf '123.dkr.ecr.us-east-1.amazonaws.com/siglake\n' ;;
      helm_values) printf 'query:\n  replicas: 1\n' ;;
      efs_file_system_id) printf 'fs-stub\n' ;;
      warehouse_bucket) printf 'stub-bucket\n' ;;
      rds_endpoint) printf 'stub-rds.example.test\n' ;;
      *) printf 'unexpected terraform output: %s\n' "${3:-}" >&2; exit 2 ;;
    esac
    ;;
  *)
    printf 'unexpected terraform command: %s\n' "$*" >&2
    exit 2
    ;;
esac
STUB

  cat > "$stub_dir/aws" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf 'aws' >> "$STUB_LOG"
printf ' <%s>' "$@" >> "$STUB_LOG"
printf '\n' >> "$STUB_LOG"

case "${1:-} ${2:-}" in
  "sts get-caller-identity")
    ;;
  "eks update-kubeconfig")
    [[ "$STUB_SCENARIO" != update-failure ]] || exit 42
    kubeconfig=""
    alias=""
    while (($#)); do
      case "$1" in
        --kubeconfig) kubeconfig=$2; shift 2 ;;
        --alias) alias=$2; shift 2 ;;
        *) shift ;;
      esac
    done
    [[ -n "$kubeconfig" && -n "$alias" ]] || exit 43
    printf '%s\n' "$kubeconfig" > "$STUB_STATE/kubeconfig-path"
    printf '%s\n' "$alias" > "$STUB_STATE/context"
    printf 'private kubeconfig for %s\n' "$alias" > "$kubeconfig"
    chmod 600 "$kubeconfig"
    if [[ "$STUB_SCENARIO" == ambient-change ]]; then
      printf 'other-cluster\n' > "$STUB_AMBIENT_FIRST"
    fi
    ;;
  "secretsmanager get-secret-value")
    printf '{"username":"stub","password":"stub"}\n'
    ;;
  *)
    printf 'unexpected aws command: %s\n' "$*" >&2
    exit 2
    ;;
esac
STUB

  cat > "$stub_dir/kubectl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl' >> "$STUB_LOG"
printf ' <%s>' "$@" >> "$STUB_LOG"
printf '\n' >> "$STUB_LOG"

kubeconfig=""
context=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[$i]}" in
    --kubeconfig) kubeconfig=${args[$((i + 1))]} ;;
    --context) context=${args[$((i + 1))]} ;;
  esac
done

expected_kubeconfig=$(<"$STUB_STATE/kubeconfig-path")
expected_context=$(<"$STUB_STATE/context")
[[ "$kubeconfig" == "$expected_kubeconfig" ]] || exit 51

if [[ " $* " == *" config current-context "* ]]; then
  if [[ "$STUB_SCENARIO" == wrong-current ]]; then
    printf 'other-cluster\n'
  else
    printf '%s\n' "$expected_context"
  fi
  exit 0
fi

[[ "$context" == "$expected_context" ]] || exit 52
if [[ " $* " == *" get ns "* ]]; then
  exit 1
fi
if [[ " $* " == *" apply -f - "* ]]; then
  consume=$(cat)
  [[ -n "$consume" ]] || exit 53
fi
STUB

  cat > "$stub_dir/helm" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf 'helm' >> "$STUB_LOG"
printf ' <%s>' "$@" >> "$STUB_LOG"
printf '\n' >> "$STUB_LOG"

if [[ "${1:-}" == version ]]; then
  printf 'v3.12.0\n'
  exit 0
fi

kubeconfig=""
context=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[$i]}" in
    --kubeconfig) kubeconfig=${args[$((i + 1))]} ;;
    --kube-context) context=${args[$((i + 1))]} ;;
  esac
done
[[ "$kubeconfig" == "$(<"$STUB_STATE/kubeconfig-path")" ]] || exit 61
[[ "$context" == "$(<"$STUB_STATE/context")" ]] || exit 62
[[ " $* " == *" upgrade --install "* ]] || exit 63
STUB

  chmod +x "$stub_dir/terraform" "$stub_dir/aws" "$stub_dir/kubectl" "$stub_dir/helm"
}

run_case() {
  local scenario=$1
  local expect_success=$2
  local case_dir="$TEST_ROOT/$scenario"
  local stub_dir="$case_dir/bin"
  local output="$case_dir/output"
  local ambient_first="$case_dir/ambient-first"
  local ambient_second="$case_dir/ambient-second"
  local rc=0

  mkdir -p "$case_dir/state" "$case_dir/tmp" "$case_dir/tf"
  printf 'caller-first\n' > "$ambient_first"
  printf 'caller-second\n' > "$ambient_second"
  : > "$case_dir/commands.log"
  write_stubs "$stub_dir"

  STUB_STATE="$case_dir/state" \
  STUB_LOG="$case_dir/commands.log" \
  STUB_SCENARIO="$scenario" \
  STUB_AMBIENT_FIRST="$ambient_first" \
  TMPDIR="$case_dir/tmp" \
  KUBECONFIG="$ambient_first:$ambient_second" \
  TF_DIR="$case_dir/tf" \
  PATH="$stub_dir:$PATH" \
    "$ROOT/deploy/aws/up.sh" > "$output" 2>&1 || rc=$?

  if [[ "$expect_success" == yes ]]; then
    [[ "$rc" -eq 0 ]] || fail "$scenario: up.sh exited $rc: $(tail -1 "$output")"
    retained=$(sed -n 's/^  export KUBECONFIG=//p' "$output")
    [[ -n "$retained" && -f "$retained" ]] \
      || fail "$scenario: successful handoff did not retain its private kubeconfig"
    [[ "$(stat -c '%a' "$retained")" == 600 ]] \
      || fail "$scenario: private kubeconfig mode is not 600"
    [[ "$retained" != "$ambient_first" && "$retained" != "$ambient_second" ]] \
      || fail "$scenario: handoff reused the caller's kubeconfig"
    rm -f "$retained"
  else
    [[ "$rc" -ne 0 ]] || fail "$scenario: up.sh unexpectedly succeeded"
    if find "$case_dir/tmp" -maxdepth 1 -name 'siglake-kubeconfig.*' -print -quit | grep -q .; then
      fail "$scenario: failed run leaked its private kubeconfig"
    fi
  fi

  printf '%s\n' "$case_dir"
}

ambient_case=$(run_case ambient-change yes)
[[ "$(<"$ambient_case/ambient-first")" == other-cluster ]] \
  || fail "ambient-change: the fixture did not change the caller's context"
[[ "$(<"$ambient_case/ambient-second")" == caller-second ]] \
  || fail "ambient-change: the caller's second kubeconfig was modified"

list_case=$(run_case kubeconfig-list yes)
[[ "$(<"$list_case/ambient-first")" == caller-first ]] \
  || fail "kubeconfig-list: the caller's first kubeconfig was modified"
[[ "$(<"$list_case/ambient-second")" == caller-second ]] \
  || fail "kubeconfig-list: the caller's second kubeconfig was modified"

failure_case=$(run_case update-failure no)
if grep -q '^kubectl' "$failure_case/commands.log"; then
  fail "update-failure: kubectl ran after update-kubeconfig failed"
fi
if grep -q '^helm .*<upgrade>' "$failure_case/commands.log"; then
  fail "update-failure: Helm wrote after update-kubeconfig failed"
fi

wrong_case=$(run_case wrong-current no)
[[ "$(grep -c '^kubectl' "$wrong_case/commands.log")" -eq 1 ]] \
  || fail "wrong-current: a Kubernetes command ran after context verification"
if grep -q '^helm .*<upgrade>' "$wrong_case/commands.log"; then
  fail "wrong-current: Helm wrote after context verification failed"
fi

printf 'ok: aws up uses one verified private kubeconfig across kubectl and Helm\n'
