#!/usr/bin/env bash
# Verify loadgen reports its local-binary prerequisite before starting work.

set -euo pipefail

cd "$(dirname "$0")/.."

check_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-loadgen-check.XXXXXX")
trap 'rm -rf -- "$check_dir"' EXIT
mkdir "$check_dir/bin" "$check_dir/empty-target"

for command in curl mktemp; do
  cat >"$check_dir/bin/$command" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "${0##*/}" >>"${LOADGEN_CHECK_CALLS:?}"
exit 99
EOF
  chmod +x "$check_dir/bin/$command"
done

calls="$check_dir/calls"
if ! PATH="$check_dir/bin:$PATH" CARGO_TARGET_DIR="$check_dir/empty-target" \
    LOADGEN_CHECK_CALLS="$calls" scripts/loadgen.sh --help >/dev/null; then
  echo "FAIL loadgen --help requires the release binary" >&2
  exit 1
fi

if output=$(PATH="$check_dir/bin:$PATH" CARGO_TARGET_DIR="$check_dir/empty-target" \
    LOADGEN_CHECK_CALLS="$calls" scripts/loadgen.sh 2>&1); then
  echo "FAIL loadgen accepted a missing release binary" >&2
  exit 1
else
  status=$?
fi

if [ "$status" -ne 1 ]; then
  echo "FAIL missing loadgen exited $status, expected 1" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

for expected in \
    "siglake-loadgen is missing or not executable at $check_dir/empty-target/release/siglake-loadgen." \
    "  cargo build --release -p siglake-loadgen" \
    "  docker compose exec ingester siglake-loadgen ..."; do
  if ! grep -Fqx "$expected" <<<"$output"; then
    echo "FAIL missing-binary diagnostic did not include: $expected" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
done

if [ -e "$calls" ]; then
  echo "FAIL loadgen started work before checking its release binary" >&2
  sed 's/^/  called: /' "$calls" >&2
  exit 1
fi

mkdir "$check_dir/empty-target/release"
cat >"$check_dir/empty-target/release/siglake-loadgen" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$check_dir/empty-target/release/siglake-loadgen"

if PATH="$check_dir/bin:$PATH" CARGO_TARGET_DIR="$check_dir/empty-target" \
    LOADGEN_CHECK_CALLS="$calls" scripts/loadgen.sh >/dev/null 2>&1; then
  echo "FAIL loadgen fixture unexpectedly passed its failing pre-flight" >&2
  exit 1
fi
if [ "$(<"$calls")" != curl ]; then
  echo "FAIL executable loadgen did not proceed to the existing pre-flight" >&2
  sed 's/^/  called: /' "$calls" >&2
  exit 1
fi

echo "ok (missing release binary fails before pre-flight; executable binary proceeds; --help remains available)"
