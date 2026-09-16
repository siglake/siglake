#!/usr/bin/env bash
#
# Refuse an oversized session brief or a stale authoritative handoff pointer.
#
# The brief is read whole at the start of every session, and until 2026-09-02 it
# carried a 2,100-line reverse-chronological status log that left no room for
# the brief. The log moved to docs/internal/STATUS_LOG_2026-06_to_09.md and the
# brief's History section says new entries go there. It also says that a new
# handoff must repoint Current state; this gate makes both rules hold on the day
# a session forgets.
#
#   scripts/check-claude-md.sh          # the tracked brief: CI's shell job and ci-local.sh
#   scripts/check-claude-md.sh FILE     # a brief fixture beside docs/internal/
#
# One status line on stdout -- `ok (N lines)`, `ok (public tree: ...)`, or
# `skipped (...)` -- which
# scripts/ci-local.sh reports verbatim as its `claude-md` job. A failure is
# `FAIL ...` on stderr and exit 1.
#
# CLAUDE.md is one of the paths the public squash removes (EXCLUDED in
# scripts/check-public-tree.py) and this script ships, so the published tree
# has nothing to cap. When both the default file and docs/internal/ are absent,
# the tree has the public squash's shape and the check is applicable and green.
# If docs/internal/ remains, the same missing default file is still `skipped`,
# so ci-local.sh --strict rejects an unexpectedly unrunnable private-tree job.
# An explicit FILE that does not exist IS a failure -- the only reason to pass
# one is to check it. This is also the one shipping file allowed to name
# CLAUDE.md and docs/internal/ (EXPECTED in the checker); ci-local.sh and ci.yml
# refer to the brief only through this script, so neither needs an exemption of
# its own.

set -euo pipefail

MAX=200

fail() { # <file> <message> [annotate]
  local checked_file=$1 msg=$2 annotate=${3:-0}
  if [ "$annotate" -eq 1 ] && [ -n "${GITHUB_ACTIONS:-}" ]; then
    echo "::error file=$checked_file::$msg"
  fi
  echo "FAIL $msg" >&2
  return 1
}

missing_default_status() { # <repository root>
  local root=$1
  if [ ! -d "$root/docs/internal" ]; then
    echo "ok (public tree: CLAUDE.md removed by design)"
  else
    echo "skipped (no CLAUDE.md in this tree; docs/internal remains)"
  fi
}

check_brief() { # <repository root> <brief> [annotate]
  local root=$1 checked_file=$2 annotate=${3:-0}
  local lines current_state target newest newest_relative
  local -a handoffs

  lines=$(wc -l <"$checked_file")
  if [ "$lines" -gt "$MAX" ]; then
    fail "$checked_file" \
      "$checked_file has $lines lines (maximum $MAX); move status entries to docs/internal/STATUS_LOG_2026-06_to_09.md, not the brief" \
      "$annotate"
    return 1
  fi

  current_state=$(sed -n '/^## Current state$/,/^## /p' "$checked_file")
  target=$(sed -n \
    's/^- \*\*Authoritative status: `\(docs\/internal\/HANDOFF_[^`]*\.md\)`\*\*.*/\1/p' \
    <<<"$current_state")
  if [ -z "$target" ]; then
    fail "$checked_file" \
      "$checked_file Current state does not name an authoritative docs/internal/HANDOFF_*.md" \
      "$annotate"
    return 1
  fi
  if [ ! -f "$root/$target" ]; then
    fail "$checked_file" \
      "$checked_file authoritative status target $target does not exist" \
      "$annotate"
    return 1
  fi

  shopt -s nullglob
  handoffs=("$root"/docs/internal/HANDOFF_*.md)
  shopt -u nullglob
  newest=$(printf '%s\n' "${handoffs[@]}" | LC_ALL=C sort | tail -1)
  newest_relative=${newest#"$root"/}
  if [ "$target" != "$newest_relative" ]; then
    fail "$checked_file" \
      "$checked_file authoritative status target $target is stale; newest handoff is $newest_relative" \
      "$annotate"
    return 1
  fi
}

run_fixtures() {
  local fixture_dir case_dir output current newer missing
  fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-claude-md.XXXXXX")
  current=HANDOFF_2099-01-01.md
  newer=HANDOFF_2099-01-02.md
  missing=HANDOFF_2099-01-03.md

  case_dir=$fixture_dir/current
  mkdir -p "$case_dir/docs/internal"
  : >"$case_dir/docs/internal/$current"
  printf '%s\n' '## Current state' \
    "- **Authoritative status: \`docs/internal/$current\`**" \
    '## History' >"$case_dir/CLAUDE.md"
  if ! output=$(check_brief "$case_dir" "$case_dir/CLAUDE.md" 2>&1); then
    echo "FAIL current handoff fixture was rejected: $output" >&2
    rm -rf -- "$fixture_dir"
    return 1
  fi

  case_dir=$fixture_dir/stale
  mkdir -p "$case_dir/docs/internal"
  : >"$case_dir/docs/internal/$current"
  : >"$case_dir/docs/internal/$newer"
  printf '%s\n' '## Current state' \
    "- **Authoritative status: \`docs/internal/$current\`**" \
    '## History' >"$case_dir/CLAUDE.md"
  if output=$(check_brief "$case_dir" "$case_dir/CLAUDE.md" 2>&1) ||
    [[ $output != *"is stale; newest handoff is docs/internal/$newer"* ]]; then
    echo "FAIL stale handoff fixture was not rejected correctly: $output" >&2
    rm -rf -- "$fixture_dir"
    return 1
  fi

  case_dir=$fixture_dir/missing
  mkdir -p "$case_dir/docs/internal"
  : >"$case_dir/docs/internal/$newer"
  printf '%s\n' '## Current state' \
    "- **Authoritative status: \`docs/internal/$missing\`**" \
    '## History' >"$case_dir/CLAUDE.md"
  if output=$(check_brief "$case_dir" "$case_dir/CLAUDE.md" 2>&1) ||
    [[ $output != *"target docs/internal/$missing does not exist"* ]]; then
    echo "FAIL missing handoff fixture was not rejected correctly: $output" >&2
    rm -rf -- "$fixture_dir"
    return 1
  fi

  case_dir=$fixture_dir/public-tree
  mkdir -p "$case_dir"
  output=$(missing_default_status "$case_dir")
  if [ "$output" != "ok (public tree: CLAUDE.md removed by design)" ]; then
    echo "FAIL public-tree missing-default fixture was not green: $output" >&2
    rm -rf -- "$fixture_dir"
    return 1
  fi

  case_dir=$fixture_dir/private-tree
  mkdir -p "$case_dir/docs/internal"
  output=$(missing_default_status "$case_dir")
  if [[ $output != skipped* ]]; then
    echo "FAIL private-tree missing-default fixture was not skipped: $output" >&2
    rm -rf -- "$fixture_dir"
    return 1
  fi

  rm -rf -- "$fixture_dir"
}

case $# in
  0)
    cd "$(dirname "$0")/.."
    file=CLAUDE.md
    if [ ! -f "$file" ]; then
      missing_default_status .
      exit 0
    fi
    ;;
  1)
    file=$1
    if [ ! -f "$file" ]; then
      echo "FAIL $file does not exist" >&2
      exit 1
    fi
    ;;
  *)
    echo "usage: $0 [FILE]" >&2
    exit 2
    ;;
esac

root=$(dirname "$file")
lines=$(wc -l <"$file")
check_brief "$root" "$file" 1
run_fixtures
echo "ok ($lines lines)"
