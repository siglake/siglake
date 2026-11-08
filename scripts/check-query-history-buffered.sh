#!/usr/bin/env bash
# Buffered real-router proof in an owned disposable socket-only PG fixture.
# No target/URI argument accepted; shares the approved isolation helper.
set -euo pipefail
[[ $# == 0 ]] || { echo 'No database target arguments accepted' >&2; exit 2; }
root=$(cd "$(dirname "$0")/.." && pwd)
exec env -i PATH=/run/current-system/sw/bin:/usr/bin:/bin bash -s -- "$root" <<'ISOLATED_FIXTURE'
set -euo pipefail
umask 077
root=$1
source /data/homes/gianarb/.local/state/gastone-agent-roadmap/milestone-2-auth/pinned/env.sh
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR=/data/homes/gianarb/.local/state/gastone-agent-roadmap/milestone-2-ndjson/target
pg=/nix/store/zjpr4rk3vqlfz012idz4i6f04g5naiwz-postgresql-16.14/bin
for tool in initdb pg_ctl createdb psql postgres; do [[ -x "$pg/$tool" ]] || { echo "Missing cached Postgres tool: $tool" >&2; exit 1; }; done
work=$(mktemp -d /tmp/siglake-history-XXXXXX)
started=false
cleanup() {
  if $started; then "$pg/pg_ctl" -D "$work/data" -m immediate -w stop >/dev/null 2>&1 || true; fi
  rm -rf -- "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
source "$root/scripts/query-history-isolation.bash"
history_isolate_environment "$work"
cd "$work"
printf 'Cargo launch cwd: %s\nManifest: %s\nCompiler: %s\nCargo: %s\n' "$PWD" "$root/Cargo.toml" "$(command -v rustc)" "$(command -v cargo)"
# Verify the sanitized PATH resolves actual native pinned tools, not rustup.
for tool in cargo rustc rustdoc cargo-clippy clippy-driver rustfmt; do
  native=$(command -v "$tool")
  [[ $native == /data/homes/gianarb/.local/state/gastone-agent-roadmap/milestone-2-auth/pinned/toolchain/bin/* && ! -L $native ]] || exit 1
  [[ $(file -b "$native") == ELF* ]] || exit 1
done
[[ $(rustc --version) == 'rustc 1.95.0 '* ]] || exit 1
[[ $(cargo --version) == 'cargo 1.95.0 '* ]] || exit 1
[[ $(cargo clippy --version) == 'clippy 0.1.95 '* ]] || exit 1
"$pg/initdb" -D "$work/data" --username=fixture_admin --no-locale --encoding=UTF8 --auth=trust >"$work/init.log"
started=true
"$pg/pg_ctl" -D "$work/data" -l "$work/server.log" -o "-k $work -h '' -c log_min_error_statement=panic -c log_error_verbosity=terse" -w start >/dev/null
"$pg/createdb" -h "$work" -U fixture_admin qh_fixture
"$pg/psql" -X -v ON_ERROR_STOP=1 -h "$work" -U fixture_admin -d qh_fixture >"$work/setup.log" <<'SQL'
CREATE ROLE qh_owner NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE qh_writer LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE qh_reader LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION;
REVOKE ALL ON DATABASE qh_fixture FROM PUBLIC;
GRANT CONNECT ON DATABASE qh_fixture TO qh_writer,qh_reader;
GRANT CREATE ON DATABASE qh_fixture TO qh_owner;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA app_catalog;
CREATE TABLE app_catalog.secrets (value text);
REVOKE ALL ON SCHEMA app_catalog FROM PUBLIC;
REVOKE ALL ON app_catalog.secrets FROM PUBLIC;
SQL
"$pg/psql" -X -v ON_ERROR_STOP=1 -h "$work" -U fixture_admin -d qh_fixture -f "$root/crates/siglake-query-server/src/history/schema.sql" >>"$work/setup.log"
printf 'isolated-history-fixture-v1\n' > "$work/fixture-marker"
export SIGLAKE_HISTORY_FIXTURE_DIR="$work"
"$pg/postgres" --version
cargo --version
rustc --version
cargo clippy --version
timeout 600 cargo test --manifest-path "$root/Cargo.toml" --offline --locked -p siglake-query-server --test query_server oidc::buffered_history:: -- --include-ignored --test-threads=1
ISOLATED_FIXTURE
