#!/usr/bin/env bash
# OFFLINE ONLY: creates/marks a NEW private socket-only PG16.14 cluster. No target
# arguments, shared DB credentials, provider calls, or runtime activation.
set -euo pipefail
[[ $# == 0 ]] || { echo 'No database target arguments accepted' >&2; exit 2; }
root=$(cd "$(dirname "$0")/.." && pwd)
exec env -i PATH=/run/current-system/sw/bin:/usr/bin:/bin bash -s -- "$root" <<'ISOLATED_POLICY'
set -euo pipefail
umask 077
root=$1
source /data/homes/gianarb/.local/state/gastone-agent-roadmap/milestone-2-auth/pinned/env.sh
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR=/data/homes/gianarb/.local/state/gastone-agent-roadmap/milestone-2-ndjson/target
pg=/nix/store/zjpr4rk3vqlfz012idz4i6f04g5naiwz-postgresql-16.14/bin
# Owned TLS regressions additionally need the already-cached public OpenSSL tool.
openssl_bin=/nix/store/clvbx203wyqkdnqc5ngyhjlhcqn5s9x9-openssl-3.6.2-bin/bin
[[ -x "$openssl_bin/openssl" ]] || { echo 'Missing cached TLS fixture tool' >&2; exit 1; }
export PATH="$pg:$openssl_bin:$PATH"
for tool in initdb pg_ctl createdb psql postgres; do [[ -x "$pg/$tool" ]] || exit 1; done
# Resolve real native tools BEFORE the private HOME; never rustup or fallback.
for tool in cargo rustc rustdoc cargo-clippy clippy-driver rustfmt; do
 native=$(command -v "$tool")
 [[ $native == /data/homes/gianarb/.local/state/gastone-agent-roadmap/milestone-2-auth/pinned/toolchain/bin/* && ! -L $native && $(file -b "$native") == ELF* ]] || exit 1
done
[[ $(rustc --version) == 'rustc 1.95.0 '* && $(cargo --version) == 'cargo 1.95.0 '* && $(cargo clippy --version) == 'clippy 0.1.95 '* ]] || exit 1
work=$(mktemp -d /tmp/siglake-policy-XXXXXX)
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
printf 'Private cwd: %s\nExplicit manifest: %s\nNot a sandbox; trusted host root/tmp configuration.\n' "$PWD" "$root/Cargo.toml"
"$pg/initdb" -D "$work/data" --username=fixture_admin --no-locale --encoding=UTF8 --auth=trust >"$work/init.log"
printf 'new-owned-policy-fixture-v1\n' > "$work/fixture-marker"
started=true
"$pg/pg_ctl" -D "$work/data" -l "$work/server.log" -o "-k $work -h '' -c log_min_messages=panic -c log_min_error_statement=panic -c log_error_verbosity=terse" -w start >/dev/null
"$pg/createdb" -h "$work" -U fixture_admin qh_fixture
"$pg/psql" -X -v ON_ERROR_STOP=1 -h "$work" -U fixture_admin -d qh_fixture >"$work/setup.log" <<'SQL'
CREATE ROLE qp_owner NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT;
CREATE ROLE qp_reader LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT;
CREATE ROLE qp_manager LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT;
CREATE ROLE qh_reader LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE qh_writer LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE fixture_other NOLOGIN;
REVOKE ALL ON DATABASE qh_fixture FROM PUBLIC;
GRANT CONNECT ON DATABASE qh_fixture TO qp_reader,qp_manager,qh_reader,qh_writer;
GRANT CREATE ON DATABASE qh_fixture TO qp_owner;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA app_catalog;
REVOKE ALL ON SCHEMA app_catalog FROM PUBLIC;
CREATE TABLE app_catalog.secrets (value text);
REVOKE ALL ON app_catalog.secrets FROM PUBLIC;
SQL
"$pg/psql" -X -v ON_ERROR_STOP=1 -h "$work" -U fixture_admin -d qh_fixture -f "$root/crates/siglake-query-server/src/query_controls/schema.sql" >>"$work/setup.log"
"$pg/psql" -X -v ON_ERROR_STOP=1 -h "$work" -U fixture_admin -d qh_fixture -c 'REVOKE CREATE ON DATABASE qh_fixture FROM qp_owner' >>"$work/setup.log"
export SIGLAKE_POLICY_FIXTURE_DIR="$work"
timeout 600 cargo test --manifest-path "$root/Cargo.toml" --offline --locked -p siglake-query-server --lib query_controls:: -- --include-ignored --test-threads=1
timeout 600 cargo test --manifest-path "$root/Cargo.toml" --offline --locked -p siglake-query-server --test query_server oidc::buffered_controls:: -- --include-ignored --test-threads=1
ISOLATED_POLICY
