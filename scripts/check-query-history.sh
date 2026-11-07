#!/usr/bin/env bash
# Own, disposable, Unix-loopback-only fixture. No target/URI argument accepted.
set -euo pipefail
[[ $# == 0 ]] || { echo 'No database target arguments accepted' >&2; exit 2; }
root=$(cd "$(dirname "$0")/.." && pwd)
exec env -i HOME="$HOME" PATH=/run/current-system/sw/bin:/usr/bin:/bin bash -s -- "$root" <<'ISOLATED_FIXTURE'
set -euo pipefail
umask 077
cd "$1"
source /data/homes/gianarb/.local/state/gastone-agent-roadmap/milestone-2-auth/pinned/env.sh
pg=/nix/store/zjpr4rk3vqlfz012idz4i6f04g5naiwz-postgresql-16.14/bin
for tool in initdb pg_ctl createdb psql postgres; do [[ -x "$pg/$tool" ]] || { echo "Missing cached Postgres tool: $tool" >&2; exit 1; }; done
[[ $(rustc --version) == 'rustc 1.95.0 '* ]] || exit 1
[[ $(cargo --version) == 'cargo 1.95.0 '* ]] || exit 1
[[ $(cargo clippy --version) == 'clippy 0.1.95 '* ]] || exit 1
work=$(mktemp -d /tmp/siglake-history-XXXXXX)
started=false
cleanup() {
  if $started; then "$pg/pg_ctl" -D "$work/data" -m immediate -w stop >/dev/null 2>&1 || true; fi
  rm -rf -- "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
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
"$pg/psql" -X -v ON_ERROR_STOP=1 -h "$work" -U fixture_admin -d qh_fixture -f crates/siglake-query-server/src/history/schema.sql >>"$work/setup.log"
printf 'isolated-history-fixture-v1\n' > "$work/fixture-marker"
export SIGLAKE_HISTORY_FIXTURE_DIR="$work"
"$pg/postgres" --version
cargo --version
rustc --version
cargo clippy --version
timeout 600 cargo test --locked -p siglake-query-server --test history_redaction_logging
timeout 600 cargo test --locked -p siglake-query-server --lib history:: -- --include-ignored --test-threads=1
# Separate, fixed synthetic row proves a real server restart, not just a new pool.
"$pg/psql" -X -v ON_ERROR_STOP=1 -h "$work" -U qh_writer -d qh_fixture >"$work/restart.log" <<'SQL'
WITH t AS MATERIALIZED (SELECT clock_timestamp() AS now)
INSERT INTO query_history.executions(execution_id,tenant,principal_kind,received_at,expires_at,owner_uuid,operation,format,priority,dry_run,redaction_version,redaction_status)
SELECT 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','restart-fixture','unknown',now,now+interval '720 hours','bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb','sql','records','interactive',false,1,'omitted' FROM t;
SQL
"$pg/pg_ctl" -D "$work/data" -m fast -w stop >/dev/null
"$pg/pg_ctl" -D "$work/data" -l "$work/server.log" -o "-k $work -h '' -c log_min_error_statement=panic -c log_error_verbosity=terse" -w start >/dev/null
[[ $("$pg/psql" -X -At -v ON_ERROR_STOP=1 -h "$work" -U fixture_admin -d qh_fixture -c "SHOW fsync") == on ]]
[[ $("$pg/psql" -X -At -v ON_ERROR_STOP=1 -h "$work" -U fixture_admin -d qh_fixture -c "SELECT count(*)=1 FROM query_history.executions WHERE execution_id='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'") == t ]]
echo 'Postgres server restart: persisted synthetic row verified (fsync=on)'
ISOLATED_FIXTURE
