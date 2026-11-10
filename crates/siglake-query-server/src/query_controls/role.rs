//! Fail-closed role AND schema-integrity check, independently of history roles.
use super::{PolicyError, Role};
use sqlx::{Connection, PgConnection};
pub(super) async fn verify(conn: &mut PgConnection, role: Role) -> Result<(), PolicyError> {
    if conn.is_in_transaction() {
        return Err(PolicyError::Unavailable);
    }
    // Do not let a role/database search_path shadow catalog-check functions.
    sqlx::query("SET search_path=pg_catalog")
        .execute(&mut *conn)
        .await
        .map_err(|_| PolicyError::Unavailable)?;
    let safe: bool = sqlx::query_scalar(r#"
 SELECT current_user=$1 AND session_user=$1 AND r.rolcanlogin AND NOT pg_is_in_recovery()
 AND current_setting('row_security')='on' AND current_setting('session_replication_role')='origin'
 AND NOT (r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolinherit)
 AND EXISTS (SELECT 1 FROM pg_roles o WHERE o.rolname='qp_owner' AND NOT (o.rolcanlogin OR o.rolsuper OR o.rolbypassrls OR o.rolcreaterole OR o.rolcreatedb OR o.rolreplication OR o.rolinherit)
   AND NOT EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member=o.oid OR m.roleid=o.oid))
 AND NOT EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member=r.oid OR m.roleid=r.oid)
 AND NOT EXISTS (SELECT 1 FROM pg_shdepend d WHERE d.refclassid='pg_authid'::regclass AND d.refobjid=r.oid AND d.deptype='o')
 AND NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspowner=r.oid)
 AND NOT has_database_privilege(current_user,current_database(),'CREATE,TEMP')
 AND has_schema_privilege(current_user,'query_policy','USAGE')
 AND has_table_privilege(current_user,'query_policy.agent_limits','SELECT')
 AND NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE left(n.nspname,3) <> 'pg_' AND n.nspname <> 'information_schema' AND has_schema_privilege(current_user,n.oid,'CREATE'))
 AND NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE left(n.nspname,3) <> 'pg_' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','p','v','m','S','f')
    AND (n.nspname <> 'query_policy' OR c.relname <> 'agent_limits')
    AND (has_table_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
      OR has_any_column_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,REFERENCES')))
 AND NOT has_table_privilege(current_user,'query_policy.agent_limits','INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
 AND NOT has_any_column_privilege(current_user,'query_policy.agent_limits','INSERT,UPDATE,REFERENCES')
 AND NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
   WHERE left(n.nspname,3) <> 'pg_' AND n.nspname <> 'information_schema' AND has_function_privilege(current_user,p.oid,'EXECUTE')
    AND NOT ($1='qp_manager' AND n.nspname='query_policy' AND p.oid IN ('query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint)'::regprocedure, 'query_policy.read_edits(text,text,text,uuid,uuid,bigint,integer)'::regprocedure)))
 AND ($1 <> 'qp_manager' OR (has_function_privilege(current_user,'query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint)','EXECUTE')
   AND has_function_privilege(current_user,'query_policy.read_edits(text,text,text,uuid,uuid,bigint,integer)','EXECUTE')))
 FROM pg_roles r WHERE r.rolname=current_user
 "#).bind(role.name()).fetch_one(&mut *conn).await.map_err(|_| PolicyError::Unavailable)?;
    if !safe {
        return Err(PolicyError::Unsafe);
    }
    let actual: serde_json::Value = sqlx::query_scalar(include_str!("integrity.sql"))
        .fetch_one(conn)
        .await
        .map_err(|_| PolicyError::Unavailable)?;
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("schema-layout-v2-contract.json"))
            .expect("reviewed policy manifest");
    if actual != expected {
        return Err(PolicyError::Unsafe);
    }
    Ok(())
}
