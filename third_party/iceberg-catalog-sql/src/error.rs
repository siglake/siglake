// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use iceberg::{Error, ErrorKind, NamespaceIdent, Result, TableIdent};

/// Format an sqlx error into iceberg error.
///
/// Unique-constraint violations map to `ErrorKind::TableAlreadyExists`: the
/// catalog's create paths are check-then-insert, so two processes racing a
/// create against a fresh namespace both pass the check and the loser hits
/// the `iceberg_tables` UNIQUE constraint at INSERT time (SQLite code 1555,
/// Postgres 23505). Callers discriminate on the kind to treat a lost
/// creation race as success — surfacing it as `Unexpected` failed service
/// startup (WI-8 local-A/B finding).
pub fn from_sqlx_error(error: sqlx::Error) -> Error {
    let is_unique_violation = error
        .as_database_error()
        .is_some_and(|db| db.is_unique_violation());
    let kind = if is_unique_violation {
        ErrorKind::TableAlreadyExists
    } else {
        ErrorKind::Unexpected
    };
    Error::new(kind, "operation failed for hitting sqlx error".to_string()).with_source(error)
}

pub fn no_such_namespace_err<T>(namespace: &NamespaceIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::Unexpected,
        format!("No such namespace: {namespace:?}"),
    ))
}

pub fn no_such_table_err<T>(table_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::Unexpected,
        format!("No such table: {table_ident:?}"),
    ))
}

pub fn table_already_exists_err<T>(table_ident: &TableIdent) -> Result<T> {
    // ErrorKind::TableAlreadyExists (not Unexpected): callers racing an
    // ensure-style create (two siglake services booting against a fresh
    // namespace) discriminate on the kind to treat a lost creation race as
    // success. The memory catalog already returns this kind; the SQL
    // catalog returning Unexpected broke that tolerance (WI-8 finding).
    Err(Error::new(
        ErrorKind::TableAlreadyExists,
        format!("Table {table_ident:?} already exists."),
    ))
}

pub fn namespace_already_exists_err<T>(namespace: &NamespaceIdent) -> Result<T> {
    // ErrorKind::NamespaceAlreadyExists (not Unexpected), the kind the memory
    // catalog returns: the namespace half of the same race. Ingester and
    // query server both bootstrap the namespace at startup with
    // check-then-create, and the loser has to tell "already there" from a
    // real failure to keep booting.
    Err(Error::new(
        ErrorKind::NamespaceAlreadyExists,
        format!("Namespace {namespace:?} already exists"),
    ))
}

/// Re-kind a failed namespace-properties INSERT.
///
/// [`from_sqlx_error`] cannot tell which table's constraint fired, so the
/// PRIMARY KEY collision two racing `create_namespace` calls produce (both
/// pass the existence check, both INSERT the default `exists=true` row)
/// arrives as `TableAlreadyExists`. Callers tolerating a lost namespace race
/// match on `NamespaceAlreadyExists`; hand them that, keeping the sqlx error
/// as the source.
pub fn namespace_insert_error(namespace: &NamespaceIdent, err: Error) -> Error {
    if err.kind() == ErrorKind::TableAlreadyExists {
        Error::new(
            ErrorKind::NamespaceAlreadyExists,
            format!("Namespace {namespace:?} already exists"),
        )
        .with_source(err)
    } else {
        err
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_insert_unique_violation_becomes_namespace_already_exists() {
        let namespace = NamespaceIdent::new("siglake".into());
        let lost = Error::new(
            ErrorKind::TableAlreadyExists,
            "operation failed for hitting sqlx error",
        );
        let mapped = namespace_insert_error(&namespace, lost);
        assert!(
            mapped.kind() == ErrorKind::NamespaceAlreadyExists,
            "{mapped}"
        );
        assert!(mapped.to_string().contains("already exists"), "{mapped}");
    }

    #[test]
    fn namespace_insert_other_errors_pass_through() {
        let namespace = NamespaceIdent::new("siglake".into());
        let other = Error::new(ErrorKind::Unexpected, "database is locked");
        let mapped = namespace_insert_error(&namespace, other);
        assert!(mapped.kind() == ErrorKind::Unexpected, "{mapped}");
        assert!(
            mapped.to_string().contains("database is locked"),
            "{mapped}"
        );
    }
}
