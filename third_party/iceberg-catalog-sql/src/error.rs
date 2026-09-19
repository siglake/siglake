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
pub fn from_sqlx_error(error: sqlx::Error) -> Error {
    let kind = if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        ErrorKind::TableAlreadyExists
    } else {
        ErrorKind::Unexpected
    };
    Error::new(kind, "operation failed for hitting sqlx error").with_source(error)
}

pub fn no_such_namespace_err<T>(namespace: &NamespaceIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::NamespaceNotFound,
        format!("No such namespace: {namespace:?}"),
    ))
}

pub fn no_such_table_err<T>(table_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::TableNotFound,
        format!("No such table: {table_ident:?}"),
    ))
}

pub fn table_already_exists_err<T>(table_ident: &TableIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::TableAlreadyExists,
        format!("Table {table_ident:?} already exists."),
    ))
}

pub fn namespace_already_exists_err<T>(namespace: &NamespaceIdent) -> Result<T> {
    Err(Error::new(
        ErrorKind::NamespaceAlreadyExists,
        format!("Namespace {namespace:?} already exists"),
    ))
}

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

    #[tokio::test]
    async fn sqlite_unique_violations_keep_create_race_kinds() {
        sqlx::any::install_default_drivers();
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE race (name TEXT PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO race(name) VALUES ('same')")
            .execute(&pool)
            .await
            .unwrap();
        let duplicate = sqlx::query("INSERT INTO race(name) VALUES ('same')")
            .execute(&pool)
            .await
            .unwrap_err();

        let table_error = from_sqlx_error(duplicate);
        assert_eq!(table_error.kind(), ErrorKind::TableAlreadyExists);
        let namespace = NamespaceIdent::new("race".into());
        let namespace_error = namespace_insert_error(&namespace, table_error);
        assert_eq!(namespace_error.kind(), ErrorKind::NamespaceAlreadyExists);
    }
}
