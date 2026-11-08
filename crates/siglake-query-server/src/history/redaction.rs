//! Metadata-only SQL omission. Never invoke a parser/tokenizer on history input:
//! the current parser dependency can log original SQL, tokens and ASTs.
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RedactionStatus {
    Omitted,
    InputLimit,
}

impl RedactionStatus {
    pub(crate) fn parse(value: &str) -> Result<Self, super::HistoryError> {
        match value {
            "omitted" => Ok(Self::Omitted),
            "input_limit" => Ok(Self::InputLimit),
            _ => Err(super::HistoryError::Unavailable),
        }
    }
}

/// SQL is unavailable until a genuinely silent parsing path is reviewed.
/// No original input is retained; no arbitrary-string constructor, Deserialize
/// or Debug implementation is exposed.
///
/// ```compile_fail
/// use siglake_query_server::history::redaction::RedactedSql;
/// let forged = RedactedSql { sql: Some("raw SQL".into()), status: todo!(), version: 1 };
/// ```
/// ```compile_fail
/// use siglake_query_server::history::redaction::RedactedSql;
/// let forged: RedactedSql = serde_json::from_str(r#"{"sql":"raw SQL"}"#).unwrap();
/// ```
/// ```compile_fail
/// use siglake_query_server::history::redaction::RedactedSql;
/// println!("{:?}", RedactedSql::parse("raw SQL"));
/// ```
#[derive(Clone, Serialize)]
pub struct RedactedSql {
    sql: Option<String>,
    status: RedactionStatus,
    version: u8,
}

impl RedactedSql {
    pub(super) fn restore_omitted(status: RedactionStatus) -> Self {
        Self {
            sql: None,
            status,
            version: 1,
        }
    }
    /// Conservative omit-on-uncertainty fallback for ALL inputs, including valid
    /// SQL. This API name does not imply parsing or canonical rendering occurs.
    pub fn parse(original: &str) -> Self {
        Self {
            sql: None,
            status: if original.len() > 64 * 1024 {
                RedactionStatus::InputLimit
            } else {
                RedactionStatus::Omitted
            },
            version: 1,
        }
    }
    pub fn sql(&self) -> Option<&str> {
        self.sql.as_deref()
    }
    pub fn status(&self) -> RedactionStatus {
        self.status
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_omitted(original: &str, status: RedactionStatus) {
        let safe = RedactedSql::parse(original);
        assert_eq!(safe.sql(), None);
        assert_eq!(safe.status(), status);
        assert_eq!(
            serde_json::to_value(safe).unwrap(),
            serde_json::json!({"sql": null, "status": status, "version": 1})
        );
    }

    #[test]
    fn all_sql_is_omitted_without_classifying_grammar() {
        for original in [
            "",
            "SELECT \"SECRET_IDENTIFIER\", 'SECRET_LITERAL', 987654321, true, NULL, $1 AS secret_alias FROM secret_path.secret_table WHERE secret_col = $$SECRET_DOLLAR$$ /*SECRET_COMMENT*/",
            "WITH secret AS (SELECT 1) SELECT * FROM secret",
            "EXPLAIN SELECT secret_function('secret')",
            "SELECT 'secret' UNION SELECT 1",
            "SELECT DATE '2099-12-31', INTERVAL '123' DAY",
            "SELECT sum(secret) OVER (ROWS 123 PRECEDING)",
            "SELECT * FROM secret ORDER BY secret LIMIT 123 OFFSET 456",
            "SELECT 'secret'::secret_type",
            "SELECT 'secret' + FROM secret",
            "SELECT 'secret",
        ] {
            assert_omitted(original, RedactionStatus::Omitted);
        }
    }

    #[test]
    fn input_limit_and_complexity_never_invoke_parser() {
        assert_omitted(&"x".repeat(65536), RedactionStatus::Omitted);
        assert_omitted(&"x".repeat(65537), RedactionStatus::InputLimit);
        assert_omitted(
            &format!("SELECT {}'SECRET'{}", "(".repeat(500), ")".repeat(500)),
            RedactionStatus::Omitted,
        );
        assert_omitted(
            &format!("SELECT {}", vec!["SECRET"; 700].join(",")),
            RedactionStatus::Omitted,
        );
    }

    #[test]
    fn generated_metadata_noninterference() {
        for seed in 0..256 {
            let marker = format!("PRIVATE_SENTINEL_{seed}_é");
            for original in [
                format!("SELECT \"{marker}\" AS \"{marker}\", '{marker}', {} FROM \"{marker}\" WHERE \"{marker}\" = '{marker}'", 876540000 + seed),
                format!("SELECT $tag${marker}$tag$ -- {marker}"),
                format!("SELECT unusual_{seed}('{marker}') /* {marker} */"),
            ] {
                assert_omitted(&original, RedactionStatus::Omitted);
            }
        }
    }
}
