//! A separate test binary owns the global logger/subscriber. Captures real `log`
//! calls (including sqlparser's DEBUG diagnostics), not just tracing events.
use siglake_query_server::history::{
    redaction::{RedactedSql, RedactionStatus},
    HistoryError,
};
use std::{
    io::Write,
    sync::{Arc, Mutex},
};

struct CaptureLog(Mutex<Vec<String>>);
static LOGGER: CaptureLog = CaptureLog(Mutex::new(Vec::new()));
impl log::Log for CaptureLog {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        self.0.lock().unwrap().push(format!(
            "{} {} {}",
            record.level(),
            record.target(),
            record.args()
        ));
    }
    fn flush(&self) {}
}

#[derive(Clone)]
struct CaptureTrace(Arc<Mutex<Vec<u8>>>);
impl Write for CaptureTrace {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn history_omission_never_logs_sql_at_debug_or_trace() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    let tracing_output = CaptureTrace(Arc::new(Mutex::new(Vec::new())));
    let sink = tracing_output.clone();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .without_time()
            .with_writer(move || sink.clone())
            .finish(),
    )
    .unwrap();

    // Prove both global facilities actually capture DEBUG/TRACE, including the
    // dependency targets responsible for the original SQL/AST/token leak.
    log::debug!(target: "sqlparser::parser", "HISTORY_LOG_DEBUG_CONTROL");
    log::trace!(target: "sqlparser::dialect", "HISTORY_LOG_TRACE_CONTROL");
    tracing::debug!("HISTORY_TRACING_DEBUG_CONTROL");
    tracing::trace!("HISTORY_TRACING_TRACE_CONTROL");
    {
        let mut logs = LOGGER.0.lock().unwrap();
        assert!(logs
            .iter()
            .any(|line| line.contains("HISTORY_LOG_DEBUG_CONTROL")));
        assert!(logs
            .iter()
            .any(|line| line.contains("HISTORY_LOG_TRACE_CONTROL")));
        logs.clear();
        let mut trace = tracing_output.0.lock().unwrap();
        let control = String::from_utf8(trace.clone()).unwrap();
        assert!(control.contains("HISTORY_TRACING_DEBUG_CONTROL"));
        assert!(control.contains("HISTORY_TRACING_TRACE_CONTROL"));
        trace.clear();
    }

    for original in [
        // Previously supported SELECT: identifiers, values, path, alias, comment.
        "SELECT \"PRIVATE_IDENTIFIER\", 'PRIVATE_VALUE', 987654321 AS PRIVATE_ALIAS FROM PRIVATE_SCHEMA.PRIVATE_TABLE WHERE PRIVATE_COLUMN = $$PRIVATE_DOLLAR$$ /* PRIVATE_COMMENT */",
        // Valid but previously unsupported function/CTE/window/EXPLAIN shapes.
        "SELECT PRIVATE_FUNCTION('PRIVATE_VALUE') /* PRIVATE_COMMENT */",
        "WITH PRIVATE_CTE AS (SELECT 'PRIVATE_VALUE') SELECT * FROM PRIVATE_CTE",
        "EXPLAIN SELECT 'PRIVATE_VALUE' FROM PRIVATE_TABLE",
        // Tokenizable parse errors: these reach parser diagnostics, unlike a
        // tokenizer-rejected unterminated string used by the original test.
        "SELECT 'PRIVATE_VALUE' + FROM PRIVATE_TABLE /* PRIVATE_COMMENT */",
        "SELECT PRIVATE_IDENTIFIER FROM PRIVATE_TABLE WHERE ) -- PRIVATE_COMMENT",
    ] {
        let safe = RedactedSql::parse(original);
        assert_eq!(safe.sql(), None);
        assert_eq!(safe.status(), RedactionStatus::Omitted);
        assert_eq!(
            serde_json::to_string(&safe).unwrap(),
            r#"{"sql":null,"status":"omitted","version":1}"#
        );
        for error in [
            HistoryError::InvalidValue,
            HistoryError::Unavailable,
            HistoryError::Privilege,
            HistoryError::Conflict,
            HistoryError::ExpiredOrAbsent,
        ] {
            let diagnostic = format!("{error:?}: {error}");
            assert!(!diagnostic.contains("PRIVATE_"));
            assert!(!diagnostic.contains(original));
        }
    }
    // No output at all is stronger than checking only a sentinel: SQL keywords,
    // token/AST formatting and partial fragments cannot silently survive either.
    assert!(LOGGER.0.lock().unwrap().is_empty());
    assert!(tracing_output.0.lock().unwrap().is_empty());
}
