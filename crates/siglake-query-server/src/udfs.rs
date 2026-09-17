//! Search-time SQL UDFs registered on the query `SessionContext`.
//!
//! ## `kv_extract(raw, key) → Utf8`
//!
//! Given a `_raw` event body and a field name, find a `<key>=<value>`
//! substring inside `raw` and return `<value>`. Supports both bare and quoted
//! values: `status=500` ⇒ `"500"`, `path="/api/v1/foo"` ⇒ `"/api/v1/foo"`.
//! No match ⇒ NULL. Lets SQL queries pull fields out of unstructured `raw`
//! text, e.g. `SELECT kv_extract(raw, 'status') FROM events`.
//!
//! Relocated from the deleted query-language crate when the query layer went
//! SQL-only (`docs/design/00-architecture-changes-2026-06.md` §1) — this is a
//! generally-useful SQL UDF, independent of that removed pipeline.
//!
//! ## `attr_get(attributes, key) → Utf8`
//!
//! Given the WS-7 `attributes` column (a JSON object string of the residual OTLP
//! attributes) and a key, return that attribute's value as text. Numbers + bools
//! are stringified (`500`, `true`); nested arrays/objects return their JSON. No
//! match / JSON null / unparseable / NULL input ⇒ NULL. Makes captured OTel
//! attributes first-class in SQL, e.g.
//! `SELECT attr_get(attributes,'k8s.namespace') ns, count(*) FROM events GROUP BY ns`
//! or `WHERE CAST(attr_get(attributes,'http.status_code') AS INT) >= 500`.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, StringBuilder};
use arrow_array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use regex::Regex;
use siglake_bloom::Tokenizer;

pub const KV_EXTRACT_FN: &str = "kv_extract";
pub const ATTR_GET_FN: &str = "attr_get";
pub const MATCH_TERMS_FN: &str = "match_terms";
pub const MATCH_ANY_FN: &str = "match_any";
pub const MATCH_PHRASE_FN: &str = "match_phrase";
pub const MATCH_PREFIX_FN: &str = "match_prefix";
pub const MATCH_ALIAS_FN: &str = "match";

/// Register the query-layer UDFs on a `SessionContext`. Idempotent:
/// re-registering replaces the prior definition.
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(KvExtractUdf::new()));
    ctx.register_udf(ScalarUDF::from(AttrGetUdf::new()));
    ctx.register_udf(ScalarUDF::from(MatchUdf::new(
        MATCH_TERMS_FN,
        MatchMode::AllTerms,
    )));
    ctx.register_udf(ScalarUDF::from(MatchUdf::new(
        MATCH_ANY_FN,
        MatchMode::AnyTerm,
    )));
    ctx.register_udf(ScalarUDF::from(MatchUdf::new(
        MATCH_PHRASE_FN,
        MatchMode::Phrase,
    )));
    ctx.register_udf(ScalarUDF::from(MatchUdf::new(
        MATCH_PREFIX_FN,
        MatchMode::Prefix,
    )));
    // WI-2: register `match(...)` as an alias for term-AND search. The parser
    // acceptance is verified in tests; if the SQL dialect ever reserves MATCH,
    // registration remains harmless and callers still have `match_terms(...)`.
    ctx.register_udf(ScalarUDF::from(MatchUdf::new(
        MATCH_ALIAS_FN,
        MatchMode::AllTerms,
    )));
}

/// Look up `key` in a JSON-object string and return its value as text:
/// string → as-is, number/bool → their literal, array/object → compact JSON,
/// JSON null / missing key / unparseable ⇒ `None`.
///
/// Scans the top-level object for the key and parses **only** the matched value
/// — it never builds the full object map or parses the other values, so an
/// `attr_get(...)` over a wide `attributes` blob is much cheaper than a full
/// `serde_json` parse per row.
fn json_attr_lookup(attributes: &str, key: &str) -> Option<String> {
    let raw = find_value_path(attributes, key)?;
    match serde_json::from_str::<serde_json::Value>(raw).ok()? {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s),
        // Number → "500", Bool → "true", Array/Object → their JSON.
        v => Some(v.to_string()),
    }
}

/// [`find_top_level_value`] with dotted-path fallback: a literal top-level
/// `"a.b"` key wins; otherwise `a.b` walks INTO nested objects one segment at
/// a time — OTLP residuals hold nested objects (`cloud.provider`,
/// `http.method`, …), and WS-7 promotion addresses their leaves by dotted key.
fn find_value_path<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    if let Some(v) = find_top_level_value(json, key) {
        return Some(v);
    }
    let (head, tail) = key.split_once('.')?;
    let inner = find_top_level_value(json, head)?;
    find_value_path(inner, tail)
}

/// Scan a top-level JSON object for `key` and return the raw slice of its value
/// (`500`, `"prod"`, `[1,2]`, …) without parsing the rest. `None` if the input
/// isn't an object, the key is absent, or it's malformed.
fn find_top_level_value<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let b = json.as_bytes();
    let mut i = skip_ws(b, 0);
    if *b.get(i)? != b'{' {
        return None;
    }
    i += 1;
    loop {
        i = skip_ws(b, i);
        match *b.get(i)? {
            b'}' => return None, // end of object, key not found
            b'"' => {}
            _ => return None, // malformed
        }
        let key_quote = i;
        let (k_start, k_end, after_key) = scan_string(b, i)?;
        i = skip_ws(b, after_key);
        if *b.get(i)? != b':' {
            return None;
        }
        i = skip_ws(b, i + 1);
        let v_start = i;
        let v_end = scan_value_end(b, i)?;

        let raw_key = &json[k_start..k_end];
        let matches = if raw_key.as_bytes().contains(&b'\\') {
            // Rare escaped key — unescape the quoted form for a correct compare.
            serde_json::from_str::<String>(&json[key_quote..after_key])
                .ok()
                .as_deref()
                == Some(key)
        } else {
            raw_key == key
        };
        if matches {
            return Some(&json[v_start..v_end]);
        }

        i = skip_ws(b, v_end);
        match *b.get(i)? {
            b',' => i += 1,
            _ => return None, // `}` (key not found) or malformed
        }
    }
}

fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while matches!(b.get(i), Some(b' ' | b'\t' | b'\n' | b'\r')) {
        i += 1;
    }
    i
}

/// `b[i]` must be `"`. Returns `(inner_start, inner_end, after_close)`.
fn scan_string(b: &[u8], i: usize) -> Option<(usize, usize, usize)> {
    let start = i + 1;
    let mut j = start;
    while j < b.len() {
        match b[j] {
            b'\\' => j += 2, // skip the escaped char
            b'"' => return Some((start, j, j + 1)),
            _ => j += 1,
        }
    }
    None
}

/// Index just past the JSON value starting at `b[i]` (string/number/bool/null/
/// array/object), honoring nested structures + strings. `None` if malformed.
fn scan_value_end(b: &[u8], i: usize) -> Option<usize> {
    match b.get(i)? {
        b'"' => scan_string(b, i).map(|(_, _, after)| after),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut j = i;
            while j < b.len() {
                match b[j] {
                    b'"' => j = scan_string(b, j)?.2,
                    b'{' | b'[' => {
                        depth += 1;
                        j += 1;
                    }
                    b'}' | b']' => {
                        depth -= 1;
                        j += 1;
                        if depth == 0 {
                            return Some(j);
                        }
                    }
                    _ => j += 1,
                }
            }
            None
        }
        _ => {
            // Scalar (number/true/false/null): up to the next delimiter.
            let mut j = i;
            while j < b.len() && !matches!(b[j], b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
            {
                j += 1;
            }
            (j > i).then_some(j)
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct AttrGetUdf {
    signature: Signature,
}

impl AttrGetUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for AttrGetUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        ATTR_GET_FN
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        // Materialize both args to arrays and dispatch row-by-row. The common
        // shape is `attr_get(col, 'literal')`; even then the per-row work is a
        // JSON parse, so there's no per-batch setup worth special-casing.
        let n = args.number_rows;
        let attrs = arg_as_str_array(&args.args[0], n, "attributes")?;
        let keys = arg_as_str_array(&args.args[1], n, "key")?;
        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            if attrs.is_null(i) || keys.is_null(i) {
                builder.append_null();
                continue;
            }
            match json_attr_lookup(attrs.value(i), keys.value(i)) {
                Some(v) => builder.append_value(&v),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Materialize a UDF arg (array or scalar) into a `StringArray` of length `n`.
fn arg_as_str_array(arg: &ColumnarValue, n: usize, label: &str) -> DfResult<Arc<StringArray>> {
    let arr = match arg {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(n)?,
    };
    arr.as_any()
        .downcast_ref::<StringArray>()
        .map(|a| Arc::new(a.clone()))
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("{label} must be Utf8"))
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MatchMode {
    AllTerms,
    AnyTerm,
    Phrase,
    Prefix,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MatchUdf {
    name: &'static str,
    mode: MatchMode,
    signature: Signature,
}

impl MatchUdf {
    fn new(name: &'static str, mode: MatchMode) -> Self {
        Self {
            name,
            mode,
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MatchUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let n = args.number_rows;
        let cells = arg_as_str_array(&args.args[0], n, self.name)?;
        let tokenizer = match_tokenizer(&args.arg_fields[0]);
        match &args.args[1] {
            ColumnarValue::Scalar(query) => {
                let query = query_scalar_string(query, self.name)?;
                let query_tokens = tokenize_query_literal(self.name, tokenizer, &query)?;
                let mut builder = BooleanBuilder::with_capacity(n);
                for i in 0..n {
                    if cells.is_null(i) {
                        builder.append_value(false);
                        continue;
                    }
                    builder.append_value(match_raw(
                        self.mode,
                        tokenizer,
                        cells.value(i),
                        &query_tokens,
                    ));
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Array(_) => {
                let queries = arg_as_str_array(&args.args[1], n, self.name)?;
                let mut builder = BooleanBuilder::with_capacity(n);
                for i in 0..n {
                    if cells.is_null(i) {
                        builder.append_value(false);
                        continue;
                    }
                    if queries.is_null(i) {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "{}: query must be a non-null Utf8 literal",
                            self.name
                        )));
                    }
                    let query = queries.value(i);
                    let query_tokens = tokenize_query_literal(self.name, tokenizer, query)?;
                    builder.append_value(match_raw(
                        self.mode,
                        tokenizer,
                        cells.value(i),
                        &query_tokens,
                    ));
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }
    }
}

fn query_scalar_string(value: &ScalarValue, name: &str) -> DfResult<String> {
    match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Ok(s.clone()),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) => {
            Err(datafusion::error::DataFusionError::Execution(format!(
                "{name}: query must be a non-null Utf8 literal"
            )))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "{name}: query must be Utf8"
        ))),
    }
}

fn match_tokenizer(field: &Field) -> Tokenizer {
    field
        .metadata()
        .get(siglake_storage::TEXT_TOKENIZER_FIELD_METADATA_KEY)
        .and_then(|name| Tokenizer::parse(name))
        .unwrap_or(Tokenizer::Default)
}

fn tokenize_query_literal(name: &str, tokenizer: Tokenizer, query: &str) -> DfResult<Vec<String>> {
    let tokens = tokenizer.tokenize(query);
    if tokens.is_empty() {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "{name}: query must contain at least one indexed token"
        )));
    }
    Ok(tokens)
}

fn match_raw(mode: MatchMode, tokenizer: Tokenizer, raw: &str, query_tokens: &[String]) -> bool {
    let cell_tokens = tokenizer.tokenize(raw);
    match mode {
        MatchMode::AllTerms => {
            let cell_terms: HashSet<&str> = cell_tokens.iter().map(String::as_str).collect();
            query_tokens
                .iter()
                .all(|term| cell_terms.contains(term.as_str()))
        }
        MatchMode::AnyTerm => {
            let cell_terms: HashSet<&str> = cell_tokens.iter().map(String::as_str).collect();
            query_tokens
                .iter()
                .any(|term| cell_terms.contains(term.as_str()))
        }
        MatchMode::Phrase => phrase_matches(&cell_tokens, query_tokens),
        MatchMode::Prefix => cell_tokens
            .iter()
            .any(|cell| query_tokens.iter().any(|prefix| cell.starts_with(prefix))),
    }
}

fn phrase_matches(cell_tokens: &[String], query_tokens: &[String]) -> bool {
    if query_tokens.len() > cell_tokens.len() {
        return false;
    }
    cell_tokens.windows(query_tokens.len()).any(|window| {
        window
            .iter()
            .map(String::as_str)
            .eq(query_tokens.iter().map(String::as_str))
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct KvExtractUdf {
    signature: Signature,
}

impl KvExtractUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for KvExtractUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        KV_EXTRACT_FN
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        // Two arg slots: raw column (vector or scalar), key (vector or scalar).
        // The common shape is `kv_extract(col, 'literal')` — vector raw + scalar
        // key — special-cased for one regex compile per batch.
        let raw_arg = &args.args[0];
        let key_arg = &args.args[1];

        match (raw_arg, key_arg) {
            // Hot path: column of raw + literal key.
            (
                ColumnarValue::Array(raw_arr),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(key))),
            )
            | (
                ColumnarValue::Array(raw_arr),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(key))),
            ) => {
                let re = compile_kv_regex(key)?;
                let raw_strs = raw_arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "kv_extract: raw must be Utf8".into(),
                        )
                    })?;
                let mut builder = StringBuilder::with_capacity(raw_strs.len(), raw_strs.len() * 8);
                for i in 0..raw_strs.len() {
                    if raw_strs.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    match extract_first(&re, raw_strs.value(i)) {
                        Some(v) => builder.append_value(&v),
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            // Slow path: scalar raw + key, or vector key. Materialize into arrays
            // and dispatch row-by-row.
            (raw, key) => {
                let n = args.number_rows;
                let raw_arr = match raw {
                    ColumnarValue::Array(a) => a.clone(),
                    ColumnarValue::Scalar(s) => s.to_array_of_size(n)?,
                };
                let key_arr = match key {
                    ColumnarValue::Array(a) => a.clone(),
                    ColumnarValue::Scalar(s) => s.to_array_of_size(n)?,
                };
                let raw_strs = raw_arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "kv_extract: raw must be Utf8".into(),
                        )
                    })?;
                let key_strs = key_arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "kv_extract: key must be Utf8".into(),
                        )
                    })?;
                let mut builder = StringBuilder::with_capacity(n, n * 8);
                for i in 0..n {
                    if raw_strs.is_null(i) || key_strs.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    let re = compile_kv_regex(key_strs.value(i))?;
                    match extract_first(&re, raw_strs.value(i)) {
                        Some(v) => builder.append_value(&v),
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }
    }
}

/// Build the `<key>=<value>` regex for a user-supplied key. Quoted form
/// preferred over bare form so a `"path=/api"`-like value isn't truncated.
fn compile_kv_regex(key: &str) -> DfResult<Regex> {
    let escaped = regex::escape(key);
    let pattern = format!(r#"(?:^|[^A-Za-z0-9_]){escaped}=(?:"([^"]*)"|([^\s,]+))"#);
    Regex::new(&pattern).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "kv_extract: regex compile failed: {e}"
        ))
    })
}

fn extract_first(re: &Regex, raw: &str) -> Option<String> {
    let captures = re.captures(raw)?;
    captures
        .get(1)
        .or_else(|| captures.get(2))
        .map(|m| m.as_str().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::BooleanArray;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::scalar::ScalarValue;
    use datafusion::logical_expr::ScalarFunctionArgs;

    fn invoke(raw: &[&str], key: &str) -> Vec<Option<String>> {
        let udf = KvExtractUdf::new();
        let raw_arr: Arc<dyn Array> = Arc::new(StringArray::from(raw.to_vec()));
        let key_val = ScalarValue::Utf8(Some(key.to_string()));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(raw_arr.clone()),
                ColumnarValue::Scalar(key_val),
            ],
            arg_fields: vec![
                Arc::new(Field::new("raw", DataType::Utf8, false)),
                Arc::new(Field::new("key", DataType::Utf8, false)),
            ],
            number_rows: raw.len(),
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(raw.len()).unwrap(),
        };
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        (0..raw.len())
            .map(|i| {
                if strs.is_null(i) {
                    None
                } else {
                    Some(strs.value(i).to_string())
                }
            })
            .collect()
    }

    #[test]
    fn bare_value() {
        assert_eq!(
            invoke(&["status=500 latency=23ms"], "status"),
            vec![Some("500".into())]
        );
    }

    #[test]
    fn quoted_value_with_spaces() {
        assert_eq!(
            invoke(&["msg=\"hello world\" status=200"], "msg"),
            vec![Some("hello world".into())]
        );
    }

    #[test]
    fn missing_key_returns_null() {
        assert_eq!(invoke(&["status=500"], "user"), vec![None]);
    }

    #[test]
    fn handles_multiple_rows() {
        let out = invoke(
            &["status=200", "no key here", "user=alice status=500", ""],
            "status",
        );
        assert_eq!(
            out,
            vec![Some("200".into()), None, Some("500".into()), None]
        );
    }

    #[test]
    fn substring_of_other_key_is_not_a_match() {
        assert_eq!(invoke(&["userstatus=foo"], "status"), vec![None]);
    }

    fn invoke_attr_get(attrs: &[&str], key: &str) -> Vec<Option<String>> {
        let udf = AttrGetUdf::new();
        let arr: Arc<dyn Array> = Arc::new(StringArray::from(attrs.to_vec()));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(arr),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(key.to_string()))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("attributes", DataType::Utf8, true)),
                Arc::new(Field::new("key", DataType::Utf8, false)),
            ],
            number_rows: attrs.len(),
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(attrs.len()).unwrap(),
        };
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        (0..attrs.len())
            .map(|i| (!strs.is_null(i)).then(|| strs.value(i).to_string()))
            .collect()
    }

    #[test]
    fn attr_get_string_number_bool() {
        let row = r#"{"k8s.namespace":"prod","http.status_code":500,"trace.sampled":true}"#;
        assert_eq!(
            invoke_attr_get(&[row], "k8s.namespace"),
            vec![Some("prod".into())]
        );
        // Number + bool stringified so CAST / equality work in SQL.
        assert_eq!(
            invoke_attr_get(&[row], "http.status_code"),
            vec![Some("500".into())]
        );
        assert_eq!(
            invoke_attr_get(&[row], "trace.sampled"),
            vec![Some("true".into())]
        );
    }

    #[test]
    fn attr_get_missing_key_and_nested_and_bad_input() {
        let row = r#"{"a":"x","nested":{"b":1},"arr":[1,2]}"#;
        assert_eq!(invoke_attr_get(&[row], "absent"), vec![None]);
        // Nested object/array → their JSON.
        assert_eq!(
            invoke_attr_get(&[row], "nested"),
            vec![Some(r#"{"b":1}"#.into())]
        );
        assert_eq!(invoke_attr_get(&[row], "arr"), vec![Some("[1,2]".into())]);
        // Unparseable / non-object → NULL, no panic.
        assert_eq!(invoke_attr_get(&["not json"], "a"), vec![None]);
    }

    #[test]
    fn attr_get_null_input_and_json_null() {
        // NULL column value → NULL; explicit JSON null → NULL (absent semantics).
        assert_eq!(invoke_attr_get(&["{\"a\":null}"], "a"), vec![None]);
    }

    /// Reference: the previous full-parse implementation. The fast scanner must
    /// match it exactly across types, nesting, escaping, whitespace, and
    /// malformed input.
    fn reference_lookup(attributes: &str, key: &str) -> Option<String> {
        let map: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(attributes).ok()?;
        match map.get(key)? {
            serde_json::Value::Null => None,
            serde_json::Value::String(s) => Some(s.clone()),
            v => Some(v.to_string()),
        }
    }

    #[test]
    fn attr_get_scanner_matches_full_parse() {
        let cases: &[(&str, &[&str])] = &[
            (
                r#"{"a":"x","b":500,"c":true,"d":1.5,"e":null}"#,
                &["a", "b", "c", "d", "e", "missing"],
            ),
            (
                r#"{"nested":{"x":1,"y":[2,3]},"arr":[1,{"z":2}],"after":9}"#,
                &["nested", "arr", "after"],
            ),
            (r#"{ "spaced" : "v" , "n" : 2 }"#, &["spaced", "n"]),
            // escaped key + a value string containing structural chars.
            (
                r#"{"esc\"key":"v","k":"has,comma}brace]quote\"x","next":1}"#,
                &["esc\"key", "k", "next"],
            ),
            // negatives / malformed.
            ("not json", &["a"]),
            ("[1,2]", &["a"]),
            ("{}", &["a"]),
            ("{\"a\":}", &["a"]),
        ];
        for (json, keys) in cases {
            for key in *keys {
                assert_eq!(
                    json_attr_lookup(json, key),
                    reference_lookup(json, key),
                    "mismatch json={json} key={key}"
                );
            }
        }
    }

    /// Manual perf check (ignored by default): the scanner vs the full parse on
    /// a wide blob where the queried key is late. Prints the speedup.
    #[test]
    #[ignore]
    fn attr_get_scanner_speedup() {
        let mut json = String::from("{");
        for i in 0..30 {
            if i > 0 {
                json.push(',');
            }
            json.push_str(&format!(
                r#""attr.key.{i}":"value-number-{i}-with-some-length""#
            ));
        }
        json.push_str(r#","http.status_code":500}"#);
        let key = "http.status_code";
        const N: u32 = 200_000;
        let t0 = std::time::Instant::now();
        for _ in 0..N {
            std::hint::black_box(reference_lookup(std::hint::black_box(&json), key));
        }
        let full = t0.elapsed();
        let t1 = std::time::Instant::now();
        for _ in 0..N {
            std::hint::black_box(json_attr_lookup(std::hint::black_box(&json), key));
        }
        let fast = t1.elapsed();
        println!(
            "attr_get: full-parse {full:?}, scanner {fast:?}, speedup {:.2}x",
            full.as_secs_f64() / fast.as_secs_f64()
        );
    }

    /// End-to-end: the UDF resolves + executes through the SQL planner once
    /// `register_udfs` has run (the path the query server uses).
    #[tokio::test]
    async fn attr_get_callable_from_sql() {
        let ctx = SessionContext::new();
        register_udfs(&ctx);
        let df = ctx
            .sql(r#"SELECT attr_get('{"k8s.namespace":"prod","code":500}', 'code') AS v"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "500");
    }

    fn invoke_match(
        name: &'static str,
        mode: MatchMode,
        raw: Vec<Option<&str>>,
        query: &str,
        tokenizer: Option<&str>,
    ) -> DfResult<Vec<bool>> {
        let n = raw.len();
        let udf = MatchUdf::new(name, mode);
        let raw_arr: Arc<dyn Array> = Arc::new(StringArray::from(raw));
        let mut raw_field = Field::new("raw", DataType::Utf8, true);
        if let Some(tokenizer) = tokenizer {
            raw_field = raw_field.with_metadata(std::collections::HashMap::from([(
                siglake_storage::TEXT_TOKENIZER_FIELD_METADATA_KEY.to_string(),
                tokenizer.to_string(),
            )]));
        }
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(raw_arr),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(query.to_string()))),
            ],
            arg_fields: vec![
                Arc::new(raw_field),
                Arc::new(Field::new("query", DataType::Utf8, false)),
            ],
            number_rows: n,
            return_field: Arc::new(Field::new("out", DataType::Boolean, false)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let out = udf.invoke_with_args(args)?;
        let arr = match out {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(n)?,
        };
        let bools = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        Ok((0..bools.len()).map(|i| bools.value(i)).collect())
    }

    #[test]
    fn match_terms_requires_every_token() {
        let got = invoke_match(
            MATCH_TERMS_FN,
            MatchMode::AllTerms,
            vec![
                Some("error timeout retry"),
                Some("error only"),
                Some("timeout only"),
                None,
            ],
            "error timeout",
            None,
        )
        .unwrap();
        assert_eq!(got, vec![true, false, false, false]);
    }

    #[test]
    fn match_any_matches_any_token() {
        let got = invoke_match(
            MATCH_ANY_FN,
            MatchMode::AnyTerm,
            vec![
                Some("error timeout retry"),
                Some("timeout only"),
                Some("healthy"),
                None,
            ],
            "error timeout",
            None,
        )
        .unwrap();
        assert_eq!(got, vec![true, true, false, false]);
    }

    #[test]
    fn match_phrase_requires_adjacent_ordered_tokens() {
        let got = invoke_match(
            MATCH_PHRASE_FN,
            MatchMode::Phrase,
            vec![
                Some("database connection refused by peer"),
                Some("database connection temporarily refused"),
                Some("refused connection database"),
                None,
            ],
            "connection refused",
            None,
        )
        .unwrap();
        assert_eq!(got, vec![true, false, false, false]);
    }

    #[test]
    fn match_prefix_checks_token_starts_with() {
        let got = invoke_match(
            MATCH_PREFIX_FN,
            MatchMode::Prefix,
            vec![
                Some("connection refused"),
                Some("reconnection loop"),
                Some("healthy"),
                None,
            ],
            "conn",
            None,
        )
        .unwrap();
        assert_eq!(got, vec![true, false, false, false]);
    }

    #[test]
    fn empty_match_query_errors() {
        let err = invoke_match(
            MATCH_TERMS_FN,
            MatchMode::AllTerms,
            vec![Some("error timeout"), Some("healthy"), None, Some("error")],
            "",
            None,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("must contain at least one indexed token"));
    }

    #[test]
    fn tokenizer_parity_uses_canonical_bloom_tokenizer() {
        let input = "GET /api/v1 Connection Refused status=500";
        assert_eq!(
            tokenize_query_literal(MATCH_TERMS_FN, Tokenizer::Default, input).unwrap(),
            siglake_bloom::tokenize(input)
        );
    }

    #[test]
    fn match_terms_honors_field_tokenizer_metadata() {
        let got = invoke_match(
            MATCH_TERMS_FN,
            MatchMode::AllTerms,
            vec![Some("timeouts connecting"), Some("timeout only")],
            "timeout",
            Some("stem"),
        )
        .unwrap();
        assert_eq!(got, vec![true, true]);
    }

    #[tokio::test]
    async fn match_udfs_execute_through_sql() {
        let ctx = SessionContext::new();
        register_udfs(&ctx);
        let df = ctx
            .sql(
                "SELECT \
                    match_terms('error timeout retry', 'error timeout') AS all_hit, \
                    match_any('healthy timeout', 'error timeout') AS any_hit, \
                    match_phrase('database connection refused', 'connection refused') AS phrase_hit, \
                    match_prefix('connection refused', 'conn') AS prefix_hit",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let bools = |idx| {
            batches[0]
                .column(idx)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        };
        assert!(bools(0));
        assert!(bools(1));
        assert!(bools(2));
        assert!(bools(3));
    }

    #[tokio::test]
    async fn match_alias_executes_through_sql() {
        let ctx = SessionContext::new();
        register_udfs(&ctx);
        let df = ctx
            .sql("SELECT match('error timeout retry', 'error timeout') AS ok")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(
            col.value(0),
            "expected `match(...)` alias to parse and execute"
        );
    }
}
