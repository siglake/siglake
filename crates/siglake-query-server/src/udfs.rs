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
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
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
use siglake_bloom::{default_token_slices, Tokenizer};

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

use siglake_core::json_attr_lookup;

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
        // Literal keys are constant for the batch. Borrow the Utf8 value once
        // instead of allocating and repeating it in an Arrow key array.
        if let ColumnarValue::Scalar(ScalarValue::Utf8(key)) = &args.args[1] {
            let key = key.as_deref();
            let n = args.number_rows;
            let attrs = arg_as_str_array(&args.args[0], n, "attributes")?;
            let mut builder = StringBuilder::with_capacity(n, n * 16);
            for i in 0..n {
                if attrs.is_null(i) || key.is_none() {
                    builder.append_null();
                    continue;
                }
                match json_attr_lookup(attrs.value(i), key.unwrap()) {
                    Some(value) => builder.append_value(&value),
                    None => builder.append_null(),
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }
        // Array and non-Utf8 scalar keys retain the existing validation path.
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
                let matcher = PreparedMatch::new(self.mode, tokenizer, &query_tokens);
                let mut builder = BooleanBuilder::with_capacity(n);
                for i in 0..n {
                    if cells.is_null(i) {
                        builder.append_value(false);
                        continue;
                    }
                    builder.append_value(matcher.matches(cells.value(i)));
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
                    let matcher = PreparedMatch::new(self.mode, tokenizer, &query_tokens);
                    builder.append_value(matcher.matches(cells.value(i)));
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

#[derive(Clone, Copy)]
struct AsciiLower<'a>(&'a str);

impl PartialEq for AsciiLower<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(other.0)
    }
}

impl Eq for AsciiLower<'_> {}

impl Hash for AsciiLower<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
    }
}

enum DefaultTermMatch<'a> {
    One(&'a str),
    Many {
        masks: HashMap<AsciiLower<'a>, u64>,
        required: u64,
    },
}

impl<'a> DefaultTermMatch<'a> {
    fn new(query_tokens: &'a [String]) -> Self {
        let first = query_tokens
            .first()
            .expect("match query tokenization is validated before preparation");
        if query_tokens.iter().all(|term| term == first) {
            return Self::One(first);
        }

        let mut masks = HashMap::with_capacity(query_tokens.len());
        for term in query_tokens {
            let next_mask = 1_u64 << masks.len();
            masks.entry(AsciiLower(term)).or_insert(next_mask);
        }
        let required = if masks.len() == u64::BITS as usize {
            u64::MAX
        } else {
            (1_u64 << masks.len()) - 1
        };
        Self::Many { masks, required }
    }

    fn any(&self, raw: &str) -> bool {
        match self {
            Self::One(term) => {
                default_token_slices(raw).any(|cell| cell.eq_ignore_ascii_case(term))
            }
            Self::Many { masks, .. } => {
                default_token_slices(raw).any(|cell| masks.contains_key(&AsciiLower(cell)))
            }
        }
    }

    fn all(&self, raw: &str) -> bool {
        match self {
            Self::One(term) => {
                default_token_slices(raw).any(|cell| cell.eq_ignore_ascii_case(term))
            }
            Self::Many { masks, required } => {
                let mut found = 0_u64;
                for cell in default_token_slices(raw) {
                    if let Some(mask) = masks.get(&AsciiLower(cell)) {
                        found |= mask;
                        if found == *required {
                            return true;
                        }
                    }
                }
                false
            }
        }
    }
}

struct PreparedMatch<'a> {
    mode: MatchMode,
    tokenizer: Tokenizer,
    query_tokens: &'a [String],
    default_terms: Option<DefaultTermMatch<'a>>,
}

impl<'a> PreparedMatch<'a> {
    fn new(mode: MatchMode, tokenizer: Tokenizer, query_tokens: &'a [String]) -> Self {
        let default_terms = (tokenizer == Tokenizer::Default
            && matches!(mode, MatchMode::AllTerms | MatchMode::AnyTerm))
        .then(|| DefaultTermMatch::new(query_tokens));
        Self {
            mode,
            tokenizer,
            query_tokens,
            default_terms,
        }
    }

    fn matches(&self, raw: &str) -> bool {
        match (&self.default_terms, self.mode) {
            (Some(terms), MatchMode::AllTerms) => terms.all(raw),
            (Some(terms), MatchMode::AnyTerm) => terms.any(raw),
            _ => match_raw_fallback(self.mode, self.tokenizer, raw, self.query_tokens),
        }
    }
}

fn match_raw_fallback(
    mode: MatchMode,
    tokenizer: Tokenizer,
    raw: &str,
    query_tokens: &[String],
) -> bool {
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
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::time::Duration;

    use arrow_array::BooleanArray;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::scalar::ScalarValue;
    use datafusion::logical_expr::ScalarFunctionArgs;
    use proptest::prelude::*;

    struct CountingAllocator;

    thread_local! {
        static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
        static ALLOCATION_COUNT: Cell<usize> = const { Cell::new(0) };
    }

    fn record_allocation(ptr: *mut u8) {
        if !ptr.is_null() && TRACK_ALLOCATIONS.get() {
            ALLOCATION_COUNT.set(ALLOCATION_COUNT.get() + 1);
        }
    }

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            record_allocation(ptr);
            ptr
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc_zeroed(layout) };
            record_allocation(ptr);
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let ptr = unsafe { System.realloc(ptr, layout, new_size) };
            record_allocation(ptr);
            ptr
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    fn allocations<T>(f: impl FnOnce() -> T) -> (T, usize) {
        ALLOCATION_COUNT.set(0);
        TRACK_ALLOCATIONS.set(true);
        let value = f();
        TRACK_ALLOCATIONS.set(false);
        let count = ALLOCATION_COUNT.get();
        (value, count)
    }

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

    /// Promote `key` out of `attributes` into a Utf8 column and read the cell
    /// back — the value a query sees once the `attr_get`→column rewrite fires.
    fn promoted_cell(attributes: &str, key: &str) -> Option<String> {
        use arrow_array::Array;
        let batch = siglake_core::events_to_record_batch(&[
            siglake_core::Event::now("raw").with_attributes(Some(attributes.to_string()))
        ])
        .unwrap();
        let col = siglake_core::PromotedColumn {
            attr_key: key.to_string(),
            carrier: Default::default(),
            attr_get_path: Vec::new(),
            name: "promoted".into(),
            ty: siglake_core::PromotedType::Utf8,
        };
        let out = siglake_core::promote_attributes(&batch, std::slice::from_ref(&col)).unwrap();
        let values = out
            .column_by_name("promoted")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (!values.is_null(0)).then(|| values.value(0).to_string())
    }

    /// #6769: `attr_get` and WS-7 promotion must resolve a key to the same
    /// value for every input — the query layer substitutes the promoted column
    /// for the UDF and prunes row groups from its statistics, so a shape where
    /// they disagree silently changes answers. Promotion's strict segment walk
    /// used to miss an inner literal dotted key (`{"a":{"b.c":1}}`) that the
    /// UDF's per-level literal retry resolves.
    #[test]
    fn attr_get_matches_promoted_extraction() {
        let cases: &[(&str, &[&str])] = &[
            // The divergent shape, and the nested shape it must not break.
            (r#"{"a":{"b.c":1}}"#, &["a.b.c", "a", "a.b"]),
            (r#"{"a":{"b":{"c":1}}}"#, &["a.b.c", "a.b", "a"]),
            // Literal-dotted precedence at both levels (the Big5
            // `log.file.path` key depends on the top-level half).
            (
                r#"{"log.file.path":"/lit","log":{"file":{"path":"/nested"}}}"#,
                &["log.file.path", "log.file", "log"],
            ),
            (
                r#"{"a":{"b.c":"inner-lit","b":{"c":"inner-nested"}}}"#,
                &["a.b.c", "a.b"],
            ),
            // No backtracking in either extractor: `a` resolves and the rest
            // fails inside it; top-level `a.b` is not retried.
            (r#"{"a":{"x":1},"a.b":{"c":9}}"#, &["a.b.c", "a.b", "a.b.x"]),
            // Nulls, scalars mid-path, absent keys, non-scalar leaves.
            (
                r#"{"a":{"b.c":null,"s":"str"},"n":5}"#,
                &["a.b.c", "a.s.x", "n.x", "absent.key", "a"],
            ),
            (
                r#"{"k8s":{"pod":"p-1","labels":{"app.kubernetes.io/name":"siglake"}}}"#,
                &["k8s.pod", "k8s.labels.app.kubernetes.io/name", "k8s.labels"],
            ),
            // Unparseable / non-object attributes.
            ("not json", &["a.b"]),
            ("[1,2]", &["a.b"]),
        ];
        for (json, keys) in cases {
            for key in *keys {
                assert_eq!(
                    json_attr_lookup(json, key),
                    promoted_cell(json, key),
                    "attr_get and promotion disagree: json={json} key={key}"
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

    fn attr_scalar_key_args(
        attrs: ColumnarValue,
        key: ColumnarValue,
        number_rows: usize,
    ) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args: vec![attrs, key],
            arg_fields: vec![
                Arc::new(Field::new("attributes", DataType::Utf8, true)),
                Arc::new(Field::new("key", DataType::Utf8, true)),
            ],
            number_rows,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        }
    }

    // Frozen from the owned helper at main 1c69e5c. Keeping the scanner and
    // decoder here makes the timing baseline independent of production.
    mod attr_owned_baseline {
        pub(super) fn json_attr_lookup(attributes: &str, key: &str) -> Option<String> {
            let raw = find_value_path(attributes, key)?;
            match serde_json::from_str::<serde_json::Value>(raw).ok()? {
                serde_json::Value::Null => None,
                serde_json::Value::String(s) => Some(s),
                v => Some(v.to_string()),
            }
        }

        fn find_value_path<'a>(json: &'a str, key: &str) -> Option<&'a str> {
            if let Some(v) = find_top_level_value(json, key) {
                return Some(v);
            }
            let (head, tail) = key.split_once('.')?;
            let inner = find_top_level_value(json, head)?;
            find_value_path(inner, tail)
        }

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
                    b'}' => return None,
                    b'"' => {}
                    _ => return None,
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
                    _ => return None,
                }
            }
        }

        fn skip_ws(b: &[u8], mut i: usize) -> usize {
            while matches!(b.get(i), Some(b' ' | b'\t' | b'\n' | b'\r')) {
                i += 1;
            }
            i
        }

        fn scan_string(b: &[u8], i: usize) -> Option<(usize, usize, usize)> {
            let start = i + 1;
            let mut j = start;
            while j < b.len() {
                match b[j] {
                    b'\\' => j += 2,
                    b'"' => return Some((start, j, j + 1)),
                    _ => j += 1,
                }
            }
            None
        }

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
                    let mut j = i;
                    while j < b.len()
                        && !matches!(b[j], b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
                    {
                        j += 1;
                    }
                    (j > i).then_some(j)
                }
            }
        }
    }

    // Frozen owned-result production UDF for semantic and timing parity.
    // Keep this independent: baseline timing must not call the candidate UDF.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct AttrGetOwnedBaselineUdf {
        signature: Signature,
    }

    impl AttrGetOwnedBaselineUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Utf8, DataType::Utf8],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for AttrGetOwnedBaselineUdf {
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
            if let ColumnarValue::Scalar(ScalarValue::Utf8(key)) = &args.args[1] {
                let key = key.as_deref();
                let n = args.number_rows;
                let attrs = arg_as_str_array(&args.args[0], n, "attributes")?;
                let mut builder = StringBuilder::with_capacity(n, n * 16);
                for i in 0..n {
                    if attrs.is_null(i) || key.is_none() {
                        builder.append_null();
                        continue;
                    }
                    match attr_owned_baseline::json_attr_lookup(attrs.value(i), key.unwrap()) {
                        Some(value) => builder.append_value(&value),
                        None => builder.append_null(),
                    }
                }
                return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
            }
            let n = args.number_rows;
            let attrs = arg_as_str_array(&args.args[0], n, "attributes")?;
            let keys = arg_as_str_array(&args.args[1], n, "key")?;
            let mut builder = StringBuilder::with_capacity(n, n * 16);
            for i in 0..n {
                if attrs.is_null(i) || keys.is_null(i) {
                    builder.append_null();
                    continue;
                }
                match attr_owned_baseline::json_attr_lookup(attrs.value(i), keys.value(i)) {
                    Some(value) => builder.append_value(&value),
                    None => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
    }

    fn attr_scalar_key_baseline(args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        AttrGetOwnedBaselineUdf::new().invoke_with_args(args)
    }

    fn attr_scalar_key_candidate(args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        AttrGetUdf::new().invoke_with_args(args)
    }

    fn attr_scalar_key_values(value: ColumnarValue, n: usize) -> Vec<Option<String>> {
        let array = attr_scalar_key_array(value, n);
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        (0..n)
            .map(|i| (!array.is_null(i)).then(|| array.value(i).to_owned()))
            .collect()
    }

    fn attr_scalar_key_array(value: ColumnarValue, n: usize) -> Arc<dyn Array> {
        match value {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(value) => value.to_array_of_size(n).unwrap(),
        }
    }

    fn assert_attr_outputs_equal(baseline: ColumnarValue, candidate: ColumnarValue, n: usize) {
        let baseline = attr_scalar_key_array(baseline, n);
        let candidate = attr_scalar_key_array(candidate, n);
        assert_eq!(baseline.data_type(), &DataType::Utf8);
        assert_eq!(candidate.data_type(), &DataType::Utf8);
        assert_eq!(candidate.to_data(), baseline.to_data());
    }

    fn invoke_attr_scalar_key(
        candidate: bool,
        attrs: ColumnarValue,
        key: ColumnarValue,
        number_rows: usize,
    ) -> DfResult<ColumnarValue> {
        let args = attr_scalar_key_args(attrs, key, number_rows);
        if candidate {
            attr_scalar_key_candidate(args)
        } else {
            attr_scalar_key_baseline(args)
        }
    }

    #[test]
    fn attr_scalar_key_candidate_matches_semantic_golds() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("testdata/attr_get_semantic_cases.json")).unwrap();
        assert_eq!(cases["cases"].as_array().unwrap().len(), 19);
        for case in cases["cases"].as_array().unwrap() {
            let raw = case["attributes"].as_str().unwrap();
            let key = case["key"].as_str().unwrap();
            let expected = case["expected"].as_str().map(str::to_owned);
            for n in [0, 1, 128, 4096] {
                let rows: Vec<Option<&str>> = (0..n)
                    .map(|i| if i % 7 == 6 { None } else { Some(raw) })
                    .collect();
                let attrs: Arc<dyn Array> = Arc::new(StringArray::from(rows));
                let dynamic_keys: Arc<dyn Array> = Arc::new(StringArray::from(
                    (0..n)
                        .map(|i| (i % 11 != 10).then_some(key))
                        .collect::<Vec<_>>(),
                ));
                for key_arg in [
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(key.to_owned()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                    ColumnarValue::Array(dynamic_keys),
                ] {
                    let scalar_null =
                        matches!(&key_arg, ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    let dynamic = matches!(&key_arg, ColumnarValue::Array(_));
                    let baseline = invoke_attr_scalar_key(
                        false,
                        ColumnarValue::Array(attrs.clone()),
                        key_arg.clone(),
                        n,
                    )
                    .unwrap();
                    let candidate = invoke_attr_scalar_key(
                        true,
                        ColumnarValue::Array(attrs.clone()),
                        key_arg,
                        n,
                    )
                    .unwrap();
                    assert_attr_outputs_equal(baseline.clone(), candidate, n);
                    for (i, actual) in attr_scalar_key_values(baseline, n).iter().enumerate() {
                        let gold = if scalar_null || i % 7 == 6 || (dynamic && i % 11 == 10) {
                            None
                        } else {
                            expected.clone()
                        };
                        assert_eq!(actual, &gold, "{} row={i}", case["id"]);
                    }
                }
            }
        }
    }

    #[test]
    fn attr_get_candidate_matches_owned_baseline_for_edge_arrays() {
        let long = format!(r#"{{"target":"{}"}}"#, "x".repeat(8192));
        let control = "{\"target\":\"a\u{1}b\"}".to_owned();
        let cases = vec![
            (Some(r#"{"target":"plain"}"#.to_owned()), Some("target")),
            (
                Some(r#"{"target":"line\nquote\"slash\\"}"#.to_owned()),
                Some("target"),
            ),
            (
                Some(r#"{"target":"λ雪🦀значение"}"#.to_owned()),
                Some("target"),
            ),
            (Some(long), Some("target")),
            (Some(control), Some("target")),
            (
                Some(r#"{"target":"first","target":"second"}"#.to_owned()),
                Some("target"),
            ),
            (
                Some(r#"{"a":{"b":"nested"},"a.b":"literal"}"#.to_owned()),
                Some("a.b"),
            ),
            (
                Some(r#"{"target":9007199254740993}"#.to_owned()),
                Some("target"),
            ),
            (Some(r#"{"target":true}"#.to_owned()), Some("target")),
            (
                Some(r#"{"target":[1,"x",null]}"#.to_owned()),
                Some("target"),
            ),
            (
                Some(r#"{"target":{"a":1,"b":"x"}}"#.to_owned()),
                Some("target"),
            ),
            (Some(r#"{"target":null}"#.to_owned()), Some("target")),
            (Some(r#"{"other":"missing"}"#.to_owned()), Some("target")),
            (Some(r#"{"target":}"#.to_owned()), Some("target")),
            (
                Some(r#"{"target":"unterminated"#.to_owned()),
                Some("target"),
            ),
            (
                Some(r#"{"target":"ok","broken":}"#.to_owned()),
                Some("target"),
            ),
            (
                Some(r#"{"target":"ok","broken":"#.to_owned()),
                Some("target"),
            ),
            (
                Some(r#"{"broken":,"target":"late"}"#.to_owned()),
                Some("target"),
            ),
            (None, Some("target")),
            (Some(r#"{"target":"null-key"}"#.to_owned()), None),
        ];
        let (attributes, keys): (Vec<_>, Vec<_>) = cases.into_iter().unzip();
        let number_rows = attributes.len();
        let attributes: Arc<dyn Array> = Arc::new(StringArray::from(attributes));
        let keys: Arc<dyn Array> = Arc::new(StringArray::from(keys));
        let baseline = invoke_attr_scalar_key(
            false,
            ColumnarValue::Array(attributes.clone()),
            ColumnarValue::Array(keys.clone()),
            number_rows,
        )
        .unwrap();
        let candidate = invoke_attr_scalar_key(
            true,
            ColumnarValue::Array(attributes),
            ColumnarValue::Array(keys),
            number_rows,
        )
        .unwrap();
        assert_attr_outputs_equal(baseline, candidate, number_rows);
    }

    #[test]
    fn attr_scalar_key_candidate_accepts_scalar_attributes() {
        for n in [0, 1, 128, 4096] {
            let attrs = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                r#"{"a.b":"literal","a":{"b":"nested"}}"#.to_owned(),
            )));
            let key = ColumnarValue::Scalar(ScalarValue::Utf8(Some("a.b".to_owned())));
            let baseline = attr_scalar_key_values(
                invoke_attr_scalar_key(false, attrs.clone(), key.clone(), n).unwrap(),
                n,
            );
            let candidate =
                attr_scalar_key_values(invoke_attr_scalar_key(true, attrs, key, n).unwrap(), n);
            assert_eq!(candidate, baseline);
            assert_eq!(candidate, vec![Some("literal".to_owned()); n]);

            let attrs = ColumnarValue::Scalar(ScalarValue::Utf8(None));
            let key = ColumnarValue::Scalar(ScalarValue::Utf8(Some("a.b".to_owned())));
            let baseline = attr_scalar_key_values(
                invoke_attr_scalar_key(false, attrs.clone(), key.clone(), n).unwrap(),
                n,
            );
            let candidate =
                attr_scalar_key_values(invoke_attr_scalar_key(true, attrs, key, n).unwrap(), n);
            assert_eq!(candidate, baseline);
            assert_eq!(candidate, vec![None; n]);
        }
    }

    #[test]
    fn attr_scalar_key_candidate_preserves_wrong_type_refusals() {
        let utf8_attrs: Arc<dyn Array> = Arc::new(StringArray::from(vec![r#"{"x":1}"#]));
        let int_attrs: Arc<dyn Array> = Arc::new(arrow_array::Int64Array::from(vec![1]));
        let int_keys: Arc<dyn Array> = Arc::new(arrow_array::Int64Array::from(vec![1]));
        let cases = [
            (
                ColumnarValue::Array(int_attrs),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".to_owned()))),
                "attributes must be Utf8",
            ),
            (
                ColumnarValue::Array(utf8_attrs.clone()),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
                "key must be Utf8",
            ),
            (
                ColumnarValue::Array(utf8_attrs),
                ColumnarValue::Array(int_keys),
                "key must be Utf8",
            ),
        ];
        for (attrs, key, expected) in cases {
            let baseline = invoke_attr_scalar_key(false, attrs.clone(), key.clone(), 1)
                .unwrap_err()
                .to_string();
            let candidate = invoke_attr_scalar_key(true, attrs, key, 1)
                .unwrap_err()
                .to_string();
            assert_eq!(candidate, baseline);
            assert!(
                candidate.contains(expected),
                "unexpected error: {candidate}"
            );
        }
    }

    proptest! {
        #[test]
        fn attr_get_candidate_matches_owned_baseline_for_arbitrary_scalar_inputs(
            attributes in proptest::option::of(any::<String>()),
            key in proptest::option::of(any::<String>()),
            number_rows in 0usize..24,
        ) {
            let attrs = ColumnarValue::Scalar(ScalarValue::Utf8(attributes));
            let key = ColumnarValue::Scalar(ScalarValue::Utf8(key));
            let baseline = invoke_attr_scalar_key(
                false,
                attrs.clone(),
                key.clone(),
                number_rows,
            ).unwrap();
            let candidate = invoke_attr_scalar_key(true, attrs, key, number_rows).unwrap();
            assert_attr_outputs_equal(baseline, candidate, number_rows);
        }

        #[test]
        fn attr_get_candidate_matches_owned_baseline_for_arbitrary_array_inputs(
            rows in proptest::collection::vec(
                (proptest::option::of(any::<String>()), proptest::option::of(any::<String>())),
                0..24,
            ),
        ) {
            let (attributes, keys): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
            let number_rows = attributes.len();
            let attrs: Arc<dyn Array> = Arc::new(StringArray::from(attributes));
            let keys: Arc<dyn Array> = Arc::new(StringArray::from(keys));
            let baseline = invoke_attr_scalar_key(
                false,
                ColumnarValue::Array(attrs.clone()),
                ColumnarValue::Array(keys.clone()),
                number_rows,
            ).unwrap();
            let candidate = invoke_attr_scalar_key(
                true,
                ColumnarValue::Array(attrs),
                ColumnarValue::Array(keys),
                number_rows,
            ).unwrap();
            assert_attr_outputs_equal(baseline, candidate, number_rows);
        }
    }

    #[test]
    fn attr_get_output_owns_buffers_after_inputs_drop() {
        let output = {
            let attributes: Arc<dyn Array> = Arc::new(StringArray::from(vec![
                Some(r#"{"target":"borrowed-source"}"#),
                Some(r#"{"target":"escaped\nsource"}"#),
                None,
            ]));
            let keys: Arc<dyn Array> = Arc::new(StringArray::from(vec![
                Some("target"),
                Some("target"),
                Some("target"),
            ]));
            attr_scalar_key_array(
                invoke_attr_scalar_key(
                    true,
                    ColumnarValue::Array(attributes),
                    ColumnarValue::Array(keys),
                    3,
                )
                .unwrap(),
                3,
            )
        };
        let output = output.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "borrowed-source");
        assert_eq!(output.value(1), "escaped\nsource");
        assert!(output.is_null(2));
    }

    struct AttrBenchmarkCondition {
        name: &'static str,
        attributes: String,
        key: &'static str,
    }

    fn attr_benchmark_field_object(fields: usize) -> String {
        let mut json = String::from("{");
        for i in 0..fields {
            if i > 0 {
                json.push(',');
            }
            json.push_str(&format!(r#""field_{i}":"value-{i}""#));
        }
        json.push('}');
        json
    }

    fn attr_benchmark_conditions() -> Vec<AttrBenchmarkCondition> {
        let mut conditions = Vec::new();
        for (size, fields) in [("tiny", 3), ("fields32", 32), ("fields128", 128)] {
            let attributes = attr_benchmark_field_object(fields);
            for (position, key) in [
                ("first", "field_0"),
                (
                    "middle",
                    if fields == 3 {
                        "field_1"
                    } else if fields == 32 {
                        "field_16"
                    } else {
                        "field_64"
                    },
                ),
                (
                    "last",
                    if fields == 3 {
                        "field_2"
                    } else if fields == 32 {
                        "field_31"
                    } else {
                        "field_127"
                    },
                ),
                ("missing", "absent"),
            ] {
                let name = match (size, position) {
                    ("tiny", "first") => "tiny_first",
                    ("tiny", "middle") => "tiny_middle",
                    ("tiny", "last") => "tiny_last",
                    ("tiny", "missing") => "tiny_missing",
                    ("fields32", "first") => "fields32_first",
                    ("fields32", "middle") => "fields32_middle",
                    ("fields32", "last") => "fields32_last",
                    ("fields32", "missing") => "fields32_missing",
                    ("fields128", "first") => "fields128_first",
                    ("fields128", "middle") => "fields128_middle",
                    ("fields128", "last") => "fields128_last",
                    ("fields128", "missing") => "fields128_missing",
                    _ => unreachable!(),
                };
                conditions.push(AttrBenchmarkCondition {
                    name,
                    attributes: attributes.clone(),
                    key,
                });
            }
        }

        let long_ascii = "x".repeat(4096);
        let escaped = format!(r#"prefix {} suffix"#, r#"quote:\" slash:\\ "#.repeat(256));
        conditions.extend([
            AttrBenchmarkCondition {
                name: "long_ascii_string",
                attributes: format!(r#"{{"target":"{long_ascii}"}}"#),
                key: "target",
            },
            AttrBenchmarkCondition {
                name: "escaped_string",
                attributes: format!(r#"{{"target":"{escaped}"}}"#),
                key: "target",
            },
            AttrBenchmarkCondition {
                name: "unicode_string",
                attributes: format!(r#"{{"target":"{}"}}"#, "λ雪🦀".repeat(512)),
                key: "target",
            },
            AttrBenchmarkCondition {
                name: "nested_dotted_path",
                attributes: r#"{"outer":{"middle":{"target":"nested"}}}"#.to_owned(),
                key: "outer.middle.target",
            },
            AttrBenchmarkCondition {
                name: "literal_dotted_precedence",
                attributes: r#"{"outer.middle.target":"literal","outer":{"middle":{"target":"nested"}}}"#.to_owned(),
                key: "outer.middle.target",
            },
            AttrBenchmarkCondition {
                name: "short_no_match",
                attributes: r#"{"a":0}"#.to_owned(),
                key: "z",
            },
            AttrBenchmarkCondition {
                name: "short_truncated_no_match",
                attributes: r#"{"a":"#.to_owned(),
                key: "z",
            },
            AttrBenchmarkCondition {
                name: "sl_signoz_v1_envelope",
                attributes: r#"{"sl_signoz_v1":{"record_id":"01JBC8Z2W9T1H6P9","severity_number":13,"severity_text":"WARN","attributes_string":"{\"customer.segment\":\"enterprise\",\"http.route\":\"/checkout\",\"region\":\"us-east-1\",\"request.kind\":\"write\",\"customer.tier\":\"gold\",\"deployment.ring\":\"canary\"}","resources_string":"{\"service.name\":\"checkout\",\"cloud.provider\":\"aws\"}"},"otel":{"scope":{"name":"example"}},"tail":"done"}"#.to_owned(),
                key: "sl_signoz_v1.attributes_string",
            },
            AttrBenchmarkCondition {
                name: "selected_number",
                attributes: r#"{"target":9007199254740993}"#.to_owned(),
                key: "target",
            },
            AttrBenchmarkCondition {
                name: "selected_bool",
                attributes: r#"{"target":true}"#.to_owned(),
                key: "target",
            },
            AttrBenchmarkCondition {
                name: "selected_array",
                attributes: r#"{"target":[1,"x",null]}"#.to_owned(),
                key: "target",
            },
            AttrBenchmarkCondition {
                name: "selected_object",
                attributes: r#"{"target":{"a":1,"b":"x"}}"#.to_owned(),
                key: "target",
            },
            AttrBenchmarkCondition {
                name: "selected_null",
                attributes: r#"{"target":null}"#.to_owned(),
                key: "target",
            },
        ]);
        conditions
    }

    const ATTR_BENCHMARK_ROW_COUNTS: [usize; 3] = [1, 128, 4096];
    const ATTR_BENCHMARK_WARMUPS: usize = 2;
    const ATTR_BENCHMARK_MEASURED_ROUNDS: usize = 7;
    const ATTR_BENCHMARK_ROWS_PER_SAMPLE: usize = 4096;
    const ATTR_BENCHMARK_TIMED_ROWS: usize = 5_529_600;
    const ATTR_BENCHMARK_TOTAL_LOOKUP_CEILING: usize = 6_000_000;

    fn attr_benchmark_invocations(row_count: usize) -> usize {
        ATTR_BENCHMARK_ROWS_PER_SAMPLE / row_count
    }

    fn attr_benchmark_args(
        condition: &AttrBenchmarkCondition,
        row_count: usize,
        invocations: usize,
        array_key: bool,
    ) -> Vec<ScalarFunctionArgs> {
        let attributes: Arc<dyn Array> =
            Arc::new(StringArray::from(vec![
                condition.attributes.as_str();
                row_count
            ]));
        let key = if array_key {
            let keys: Arc<dyn Array> = Arc::new(StringArray::from(vec![condition.key; row_count]));
            ColumnarValue::Array(keys)
        } else {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(condition.key.to_owned())))
        };
        (0..invocations)
            .map(|_| {
                attr_scalar_key_args(
                    ColumnarValue::Array(attributes.clone()),
                    key.clone(),
                    row_count,
                )
            })
            .collect()
    }

    struct AttrBenchmarkUdfs {
        baseline: AttrGetOwnedBaselineUdf,
        candidate: AttrGetUdf,
    }

    impl AttrBenchmarkUdfs {
        fn new() -> Self {
            Self {
                baseline: AttrGetOwnedBaselineUdf::new(),
                candidate: AttrGetUdf::new(),
            }
        }

        fn callable(&self, candidate: bool) -> &dyn ScalarUDFImpl {
            if candidate {
                &self.candidate
            } else {
                &self.baseline
            }
        }
    }

    fn attr_benchmark_invoke(
        callable: &dyn ScalarUDFImpl,
        args: ScalarFunctionArgs,
    ) -> ColumnarValue {
        callable.invoke_with_args(args).unwrap()
    }

    fn attr_benchmark_checksum(value: &ColumnarValue, row_count: usize) -> u64 {
        const OFFSET: u64 = 0xcbf29ce484222325;
        const PRIME: u64 = 0x100000001b3;
        let array = match value {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(value) => value.to_array_of_size(row_count).unwrap(),
        };
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        let mut hash = OFFSET;
        let mut absorb = |byte| {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(PRIME);
        };
        for i in 0..row_count {
            if array.is_null(i) {
                absorb(0);
            } else {
                absorb(1);
                let value = array.value(i);
                for byte in u64::try_from(value.len()).unwrap().to_le_bytes() {
                    absorb(byte);
                }
                for byte in value.bytes() {
                    absorb(byte);
                }
            }
        }
        hash
    }

    fn attr_benchmark_sample(
        callable: &dyn ScalarUDFImpl,
        args: Vec<ScalarFunctionArgs>,
    ) -> Duration {
        let start = std::time::Instant::now();
        for args in args {
            let output = attr_benchmark_invoke(callable, args);
            drop(std::hint::black_box(output));
        }
        start.elapsed()
    }

    #[test]
    fn attr_benchmark_reuses_symmetric_udfs_outside_invocation_counts() {
        let (baseline_udf, baseline_constructor_allocations) =
            allocations(AttrGetOwnedBaselineUdf::new);
        let (candidate_udf, candidate_constructor_allocations) = allocations(AttrGetUdf::new);
        assert_eq!(
            candidate_constructor_allocations, baseline_constructor_allocations,
            "both test UDF constructors must have the same signature allocation cost"
        );
        assert!(candidate_constructor_allocations > 0);

        let udfs = AttrBenchmarkUdfs {
            baseline: baseline_udf,
            candidate: candidate_udf,
        };
        let condition = AttrBenchmarkCondition {
            name: "constructor_control",
            attributes: r#"{"target":"value"}"#.to_owned(),
            key: "target",
        };
        for array_key in [false, true] {
            let baseline_args = attr_benchmark_args(&condition, 3, 4, array_key);
            let candidate_args = attr_benchmark_args(&condition, 3, 4, array_key);
            let baseline_callable = udfs.callable(false);
            let candidate_callable = udfs.callable(true);
            let mut baseline_outputs = Vec::with_capacity(4);
            let mut candidate_outputs = Vec::with_capacity(4);
            let (_, baseline_allocations) = allocations(|| {
                for args in baseline_args {
                    baseline_outputs.push(attr_benchmark_invoke(baseline_callable, args));
                }
            });
            let (_, candidate_allocations) = allocations(|| {
                for args in candidate_args {
                    candidate_outputs.push(attr_benchmark_invoke(candidate_callable, args));
                }
            });
            assert_eq!(candidate_allocations, baseline_allocations);
            for (baseline, candidate) in baseline_outputs.into_iter().zip(candidate_outputs) {
                assert_attr_outputs_equal(baseline, candidate, 3);
            }
        }

        let attrs: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some(r#"{"target":"owned"}"#),
            None,
            Some(r#"{"other":"missing"}"#),
        ]));
        let null_keys: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some("target"),
            None,
            Some("target"),
        ]));
        let baseline = attr_benchmark_invoke(
            udfs.callable(false),
            attr_scalar_key_args(
                ColumnarValue::Array(attrs.clone()),
                ColumnarValue::Array(null_keys.clone()),
                3,
            ),
        );
        let candidate = attr_benchmark_invoke(
            udfs.callable(true),
            attr_scalar_key_args(
                ColumnarValue::Array(attrs),
                ColumnarValue::Array(null_keys),
                3,
            ),
        );
        assert_attr_outputs_equal(baseline, candidate.clone(), 3);
        assert_eq!(
            attr_scalar_key_values(candidate, 3),
            vec![Some("owned".to_owned()), None, None]
        );

        let int_keys: Arc<dyn Array> = Arc::new(arrow_array::Int64Array::from(vec![1]));
        let attrs: Arc<dyn Array> = Arc::new(StringArray::from(vec![r#"{"target":"value"}"#]));
        let baseline_error = udfs
            .callable(false)
            .invoke_with_args(attr_scalar_key_args(
                ColumnarValue::Array(attrs.clone()),
                ColumnarValue::Array(int_keys.clone()),
                1,
            ))
            .unwrap_err()
            .to_string();
        let candidate_error = udfs
            .callable(true)
            .invoke_with_args(attr_scalar_key_args(
                ColumnarValue::Array(attrs),
                ColumnarValue::Array(int_keys),
                1,
            ))
            .unwrap_err()
            .to_string();
        assert_eq!(candidate_error, baseline_error);
    }

    #[test]
    fn attr_get_arrow_benchmark_schedule_is_fixed_and_bounded() {
        let conditions = attr_benchmark_conditions();
        assert_eq!(conditions.len(), 25);
        let rounds = ATTR_BENCHMARK_WARMUPS + ATTR_BENCHMARK_MEASURED_ROUNDS;
        let samples = conditions.len() * ATTR_BENCHMARK_ROW_COUNTS.len() * rounds * 2;
        assert_eq!(samples, 1350);
        let timed_rows = samples * ATTR_BENCHMARK_ROWS_PER_SAMPLE;
        assert_eq!(timed_rows, ATTR_BENCHMARK_TIMED_ROWS);

        let parity_rows = conditions.len() * 2 * ATTR_BENCHMARK_ROW_COUNTS.iter().sum::<usize>();
        let scalar_allocation_rows = parity_rows;
        let array_allocation_rows = 2 * ATTR_BENCHMARK_ROW_COUNTS.iter().sum::<usize>();
        assert!(
            timed_rows + parity_rows + scalar_allocation_rows + array_allocation_rows
                <= ATTR_BENCHMARK_TOTAL_LOOKUP_CEILING
        );
        for row_count in ATTR_BENCHMARK_ROW_COUNTS {
            let invocations = attr_benchmark_invocations(row_count);
            assert_eq!(invocations * row_count, ATTR_BENCHMARK_ROWS_PER_SAMPLE);
        }
    }

    #[test]
    #[ignore = "timing requires the reviewed isolated optimized envelope"]
    fn attr_get_arrow_microbenchmark() {
        let conditions = attr_benchmark_conditions();
        let rounds = ATTR_BENCHMARK_WARMUPS + ATTR_BENCHMARK_MEASURED_ROUNDS;
        let udfs = AttrBenchmarkUdfs::new();

        for condition in &conditions {
            for row_count in ATTR_BENCHMARK_ROW_COUNTS {
                let baseline = attr_benchmark_invoke(
                    udfs.callable(false),
                    attr_benchmark_args(condition, row_count, 1, false)
                        .pop()
                        .unwrap(),
                );
                let candidate = attr_benchmark_invoke(
                    udfs.callable(true),
                    attr_benchmark_args(condition, row_count, 1, false)
                        .pop()
                        .unwrap(),
                );
                let checksum = attr_benchmark_checksum(&baseline, row_count);
                assert_eq!(attr_benchmark_checksum(&candidate, row_count), checksum);
                assert_attr_outputs_equal(baseline, candidate, row_count);

                for candidate in [false, true] {
                    let callable = udfs.callable(candidate);
                    let mut args = attr_benchmark_args(condition, row_count, 1, false);
                    let (output, count) =
                        allocations(|| attr_benchmark_invoke(callable, args.pop().unwrap()));
                    drop(std::hint::black_box(output));
                    println!(
                        "{{\"benchmark\":\"attr_get_arrow_allocations\",\"pid\":{},\"condition\":\"{}\",\"rowcount\":{},\"key_representation\":\"scalar\",\"implementation\":\"{}\",\"invocations\":1,\"rows\":{},\"allocations\":{},\"allocations_per_invocation\":{},\"allocations_per_row\":{}}}",
                        std::process::id(),
                        condition.name,
                        row_count,
                        if candidate { "candidate" } else { "baseline" },
                        row_count,
                        count,
                        count,
                        count as f64 / row_count as f64,
                    );
                }

                if condition.name == "tiny_first" {
                    for candidate in [false, true] {
                        let callable = udfs.callable(candidate);
                        let mut args = attr_benchmark_args(condition, row_count, 1, true);
                        let (output, count) =
                            allocations(|| attr_benchmark_invoke(callable, args.pop().unwrap()));
                        drop(std::hint::black_box(output));
                        println!(
                            "{{\"benchmark\":\"attr_get_arrow_allocations\",\"pid\":{},\"condition\":\"{}\",\"rowcount\":{},\"key_representation\":\"array\",\"implementation\":\"{}\",\"invocations\":1,\"rows\":{},\"allocations\":{},\"allocations_per_invocation\":{},\"allocations_per_row\":{}}}",
                            std::process::id(),
                            condition.name,
                            row_count,
                            if candidate { "candidate" } else { "baseline" },
                            row_count,
                            count,
                            count,
                            count as f64 / row_count as f64,
                        );
                    }
                }

                let invocations = attr_benchmark_invocations(row_count);
                for round in 0..rounds {
                    let (phase, ordinal) = if round < ATTR_BENCHMARK_WARMUPS {
                        ("warmup", round)
                    } else {
                        ("measured", round - ATTR_BENCHMARK_WARMUPS)
                    };
                    let implementations = if round.is_multiple_of(2) {
                        [false, true]
                    } else {
                        [true, false]
                    };
                    for (order, candidate) in implementations.into_iter().enumerate() {
                        let callable = udfs.callable(candidate);
                        let args = attr_benchmark_args(condition, row_count, invocations, false);
                        let elapsed = attr_benchmark_sample(callable, args);
                        println!(
                            "{{\"benchmark\":\"attr_get_arrow\",\"pid\":{},\"condition\":\"{}\",\"rowcount\":{},\"invocations\":{},\"rows\":{},\"phase\":\"{}\",\"ordinal\":{},\"order\":{},\"implementation\":\"{}\",\"elapsed\":{},\"elapsed_unit\":\"ns\",\"below_1ms\":{},\"checksum\":{}}}",
                            std::process::id(),
                            condition.name,
                            row_count,
                            invocations,
                            invocations * row_count,
                            phase,
                            ordinal,
                            order,
                            if candidate { "candidate" } else { "baseline" },
                            elapsed.as_nanos(),
                            elapsed < Duration::from_millis(1),
                            checksum,
                        );
                    }
                }
            }
        }
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
    fn prepared_match_is_exactly_the_previous_matcher() {
        let capped = (0..70)
            .map(|i| format!("Token{i:02}"))
            .collect::<Vec<_>>()
            .join(" / ");
        let corpus = [
            "Queen checkout completed in 17ms",
            "QUEEN,checkout; retrying",
            "short a AB 12 and café",
            "timeouts connecting classes",
            "connection refused by peer",
            "healthy",
            &capped,
        ];
        let cases = [
            (Tokenizer::Default, "queen"),
            (Tokenizer::Default, "queen queen"),
            (Tokenizer::Default, "queen checkout"),
            (Tokenizer::Default, "token63 token69"),
            (Tokenizer::Raw, "queen checkout"),
            (Tokenizer::Stem, "timeout connect"),
        ];
        let modes = [
            MatchMode::AllTerms,
            MatchMode::AnyTerm,
            MatchMode::Phrase,
            MatchMode::Prefix,
        ];

        for (tokenizer, query) in cases {
            let query_tokens = tokenizer.tokenize(query);
            for mode in modes {
                let prepared = PreparedMatch::new(mode, tokenizer, &query_tokens);
                for raw in corpus {
                    assert_eq!(
                        prepared.matches(raw),
                        match_raw_fallback(mode, tokenizer, raw, &query_tokens),
                        "mode={mode:?} tokenizer={tokenizer:?} query={query:?} raw={raw:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn prepared_match_handles_all_64_distinct_query_terms() {
        let query = (0..64)
            .map(|i| format!("term{i:02}"))
            .collect::<Vec<_>>()
            .join(" ");
        let raw = query.to_ascii_uppercase();
        let query_tokens = Tokenizer::Default.tokenize(&query);
        let prepared = PreparedMatch::new(MatchMode::AllTerms, Tokenizer::Default, &query_tokens);
        assert!(prepared.matches(&raw));
        assert_eq!(
            prepared.matches(&raw),
            match_raw_fallback(MatchMode::AllTerms, Tokenizer::Default, &raw, &query_tokens)
        );
    }

    fn match_cost_corpus() -> Vec<String> {
        (0..512)
            .map(|row| {
                let needle = if row % 5 == 0 { " queen" } else { " healthy" };
                format!(
                    "2026-10-26T12:00:00Z service=checkout request completed{needle} status=200 row={row:04}"
                )
            })
            .collect()
    }

    fn run_match_corpus(corpus: &[String], mut matches: impl FnMut(&str) -> bool) -> usize {
        corpus
            .iter()
            .filter(|raw| std::hint::black_box(matches(std::hint::black_box(raw))))
            .count()
    }

    #[test]
    fn default_term_paths_allocate_nothing_per_cell() {
        let corpus = match_cost_corpus();
        for (mode, query) in [
            (MatchMode::AllTerms, "queen"),
            (MatchMode::AllTerms, "queen checkout"),
            (MatchMode::AnyTerm, "absent queen"),
        ] {
            let query_tokens = Tokenizer::Default.tokenize(query);
            let prepared = PreparedMatch::new(mode, Tokenizer::Default, &query_tokens);

            let (optimized_hits, optimized_allocations) =
                allocations(|| run_match_corpus(&corpus, |raw| prepared.matches(raw)));
            let (reference_hits, reference_allocations) = allocations(|| {
                run_match_corpus(&corpus, |raw| {
                    match_raw_fallback(mode, Tokenizer::Default, raw, &query_tokens)
                })
            });

            assert_eq!(
                optimized_hits, reference_hits,
                "mode={mode:?} query={query}"
            );
            assert_eq!(optimized_allocations, 0, "mode={mode:?} query={query}");
            assert!(
                reference_allocations >= corpus.len(),
                "the reference must expose its per-cell allocation: {reference_allocations} allocations for {} cells; mode={mode:?} query={query}",
                corpus.len()
            );
        }
    }

    fn process_cpu() -> Duration {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let rc = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
        assert_eq!(rc, 0, "read process CPU clock");
        Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    }

    fn cpu_sample(mut f: impl FnMut() -> usize) -> (usize, Duration) {
        let start = process_cpu();
        let mut hits = 0;
        for _ in 0..200 {
            hits += std::hint::black_box(f());
        }
        (hits, process_cpu() - start)
    }

    /// Identical-corpus, single-process CPU control for the allocation rewrite.
    /// The previous matcher is retained as the exact-result and cost oracle;
    /// process CPU excludes scheduler wait, and both arms use the same wrapper.
    ///
    /// Run with:
    /// `cargo test --release -p siglake-query-server default_match_cpu_control -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn default_match_cpu_control() {
        let corpus = match_cost_corpus();
        let query_tokens = Tokenizer::Default.tokenize("queen");
        let prepared = PreparedMatch::new(MatchMode::AllTerms, Tokenizer::Default, &query_tokens);
        let optimized = || run_match_corpus(&corpus, |raw| prepared.matches(raw));
        let reference = || {
            run_match_corpus(&corpus, |raw| {
                match_raw_fallback(MatchMode::AllTerms, Tokenizer::Default, raw, &query_tokens)
            })
        };

        std::hint::black_box(optimized());
        std::hint::black_box(reference());
        let mut optimized_samples = Vec::with_capacity(7);
        let mut reference_samples = Vec::with_capacity(7);
        let mut optimized_hits = 0;
        let mut reference_hits = 0;
        for sample in 0..7 {
            if sample % 2 == 0 {
                let (hits, cpu) = cpu_sample(&reference);
                reference_hits = hits;
                reference_samples.push(cpu);
                let (hits, cpu) = cpu_sample(&optimized);
                optimized_hits = hits;
                optimized_samples.push(cpu);
            } else {
                let (hits, cpu) = cpu_sample(&optimized);
                optimized_hits = hits;
                optimized_samples.push(cpu);
                let (hits, cpu) = cpu_sample(&reference);
                reference_hits = hits;
                reference_samples.push(cpu);
            }
        }
        optimized_samples.sort_unstable();
        reference_samples.sort_unstable();
        let optimized_median = optimized_samples[optimized_samples.len() / 2];
        let reference_median = reference_samples[reference_samples.len() / 2];
        let ratio = optimized_median.as_secs_f64() / reference_median.as_secs_f64();
        eprintln!(
            "default match CPU: optimized={optimized_median:?} reference={reference_median:?} ratio={ratio:.3}"
        );

        assert_eq!(optimized_hits, reference_hits);
        assert!(
            ratio <= 0.5,
            "optimized/reference CPU ratio {ratio:.3} exceeded 0.500"
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
