//! The exact text lookup shared by the attr_get UDF and explicit raw promotions.

/// Look up `key` in a JSON-object string and return its value as text:
/// string → as-is, number/bool → their literal, array/object → compact JSON,
/// JSON null / missing key / unparseable ⇒ `None`.
///
/// Scans the top-level object for the key and parses **only** the matched value
/// — it never builds the full object map or parses the other values, so an
/// `attr_get(...)` over a wide `attributes` blob is much cheaper than a full
/// `serde_json` parse per row.
pub fn json_attr_lookup(attributes: &str, key: &str) -> Option<String> {
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

/// A borrowed dotted key prepared once for repeated attribute lookups.
///
/// The first object level is scanned once for literal-key precedence and the
/// first path head. Nested lookup shares the traversal needed to find the
/// head value boundary, rather than scanning that object a second time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedDottedJsonAttrKey<'a> {
    key: &'a str,
    head: &'a str,
    tail: &'a str,
}

impl<'a> PreparedDottedJsonAttrKey<'a> {
    /// Prepare a dotted key without allocating. A flat key returns `None`.
    pub fn new(key: &'a str) -> Option<Self> {
        let (head, tail) = key.split_once('.')?;
        Some(Self { key, head, tail })
    }

    /// Look up this prepared dotted key while preserving legacy results.
    pub fn lookup(&self, attributes: &str) -> Option<String> {
        // A literal dotted projection can follow a large nested envelope. In
        // that case speculative nested lookup is wasted: the literal wins.
        // This SIMD substring check is only a performance hint. False hits
        // (including keys inside string values) use the original lookup, and
        // escaped literal keys missed by the hint still use the full scanner.
        if memchr::memmem::find(attributes.as_bytes(), self.key.as_bytes()).is_some() {
            return json_attr_lookup(attributes, self.key);
        }
        let raw = match find_dotted_top_level_value(attributes, self.key, self.head, self.tail) {
            DottedTopLevelValue::Literal(value) => value,
            DottedTopLevelValue::Head(value) => value?,
        };
        match serde_json::from_str::<serde_json::Value>(raw).ok()? {
            serde_json::Value::Null => None,
            serde_json::Value::String(value) => Some(value),
            value => Some(value.to_string()),
        }
    }
}

enum DottedTopLevelValue<'a> {
    Literal(&'a str),
    Head(Option<&'a str>),
}

fn find_dotted_top_level_value<'a>(
    json: &'a str,
    literal: &str,
    head: &str,
    tail: &str,
) -> DottedTopLevelValue<'a> {
    let b = json.as_bytes();
    let mut i = skip_ws(b, 0);
    if !matches!(b.get(i), Some(b'{')) {
        return DottedTopLevelValue::Head(None);
    }
    i += 1;
    let mut first_head = None;
    loop {
        i = skip_ws(b, i);
        match b.get(i) {
            Some(b'"') => {}
            _ => return DottedTopLevelValue::Head(first_head.flatten()),
        }
        let key_quote = i;
        let Some((k_start, k_end, after_key)) = scan_string(b, i) else {
            return DottedTopLevelValue::Head(first_head.flatten());
        };
        i = skip_ws(b, after_key);
        if !matches!(b.get(i), Some(b':')) {
            return DottedTopLevelValue::Head(first_head.flatten());
        }
        i = skip_ws(b, i + 1);
        let v_start = i;
        let raw_key = &json[k_start..k_end];
        let decoded;
        let member_key = if raw_key.as_bytes().contains(&b'\\') {
            decoded = serde_json::from_str::<String>(&json[key_quote..after_key]).ok();
            decoded.as_deref()
        } else {
            Some(raw_key)
        };
        if member_key == Some(literal) {
            return match scan_value_end(b, i) {
                Some(end) => DottedTopLevelValue::Literal(&json[v_start..end]),
                None => DottedTopLevelValue::Head(first_head.flatten()),
            };
        }
        let v_end = if first_head.is_none() && member_key == Some(head) {
            let Some((end, value)) = scan_value_with_lookup(json, i, tail) else {
                return DottedTopLevelValue::Head(None);
            };
            // Remember a first head even when its nested lookup is missing.
            first_head = Some(value);
            end
        } else {
            let Some(end) = scan_value_end(b, i) else {
                return DottedTopLevelValue::Head(first_head.flatten());
            };
            end
        };

        i = skip_ws(b, v_end);
        match b.get(i) {
            Some(b',') => i += 1,
            _ => return DottedTopLevelValue::Head(first_head.flatten()),
        }
    }
}

/// Find a value's boundary and a lookup inside it in one traversal. Unlike
/// the public lookup, the caller needs the complete enclosing boundary before
/// accepting a head. On malformed object grammar we stop looking up keys but
/// keep the legacy structural scan, which deliberately is not a JSON validator.
fn scan_value_with_lookup<'a>(
    json: &'a str,
    start: usize,
    key: &str,
) -> Option<(usize, Option<&'a str>)> {
    let b = json.as_bytes();
    if b.get(start) != Some(&b'{') {
        return scan_value_end(b, start).map(|end| (end, None));
    }
    let path = key.split_once('.');
    let mut first_head = None;
    let mut i = start + 1;
    loop {
        i = skip_ws(b, i);
        if b.get(i) != Some(&b'"') {
            return scan_container_end(b, i, 1).map(|end| (end, first_head.flatten()));
        }
        let key_quote = i;
        let (k_start, k_end, after_key) = scan_string(b, i)?;
        i = skip_ws(b, after_key);
        if b.get(i) != Some(&b':') {
            return scan_container_end(b, i, 1).map(|end| (end, first_head.flatten()));
        }
        i = skip_ws(b, i + 1);
        let raw_key = &json[k_start..k_end];
        let decoded;
        let member_key = if raw_key.as_bytes().contains(&b'\\') {
            decoded = serde_json::from_str::<String>(&json[key_quote..after_key]).ok();
            decoded.as_deref()
        } else {
            Some(raw_key)
        };
        if member_key == Some(key) {
            let Some(end) = scan_value_end(b, i) else {
                return scan_container_end(b, i, 1).map(|end| (end, first_head.flatten()));
            };
            return scan_container_end(b, end, 1).map(|outer_end| (outer_end, Some(&json[i..end])));
        }
        let end = if let Some((_, tail)) =
            path.filter(|(head, _)| first_head.is_none() && member_key == Some(*head))
        {
            let Some((end, value)) = scan_value_with_lookup(json, i, tail) else {
                return scan_container_end(b, i, 1).map(|end| (end, first_head.flatten()));
            };
            first_head = Some(value);
            end
        } else {
            let Some(end) = scan_value_end(b, i) else {
                return scan_container_end(b, i, 1).map(|end| (end, first_head.flatten()));
            };
            end
        };
        i = skip_ws(b, end);
        if b.get(i) == Some(&b',') {
            i += 1;
        } else {
            return scan_container_end(b, i, 1).map(|end| (end, first_head.flatten()));
        }
    }
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
        // JSON strings often dominate the bytes skipped during an attribute
        // lookup. Search ordinary bytes in bulk, preserving the original
        // one-byte escape rule (including malformed/truncated strings).
        j += memchr::memchr2(b'"', b'\\', &b[j..])?;
        if b[j] == b'"' {
            return Some((start, j, j + 1));
        }
        j += 2; // skip the escape and exactly one following byte
    }
    None
}

/// Index just past the JSON value starting at `b[i]` (string/number/bool/null/
/// array/object), honoring nested structures + strings. `None` if malformed.
fn scan_value_end(b: &[u8], i: usize) -> Option<usize> {
    match b.get(i)? {
        b'"' => scan_string(b, i).map(|(_, _, after)| after),
        b'{' | b'[' => scan_container_end(b, i, 0),
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

/// Continue the permissive balanced-delimiter scan at an existing depth.
fn scan_container_end(b: &[u8], mut j: usize, mut depth: usize) -> Option<usize> {
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

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{json_attr_lookup, PreparedDottedJsonAttrKey};

    // Frozen pre-fusion implementation: its scanners and path traversal are
    // independent, including deliberately permissive malformed-input behavior.
    mod reference {
        pub fn json_attr_lookup(attributes: &str, key: &str) -> Option<String> {
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
                // JSON strings often dominate the bytes skipped during an attribute
                // lookup. Search ordinary bytes in bulk, preserving the original
                // one-byte escape rule (including malformed/truncated strings).
                j += memchr::memchr2(b'"', b'\\', &b[j..])?;
                if b[j] == b'"' {
                    return Some((start, j, j + 1));
                }
                j += 2; // skip the escape and exactly one following byte
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

    fn assert_prepared_parity(json: &str, key: &str) {
        let prepared = PreparedDottedJsonAttrKey::new(key).expect("test key must be dotted");
        assert_eq!(
            prepared.lookup(json),
            reference::json_attr_lookup(json, key),
            "json={json:?} key={key:?}"
        );
    }

    #[test]
    fn prepared_dotted_lookup_preserves_legacy_counterexamples() {
        let cases = [
            (r#"{"a":{"b":"nested"},"a.b":"literal"}"#, "a.b"),
            (r#"{"a":{"b":"first"},"a":{"b":"second"}}"#, "a.b"),
            (r#"{"a":{"b":"nested"},"a.b":null}"#, "a.b"),
            (r#"{"a":{"b":"nested"},"a.b":tru}"#, "a.b"),
            (r#"{"a":{"b":"nested"},"broken":"#, "a.b"),
            (r#"{"broken":,"a":{"b":"nested"}}"#, "a.b"),
            (r#"{"a":{"b":"nested"},"a.b":"unterminated}"#, "a.b"),
            (r#"{"a":{"b":"nested"},"a\u002eb":"literal"}"#, "a.b"),
            (r#"{"\u0061":{"b":"nested"}}"#, "a.b"),
            (r#"{"":{"a":{"":{"b":"deep"}}}}"#, ".a..b"),
            (r#"{"雪":{"λ":"unicode"}}"#, "雪.λ"),
            (r#"{"bad\u":"x","a":{"b":"nested"}}"#, "a.b"),
            (r#"{"a":{"":{"b":"empty"}}}"#, "a..b"),
            (
                r#"{"a":{"b.c":"inner-literal","b":{"c":"nested"}}}"#,
                "a.b.c",
            ),
        ];
        for (json, key) in cases {
            assert_prepared_parity(json, key);
        }

        let prepared = PreparedDottedJsonAttrKey::new("a.b").unwrap();
        assert_eq!(prepared.lookup(r#"{"a":{"b":"nested"},"a.b":tru}"#), None);
        assert_eq!(
            prepared.lookup(r#"{"a":{"b":"nested"},"a.b":"unterminated}"#),
            Some("nested".to_owned())
        );
    }

    #[test]
    fn literal_hint_preserves_false_hits_and_escaped_literal_precedence() {
        for json in [
            r#"{"note":"a.b","a":{"b":9007199254741791}}"#,
            r#"{"note":"a.b","a":{},"a":{"b":"later"}}"#,
            r#"{"a":{"b":"nested"},"a.b":{"exact":9007199254741791}}"#,
            r#"{"a":{"b":"nested"},"a\u002eb":"escaped"}"#,
            r#"{"note":"a.b","a":{"b":"nested"},"a\u002eb":null}"#,
            r#"{"note":"a.b","a":{"b":},"a\u002eb":"escaped"}"#,
        ] {
            assert_prepared_parity(json, "a.b");
        }
    }

    #[test]
    fn fused_lookup_preserves_nested_duplicates_numbers_and_malformed_boundaries() {
        let cases = [
            r#"{"a":{"b":{},"b":{"c":4}}}"#,
            r#"{"a":{"b":,"b.c":5},"a.b.c":7}"#,
            r#"{"a":{"b":{"c":4},"b.c":null}}"#,
            r#"{"a":{"b":{"c":4},"b.c":}}"#,
            r#"{"a":{"b":{"c":4},"b.c":"unterminated}}"#,
            r#"{"a":{"b":{"c":4},"b.c":tru}}"#,
            r#"{"a":{"b":{"c":4},"broken":},"a.b.c":8}"#,
            r#"{"a":{"b":{"c":4}]}}"#,
            r#"{"a":{"b":{"c":4},"b.c":5},"a.b.c":6}"#,
            r#"{"a":{"b":{"c":4},"b.c":5},"a.b.c":}"#,
            r#"{"a":{"b":{"c":4},"\u0062.c":5}}"#,
            r#"{"a":{"b":{"c":9223372036854775807}}}"#,
            r#"{"a":{"b":{"c":18446744073709551615}}}"#,
            r#"{"a":{"b":{"c":-9223372036854775808}}}"#,
            r#"{"a":{"b":{"c":9007199254741791}}}"#,
            r#"{"a":{"b":{"c":-0.0}}}"#,
            r#"{"a":{"b":{"c":1.2345678901234567}}}"#,
        ];
        for json in cases {
            for key in ["a.b.c", "a.b", "a.missing", "missing.b.c"] {
                assert_prepared_parity(json, key);
                for end in 0..json.len() {
                    if json.is_char_boundary(end) {
                        assert_prepared_parity(&json[..end], key);
                    }
                }
                // Single-byte structural damage also covers mismatched closing
                // delimiters and malformed tails after an already found value.
                for (at, ch) in json.char_indices() {
                    if "{}[],:\\\"".contains(ch) {
                        for replacement in ["", "}", "]", ",", ":", "\\", "\""] {
                            let damaged = format!(
                                "{}{}{}",
                                &json[..at],
                                replacement,
                                &json[at + ch.len_utf8()..]
                            );
                            assert_prepared_parity(&damaged, key);
                        }
                    }
                }
            }
        }
        assert_eq!(
            PreparedDottedJsonAttrKey::new("a.b.c")
                .unwrap()
                .lookup(r#"{"a":{"b":{"c":9007199254741791}}}"#),
            Some("9007199254741791".into())
        );
    }

    // Independent copy of the pre-optimization scanner: comparing two public
    // lookup paths would miss a shared scanner regression.
    fn scalar_scan_string(b: &[u8], i: usize) -> Option<(usize, usize, usize)> {
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

    #[test]
    fn string_scan_preserves_escape_and_truncation_edges() {
        for bytes in [
            b"\"\"".as_slice(),
            b"\"unterminated",
            b"\"trailing\\",
            b"\"escaped\\\"quote\"tail",
            b"\"even\\\\\"tail",
            b"\"odd\\\\\\\"tail",
            "\"雪λ\\u0022\"tail".as_bytes(),
        ] {
            assert_eq!(super::scan_string(bytes, 0), scalar_scan_string(bytes, 0));
        }
        let mut long = vec![b'x'; 8192];
        long[0] = b'"';
        long[4095] = b'\\';
        long[4096] = b'"'; // escaped across a typical vector/page boundary
        long[8191] = b'"';
        assert_eq!(super::scan_string(&long, 0), Some((1, 8191, 8192)));
    }

    fn json_value_strategy() -> impl Strategy<Value = serde_json::Value> {
        let leaf = prop_oneof![
            Just(serde_json::Value::Null),
            any::<bool>().prop_map(serde_json::Value::Bool),
            any::<i64>().prop_map(|value| serde_json::Value::Number(value.into())),
            any::<String>().prop_map(serde_json::Value::String),
        ];
        leaf.prop_recursive(4, 64, 8, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..8).prop_map(serde_json::Value::Array),
                proptest::collection::btree_map(any::<String>(), inner, 0..8)
                    .prop_map(|map| { serde_json::Value::Object(map.into_iter().collect()) }),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]


        #[test]
        fn fused_lookup_matches_reference_for_present_nested_paths(
            value in json_value_strategy(),
            sibling in json_value_strategy(),
            key in "[a-z]{0,8}",
            literal in any::<bool>(),
            duplicate in any::<bool>(),
            damage in 0usize..4096,
        ) {
            let leaf = serde_json::to_string(&value).unwrap();
            let sibling = serde_json::to_string(&sibling).unwrap();
            let literal_field = if literal { format!(",\"b.{key}\":{sibling}") } else { String::new() };
            let duplicate_field = if duplicate { format!(",\"b\":{{\"{key}\":{sibling}}}") } else { String::new() };
            let json = format!("{{\"before\":{sibling},\"a\":{{\"before\":{sibling},\"b\":{{\"{key}\":{leaf},\"after\":{sibling}}}{duplicate_field}{literal_field}}},\"after\":{sibling}}}");
            let path = format!("a.b.{key}");
            assert_prepared_parity(&json, &path);
            let at = damage % json.len();
            if json.is_char_boundary(at) {
                assert_prepared_parity(&json[..at], &path);
                let damaged = format!("{}]{}", &json[..at], &json[at..]);
                assert_prepared_parity(&damaged, &path);
            }
        }

        #[test]
        fn string_scan_matches_scalar_for_arbitrary_bytes(
            prefix in proptest::collection::vec(any::<u8>(), 0..64),
            suffix in proptest::collection::vec(any::<u8>(), 0..4096),
        ) {
            let start = prefix.len();
            let mut bytes = prefix;
            bytes.push(b'"');
            bytes.extend(suffix);
            prop_assert_eq!(super::scan_string(&bytes, start), scalar_scan_string(&bytes, start));
        }

        #[test]
        fn prepared_dotted_lookup_matches_legacy_for_arbitrary_utf8(
            json in any::<String>(),
            head in any::<String>(),
            tail in any::<String>(),
        ) {
            let key = format!("{head}.{tail}");
            let prepared = PreparedDottedJsonAttrKey::new(&key).unwrap();
            prop_assert_eq!(prepared.lookup(&json), json_attr_lookup(&json, &key));
        }

        #[test]
        fn prepared_dotted_lookup_matches_legacy_for_valid_json_objects(
            object in proptest::collection::btree_map(any::<String>(), json_value_strategy(), 0..16),
            head in any::<String>(),
            tail in any::<String>(),
        ) {
            let json = serde_json::to_string(&object).unwrap();
            let key = format!("{head}.{tail}");
            let prepared = PreparedDottedJsonAttrKey::new(&key).unwrap();
            prop_assert_eq!(prepared.lookup(&json), json_attr_lookup(&json, &key));
        }
    }
}
