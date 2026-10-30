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
    let Some((head, tail)) = key.split_once('.') else {
        return find_top_level_value(json, key);
    };
    match find_dotted_top_level_value(json, key, head) {
        DottedTopLevelValue::Literal(value) => Some(value),
        DottedTopLevelValue::Head(inner) => find_value_path(inner?, tail),
    }
}

enum DottedTopLevelValue<'a> {
    Literal(&'a str),
    Head(Option<&'a str>),
}

/// Scan once for a dotted literal while retaining the first path-head value.
/// A scannable literal wins even when its value later fails JSON decoding. If
/// scanning stops first, the retained head reproduces the legacy second scan.
fn find_dotted_top_level_value<'a>(
    json: &'a str,
    literal: &str,
    head: &str,
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
            _ => return DottedTopLevelValue::Head(first_head),
        }
        let key_quote = i;
        let Some((k_start, k_end, after_key)) = scan_string(b, i) else {
            return DottedTopLevelValue::Head(first_head);
        };
        i = skip_ws(b, after_key);
        if !matches!(b.get(i), Some(b':')) {
            return DottedTopLevelValue::Head(first_head);
        }
        i = skip_ws(b, i + 1);
        let v_start = i;
        let Some(v_end) = scan_value_end(b, i) else {
            return DottedTopLevelValue::Head(first_head);
        };

        let raw_key = &json[k_start..k_end];
        if raw_key.as_bytes().contains(&b'\\') {
            let decoded = serde_json::from_str::<String>(&json[key_quote..after_key]).ok();
            if decoded.as_deref() == Some(literal) {
                return DottedTopLevelValue::Literal(&json[v_start..v_end]);
            }
            if first_head.is_none() && decoded.as_deref() == Some(head) {
                first_head = Some(&json[v_start..v_end]);
            }
        } else {
            if raw_key == literal {
                return DottedTopLevelValue::Literal(&json[v_start..v_end]);
            }
            if first_head.is_none() && raw_key == head {
                first_head = Some(&json[v_start..v_end]);
            }
        }

        i = skip_ws(b, v_end);
        match b.get(i) {
            Some(b',') => i += 1,
            _ => return DottedTopLevelValue::Head(first_head),
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

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::json_attr_lookup;

    // Frozen byte-for-byte from main 1c69e5c. This remains independent of the
    // candidate scanner so malformed-input behavior has an exact oracle.
    mod legacy {
        pub fn json_attr_lookup(attributes: &str, key: &str) -> Option<String> {
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

    fn assert_legacy_parity(json: &str, key: &str) {
        assert_eq!(
            json_attr_lookup(json, key),
            legacy::json_attr_lookup(json, key),
            "json={json:?} key={key:?}"
        );
    }

    #[test]
    fn dotted_lookup_preserves_legacy_counterexamples() {
        let cases = [
            (r#"{"a":{"b":"nested"},"a.b":"literal"}"#, "a.b"),
            (r#"{"a":{"b":"first"},"a":{"b":"second"}}"#, "a.b"),
            (r#"{"a":{"b":"nested"},"a.b":null}"#, "a.b"),
            (r#"{"a":{"b":"nested"},"a.b":tru}"#, "a.b"),
            (r#"{"a":{"b":"nested"},"broken":"#, "a.b"),
            (r#"{"broken":,"a":{"b":"nested"}}"#, "a.b"),
            (r#"{"a":{"b":"nested"},"a.b":tru}"#, "a.b"),
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
            assert_legacy_parity(json, key);
        }

        assert_eq!(
            json_attr_lookup(r#"{"a":{"b":"nested"},"a.b":tru}"#, "a.b"),
            None
        );
        assert_eq!(
            json_attr_lookup(r#"{"a":{"b":"nested"},"a.b":"unterminated}"#, "a.b"),
            Some("nested".to_owned())
        );
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
        fn dotted_lookup_matches_legacy_for_arbitrary_utf8(json in any::<String>(), key in any::<String>()) {
            prop_assert_eq!(json_attr_lookup(&json, &key), legacy::json_attr_lookup(&json, &key));
        }

        #[test]
        fn dotted_lookup_matches_legacy_for_valid_json_objects(
            object in proptest::collection::btree_map(any::<String>(), json_value_strategy(), 0..16),
            key in any::<String>(),
        ) {
            let json = serde_json::to_string(&object).unwrap();
            prop_assert_eq!(json_attr_lookup(&json, &key), legacy::json_attr_lookup(&json, &key));
        }
    }
}
