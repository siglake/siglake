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
