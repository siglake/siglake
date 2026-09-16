//! `siglake sql` — the interactive SQL client for a running query server.
//!
//! One-shot (`siglake sql "SELECT …"`) or a REPL (no query argument), over
//! `POST /api/v1/sql`. Table output surfaces what the API already returns —
//! the per-query scan stats and server time — so the zero-scan fast paths
//! are VISIBLE, and `--dry-run` prints the cost estimate without executing.

use std::io::Write as _;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use serde_json::Value;

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq)]
pub enum SqlOutput {
    /// Aligned columns + a stats footer (default).
    Table,
    /// The raw response JSON, pretty-printed.
    Json,
    /// One JSON object per row (streamed from the server).
    Ndjson,
}

pub struct SqlClientOpts {
    pub endpoint: String,
    pub token: Option<String>,
    pub format: SqlOutput,
    pub dry_run: bool,
    pub quiet: bool,
}

pub async fn run(opts: &SqlClientOpts, query: Option<String>) -> Result<()> {
    let client = reqwest::Client::builder()
        .build()
        .context("build http client")?;
    match query {
        Some(q) => run_one(&client, opts, &q).await,
        None => repl(&client, opts).await,
    }
}

async fn repl(client: &reqwest::Client, opts: &SqlClientOpts) -> Result<()> {
    eprintln!(
        "siglake sql — connected to {} (\\d lists indexes, \\q quits)",
        opts.endpoint
    );
    let mut rl = rustyline::DefaultEditor::new().context("init line editor")?;
    let history = dirs_history_path();
    if let Some(h) = &history {
        let _ = rl.load_history(h);
    }
    loop {
        match rl.readline("siglake> ") {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(line);
                match line {
                    "\\q" | "exit" | "quit" => break,
                    "\\d" => {
                        if let Err(e) = list_indexes(client, opts).await {
                            eprintln!("error: {e:#}");
                        }
                    }
                    q => {
                        let q = q.trim_end_matches(';');
                        if let Err(e) = run_one(client, opts, q).await {
                            eprintln!("error: {e:#}");
                        }
                    }
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => continue,
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(e) => return Err(e).context("read line"),
        }
    }
    if let Some(h) = &history {
        let _ = rl.save_history(h);
    }
    Ok(())
}

fn dirs_history_path() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".siglake_history"))
}

fn authed(req: reqwest::RequestBuilder, opts: &SqlClientOpts) -> reqwest::RequestBuilder {
    match &opts.token {
        Some(t) => req.bearer_auth(t),
        None => req,
    }
}

async fn list_indexes(client: &reqwest::Client, opts: &SqlClientOpts) -> Result<()> {
    let url = format!("{}/api/v1/indexes", opts.endpoint.trim_end_matches('/'));
    let resp = authed(client.get(&url), opts).send().await?;
    let status = resp.status();
    let body: Value = resp.json().await.context("parse indexes response")?;
    if !status.is_success() {
        bail!("{status}: {body}");
    }
    // Accept either a bare array or `{ indexes: [...] }`.
    let list = body
        .get("indexes")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| body.as_array().cloned())
        .unwrap_or_default();
    println!("events");
    for entry in list {
        let id = entry
            .get("index_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| entry.as_str().map(str::to_string));
        if let Some(id) = id {
            if id != "events" {
                println!("{id}");
            }
        }
    }
    Ok(())
}

async fn run_one(client: &reqwest::Client, opts: &SqlClientOpts, query: &str) -> Result<()> {
    let url = format!("{}/api/v1/sql", opts.endpoint.trim_end_matches('/'));
    let mut body = serde_json::json!({ "query": query });
    if opts.dry_run {
        body["dry_run"] = Value::Bool(true);
    }
    if opts.format == SqlOutput::Ndjson && !opts.dry_run {
        body["format"] = Value::String("ndjson".into());
    }
    let started = Instant::now();
    let resp = authed(client.post(&url), opts)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = resp.status();
    let server_micros: Option<u64> = resp
        .headers()
        .get("x-siglake-server-micros")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());

    if opts.format == SqlOutput::Ndjson && !opts.dry_run && status.is_success() {
        // Stream rows straight through.
        let mut stream = resp;
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        while let Some(chunk) = stream.chunk().await? {
            out.write_all(&chunk)?;
        }
        return Ok(());
    }

    let text = resp.text().await.context("read response body")?;
    let payload: Value = serde_json::from_str(&text)
        .with_context(|| format!("non-JSON response ({status}): {}", truncate(&text, 400)))?;
    if !status.is_success() {
        bail!("{status}: {}", serde_json::to_string_pretty(&payload)?);
    }
    let wall_ms = started.elapsed().as_secs_f64() * 1e3;

    match opts.format {
        SqlOutput::Json => {
            println!("{}", serde_json::to_string_pretty(&payload)?);
        }
        SqlOutput::Table | SqlOutput::Ndjson => {
            if opts.dry_run {
                render_cost(&payload);
            } else {
                render_table(&payload);
                if !opts.quiet {
                    render_footer(&payload, wall_ms, server_micros);
                }
            }
        }
    }
    Ok(())
}

fn truncate(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

/// Render a value for a table cell: bare strings unquoted, everything else
/// compact JSON, NULL as empty.
fn cell(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

const MAX_CELL: usize = 96;

fn clip(s: &str) -> String {
    if s.chars().count() <= MAX_CELL {
        return s.to_string();
    }
    let clipped: String = s.chars().take(MAX_CELL - 1).collect();
    format!("{clipped}…")
}

pub fn render_table_to(payload: &Value, out: &mut impl std::io::Write) -> std::io::Result<()> {
    let columns: Vec<String> = payload
        .get("columns")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let empty = Vec::new();
    let rows = payload
        .get("rows")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    if columns.is_empty() {
        return writeln!(out, "(no columns)");
    }
    let rendered: Vec<Vec<String>> = rows
        .iter()
        .map(|r| {
            columns
                .iter()
                .map(|c| clip(&cell(r.get(c).unwrap_or(&Value::Null))))
                .collect()
        })
        .collect();
    let mut widths: Vec<usize> = columns.iter().map(|c| c.chars().count()).collect();
    for row in &rendered {
        for (i, v) in row.iter().enumerate() {
            widths[i] = widths[i].max(v.chars().count());
        }
    }
    let line = |out: &mut dyn std::io::Write, cells: &[String]| -> std::io::Result<()> {
        let mut parts = Vec::with_capacity(cells.len());
        for (i, v) in cells.iter().enumerate() {
            parts.push(format!("{v:<width$}", width = widths[i]));
        }
        writeln!(out, "{}", parts.join(" | ").trim_end())
    };
    line(out, &columns)?;
    let rule: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    writeln!(out, "{}", rule.join("-+-"))?;
    for row in &rendered {
        line(out, row)?;
    }
    Ok(())
}

fn render_table(payload: &Value) {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let _ = render_table_to(payload, &mut out);
}

fn render_footer(payload: &Value, wall_ms: f64, server_micros: Option<u64>) {
    let row_count = payload
        .get("row_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let truncated = payload
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut parts = vec![format!(
        "{row_count} row{}{}",
        if row_count == 1 { "" } else { "s" },
        if truncated { " (truncated)" } else { "" }
    )];
    if let Some(stats) = payload.get("stats") {
        if let Some(scanned) = stats.get("rows_scanned").and_then(Value::as_u64) {
            parts.push(if scanned == 0 {
                "0 rows scanned (fast path)".to_string()
            } else {
                format!("{scanned} rows scanned")
            });
        }
    }
    if let Some(us) = server_micros {
        parts.push(format!("server {:.1} ms", us as f64 / 1e3));
    }
    parts.push(format!("wall {wall_ms:.1} ms"));
    eprintln!("({})", parts.join(" · "));
}

fn render_cost(payload: &Value) {
    let cost = payload.get("cost").unwrap_or(payload);
    println!(
        "{}",
        serde_json::to_string_pretty(cost).unwrap_or_else(|_| cost.to_string())
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_renders_aligned_columns_and_nulls() {
        let payload = serde_json::json!({
            "columns": ["host", "n"],
            "row_count": 2,
            "rows": [
                {"host": "web-1", "n": 42},
                {"host": null, "n": 7},
            ],
        });
        let mut buf = Vec::new();
        render_table_to(&payload, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s, "host  | n\n------+---\nweb-1 | 42\n      | 7\n");
    }

    #[test]
    fn long_cells_clip_with_ellipsis() {
        let long = "x".repeat(200);
        let payload = serde_json::json!({
            "columns": ["raw"],
            "rows": [{"raw": long}],
        });
        let mut buf = Vec::new();
        render_table_to(&payload, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        let data_line = s.lines().nth(2).unwrap();
        assert_eq!(data_line.chars().count(), MAX_CELL);
        assert!(data_line.ends_with('…'));
    }
}
