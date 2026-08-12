//! Template expansion for read SQL.
//!
//! Supported tokens (used in a job's `source.where` and `source.columns`):
//!
//! * `${state.<key>}` – the stored high-water mark for `key`, rendered as a SQL
//!   literal (missing values are handled by the caller's resolver).
//! * `${env.<NAME>}`  – environment variable.
//! * `${sys.now('FORMAT') ±N<unit>}` – current local time, optional offset
//!   where `<unit>` is one of `S` (seconds), `M` (minutes), `H` (hours) or
//!   `D` (days). Examples: `${sys.now('%Y-%m-%d %H:%M:%S')}`,
//!   `${sys.now('%Y-%m-%d') -1D}`.
//!
//! Unknown tokens are left verbatim so a mistake is visible in the query.

use std::fmt::Write as _;

use chrono::Local;

use crate::error::{AppError, AppResult};

pub struct Template<'a> {
    /// Renders a state value as a ready-to-use SQL literal.
    pub resolve_state: &'a dyn Fn(&str) -> AppResult<Option<String>>,
    /// Current time, injectable for tests.
    pub now: &'a dyn Fn() -> chrono::DateTime<Local>,
}

impl Default for Template<'static> {
    fn default() -> Self {
        Template {
            resolve_state: &|_| Ok(None),
            now: &Local::now,
        }
    }
}

impl<'a> Template<'a> {
    pub fn expand(&self, input: &str) -> AppResult<String> {
        let mut out = String::with_capacity(input.len());
        let mut rest = input;
        while let Some(start) = rest.find("${") {
            out.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            let Some(end) = after.find('}') else {
                out.push_str(&rest[start..]);
                return Ok(out);
            };
            let token = &after[..end];
            let rendered = self.expand_token(token.trim())?;
            out.push_str(&rendered);
            rest = &after[end + 1..];
        }
        out.push_str(rest);
        Ok(out)
    }

    fn expand_token(&self, token: &str) -> AppResult<String> {
        if let Some(key) = token.strip_prefix("state.") {
            let v = (self.resolve_state)(key)?
                .ok_or_else(|| AppError::Template(format!("state '{key}' has no value")))?;
            return Ok(v);
        }
        if let Some(name) = token.strip_prefix("env.") {
            return std::env::var(name)
                .map_err(|_| AppError::Template(format!("environment variable '{name}' not set")));
        }
        if let Some(expr) = token.strip_prefix("sys.") {
            return self.expand_sys(expr.trim());
        }
        Err(AppError::Template(format!("unknown token '${{{token}}}'")))
    }

    fn expand_sys(&self, expr: &str) -> AppResult<String> {
        // expr := now('FORMAT') [+|-]? N<unit>
        let (call, offset) = match expr.find(')') {
            Some(idx) => (&expr[..=idx], expr[idx + 1..].trim()),
            None => (expr, ""),
        };
        let inner = call
            .trim_start_matches("now(")
            .trim_end_matches(')')
            .trim();
        let fmt = inner
            .strip_prefix('\'')
            .and_then(|s| s.strip_suffix('\''))
            .ok_or_else(|| AppError::Template(format!("bad sys.now format: '{call}'")))?;
        let chrono_fmt = strftime_to_chrono(fmt)?;

        let now = (self.now)();
        let dt = if offset.is_empty() {
            now
        } else {
            let (sign, num, unit) = parse_offset(offset)?;
            let secs: i64 = match unit {
                'S' => num,
                'M' => num * 60,
                'H' => num * 3600,
                'D' => num * 86400,
                _ => unreachable!(),
            };
            let delta = chrono::Duration::seconds(if sign == '+' { secs } else { -secs });
            now.checked_add_signed(delta).ok_or_else(|| {
                AppError::Template(format!("sys.now offset overflow: '{offset}'"))
            })?
        };
        // Render as a quoted SQL literal (like state date/string values), so
        // `create_time > ${sys.now('%Y-%m-%d %H:%M:%S') -30S}` produces
        // `create_time > '2026-08-12 02:43:54'` instead of a syntax error.
        Ok(format!(
            "'{}'",
            dt.format(&chrono_fmt).to_string().replace('\'', "''")
        ))
    }
}

/// Parse an offset like `-30S`, `+10M`, `-1H`, `2D`.
fn parse_offset(s: &str) -> AppResult<(char, i64, char)> {
    let mut chars = s.chars().peekable();
    let mut sign = '+';
    if let Some(&c) = chars.peek() {
        if c == '+' || c == '-' {
            sign = chars.next().unwrap();
        }
    }
    let mut digits = String::new();
    loop {
        match chars.peek() {
            Some(&c) if c.is_ascii_digit() => {
                digits.push(chars.next().unwrap());
            }
            _ => break,
        }
    }
    if digits.is_empty() {
        return Err(AppError::Template(format!("bad offset: '{s}'")));
    }
    let num: i64 = digits
        .parse()
        .map_err(|_| AppError::Template(format!("bad offset number: '{s}'")))?;
    // peek (do not consume) the trailing unit character
    let unit = *chars
        .peek()
        .ok_or_else(|| AppError::Template(format!("bad offset unit: '{s}'")))?;
    if !matches!(unit, 'S' | 'M' | 'H' | 'D') {
        return Err(AppError::Template(format!(
            "bad offset unit '{unit}' (expected S/M/H/D): '{s}'"
        )));
    }
    Ok((sign, num, unit))
}

/// Translate the small strftime subset we document into chrono format codes.
fn strftime_to_chrono(fmt: &str) -> AppResult<String> {
    let mut out = String::with_capacity(fmt.len());
    let mut it = fmt.chars();
    while let Some(c) = it.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(n) = it.next() else {
            return Err(AppError::Template("trailing '%' in format".into()));
        };
        match n {
            'Y' | 'y' | 'm' | 'd' | 'H' | 'M' | 'S' => {
                let _ = write!(out, "%{n}");
            }
            'F' => out.push_str("%Y-%m-%d"),
            'T' => out.push_str("%H:%M:%S"),
            'z' | 'Z' => out.push_str("%z"),
            '%' => out.push('%'),
            other => {
                return Err(AppError::Template(format!(
                    "unsupported strftime specifier '%{other}'"
                )))
            }
        };
    }
    Ok(out)
}

/// Render a stored state value as a SQL literal for interpolation.
pub fn state_to_literal(v: &libdatasource::model::Value) -> String {
    match v {
        libdatasource::model::Value::Null => "NULL".into(),
        libdatasource::model::Value::Bool(b) => if *b { "1" } else { "0" }.into(),
        libdatasource::model::Value::Int(i) => i.to_string(),
        libdatasource::model::Value::UInt(u) => u.to_string(),
        libdatasource::model::Value::Float(f) => f.to_string(),
        libdatasource::model::Value::Decimal(s)
        | libdatasource::model::Value::String(s)
        | libdatasource::model::Value::Date(s) => format!("'{}'", s.replace('\'', "''")),
        libdatasource::model::Value::Bytes(b) => format!("X'{}'", hex(b)),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02X}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn env_and_state() {
        // SAFETY: setting a unique test-only env var is fine in a single
        // threaded test binary.
        unsafe { std::env::set_var("DT_TEST_VAR", "hello") };
        let t = Template {
            resolve_state: &|k| {
                if k == "max_id" {
                    Ok(Some("42".into()))
                } else {
                    Ok(None)
                }
            },
            now: &Local::now,
        };
        let out = t.expand("id > ${state.max_id} and x = '${env.DT_TEST_VAR}'").unwrap();
        assert_eq!(out, "id > 42 and x = 'hello'");
    }

    #[test]
    fn sys_now_offsets() {
        let fixed = Local.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let t = Template {
            resolve_state: &|_| Ok(None),
            now: &(move || fixed),
        };
        let out = t.expand("${sys.now('%Y-%m-%d %H:%M:%S')}").unwrap();
        assert_eq!(out, "'2024-01-01 12:00:00'");
        let out = t.expand("${sys.now('%Y-%m-%d') -1D}").unwrap();
        assert_eq!(out, "'2023-12-31'");
        let out = t.expand("${sys.now('%H:%M:%S') -30S}").unwrap();
        assert_eq!(out, "'11:59:30'");
    }
}