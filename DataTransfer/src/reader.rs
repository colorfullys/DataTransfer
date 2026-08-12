//! Paginated source reader.
//!
//! Builds `SELECT <cols> FROM <table> WHERE <expanded template>` and walks it
//! page by page with `Datasource::query_page`. High-water-mark state is read
//! (immutably) at construction and updated by the job runner afterwards.

use std::collections::BTreeMap;
use std::sync::Arc;

use libdatasource::datasource::Datasource;
use libdatasource::model::{TableSchema, Value};

use crate::config::{SourceConfig, StateSpec};
use crate::error::{AppError, AppResult};
use crate::state::StateStore;
use crate::templates::{state_to_literal, Template};

pub struct Reader {
    ds: Arc<dyn Datasource>,
    sql: String,
    source: SourceConfig,
    schema: Option<TableSchema>,
    page_size: u64,
    offset: u64,
    done: bool,
}

impl Reader {
    /// Build the reader: resolves the WHERE template (stored state values or
    /// schema-derived defaults for first runs) and stores the query that is
    /// paged during the run.
    pub fn new(
        ds: Arc<dyn Datasource>,
        conn_type: &str,
        source: SourceConfig,
        specs: &BTreeMap<String, StateSpec>,
        state: &StateStore,
        page_size: u64,
    ) -> AppResult<Reader> {
        let schema = ds.get_schema(&source.table).ok();
        let expander = Template {
            resolve_state: &|key| resolve_state_default(state, specs, &schema, key),
            now: &chrono::Local::now,
        };
        let sql = build_select(&source, &expander, conn_type)?;
        log::debug!("reader '{}' SQL: {sql}", source.table);
        Ok(Reader {
            ds,
            sql,
            source,
            schema,
            page_size,
            offset: 0,
            done: false,
        })
    }

    pub fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_ref()
    }

    /// Fetch the next page. Returns `Ok(None)` when the source is exhausted.
    pub fn next_page(&mut self) -> AppResult<Option<Vec<libdatasource::model::Row>>> {
        if self.done {
            return Ok(None);
        }
        let rows = self
            .ds
            .query_page(&self.sql, self.offset, self.page_size)
            .map_err(|e| {
                AppError::Datasource(format!(
                    "read {} (offset {}): {e}",
                    self.source.table, self.offset
                ))
            })?;
        self.offset += self.page_size;
        if rows.is_empty() {
            self.done = true;
            return Ok(None);
        }
        Ok(Some(rows))
    }
}

/// Build `SELECT <cols> FROM <table> [WHERE <expanded>]`.
fn build_select(source: &SourceConfig, t: &Template, conn_type: &str) -> AppResult<String> {
    let quote = |n: &str| -> String {
        if conn_type == "mysql" {
            format!("`{}`", n.replace('`', "``"))
        } else {
            format!("\"{}\"", n.replace('"', "\"\""))
        }
    };

    let mut sql = String::from("SELECT ");
    if source.columns.is_empty() {
        sql.push('*');
    } else {
        let cols: Vec<String> = source.columns.iter().map(|c| quote(c)).collect();
        sql.push_str(&cols.join(", "));
    }
    sql.push_str(" FROM ");
    sql.push_str(&quote(&source.table));

    if let Some(w) = &source.where_clause {
        let w = w.trim();
        if !w.is_empty() {
            let expanded = t.expand(w)?;
            sql.push_str(" WHERE ");
            sql.push_str(&expanded);
        }
    }
    Ok(sql)
}

/// Resolve `${state.key}` to a SQL literal: the stored value if present, else
/// a schema-derived low bound so the first run reads everything.
fn resolve_state_default(
    state: &StateStore,
    specs: &BTreeMap<String, StateSpec>,
    schema: &Option<TableSchema>,
    key: &str,
) -> AppResult<Option<String>> {
    if let Some(v) = state.get(key) {
        return Ok(Some(state_to_literal(v)));
    }
    let spec = specs.get(key).ok_or_else(|| {
        AppError::Template(format!("state key '{key}' referenced but not declared in sync.state"))
    })?;
    let default = schema
        .as_ref()
        .and_then(|s| s.get(&spec.column))
        .map(|c| default_literal(&c.data_type))
        .unwrap_or_else(|| "0".to_string());
    Ok(Some(default))
}

/// A safe lower bound for a first run, based on the column's native type.
fn default_literal(data_type: &str) -> String {
    let t = data_type.to_ascii_lowercase();
    if t.contains("time") || t.contains("date") {
        "'1970-01-01 00:00:00'".into()
    } else if t.contains("char")
        || t.contains("text")
        || t.contains("string")
        || t.contains("uuid")
        || t.contains("clob")
    {
        "''".into()
    } else {
        "0".into()
    }
}

/// Strictly-greater comparison across numeric and string/date values.
pub fn value_gt(a: &Value, b: &Value) -> bool {
    match (num(a), num(b)) {
        (Some(x), Some(y)) => x > y,
        _ => match (a.as_str(), b.as_str()) {
            (Some(x), Some(y)) => x > y,
            _ => false,
        },
    }
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::UInt(u) => Some(*u as f64),
        Value::Float(f) => Some(*f),
        Value::Decimal(s) => s.trim().parse().ok(),
        _ => None,
    }
}