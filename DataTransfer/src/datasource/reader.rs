//! Paginated source reader.
//!
//! Builds `SELECT <cols> FROM <table> WHERE <expanded template>` and walks it
//! page by page with `Datasource::query_page`. High-water-mark state is read
//! (immutably) at construction and updated by the job runner afterwards.

use std::collections::BTreeMap;
use std::sync::Arc;

use libdatasource::datasource::Datasource;
use libdatasource::model::{Column, TableSchema, Value};

use crate::config::{SourceConfig, StateSpec};
use crate::error::{AppError, AppResult};
use crate::runtime::StateStore;
use crate::support::templates::{state_to_literal, Template};

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
        let schema = if !source.table.is_empty() {
            ds.get_schema(&source.table).ok()
        } else if let Some(sel) = &source.select {
            match probe_select_schema(ds.as_ref(), conn_type, sel, &|key| {
                resolve_state_default(state, specs, &None, key)
            }) {
                Ok(Some(s)) => {
                    log::debug!(
                        "job reader '<custom-select>': inferred schema from select probe ({} column(s))",
                        s.columns.len()
                    );
                    Some(s)
                }
                Ok(None) => None,
                Err(e) => {
                    log::warn!("job reader '<custom-select>': could not probe schema from select: {e}");
                    None
                }
            }
        } else {
            None
        };
        let expander = Template {
            resolve_state: &|key| resolve_state_default(state, specs, &schema, key),
            now: &chrono::Local::now,
        };
        let sql = build_select(&source, &expander, conn_type)?;
        let batch_size = source.batch_limit.unwrap_or(page_size).max(1);
        log::debug!("reader '{}' SQL: {sql}", source_label(&source));
        log::info!("job reader '{}' SQL: {sql}", source_label(&source));
        if source.batch_limit.is_some() {
            log::info!(
                "job reader '{}' batch limit: {} row(s) per query",
                source_label(&source),
                batch_size
            );
        }
        Ok(Reader {
            ds,
            sql,
            source,
            schema,
            page_size: batch_size,
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
                    "read {} (offset {}): {e}\nsql: {}",
                    source_label(&self.source),
                    self.offset,
                    self.sql
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

/// A stable label for log/error output when no `table` is configured.
fn source_label(source: &SourceConfig) -> &str {
    if source.table.is_empty() {
        "<custom-select>"
    } else {
        &source.table
    }
}

/// Infer the result schema of a custom `select` without a `table` to read via
/// `get_schema`. Runs `SELECT * FROM (<expanded select>) LIMIT 1` and derives
/// each column's type from the returned sample value.
///
/// Returns `Ok(None)` when the select returns no rows (nothing to infer from).
fn probe_select_schema(
    ds: &dyn Datasource,
    conn_type: &str,
    select: &str,
    resolve_state: &dyn Fn(&str) -> AppResult<Option<String>>,
) -> AppResult<Option<TableSchema>> {
    let expanded = Template {
        resolve_state,
        now: &chrono::Local::now,
    }
    .expand(select)?;
    let s = expanded.trim().trim_end_matches([';', ' ']);
    if s.is_empty() {
        return Ok(None);
    }
    let sql = if conn_type.starts_with("oracle") {
        format!("SELECT * FROM ({s}) WHERE ROWNUM <= 1")
    } else {
        format!("SELECT * FROM ({s}) dt_probe LIMIT 1")
    };
    let mut rows = ds
        .query(&sql)
        .map_err(|e| AppError::Datasource(format!("probe select schema: {e}\nsql: {sql}")))?;
    if rows.is_empty() {
        return Ok(None);
    }
    let sample = rows.remove(0);
    let columns: Vec<Column> = sample
        .data
        .iter()
        .map(|(name, v)| Column {
            name: name.clone(),
            data_type: infer_type(v).to_string(),
            nullable: true,
            is_primary: false,
            ordinal: 0,
        })
        .collect();
    Ok(Some(TableSchema {
        table: "<custom-select>".to_string(),
        columns,
    }))
}

/// Coarse column type approximation from a sampled value — only used to pick a
/// safe first-run lower bound for `${state.*}` templates.
fn infer_type(v: &Value) -> &'static str {
    match v {
        Value::Null | Value::String(_) | Value::Bytes(_) => "varchar",
        Value::Bool(_) => "bool",
        Value::Int(_) | Value::UInt(_) => "bigint",
        Value::Float(_) => "double",
        Value::Decimal(_) => "decimal",
        Value::Date(_) => "datetime",
    }
}

/// Build the reader's base query.
///
/// * If `source.select` is given it is used verbatim as the full SELECT (for
///   joins etc.); `${state.*}` / `${sys.now(...)}` templates are expanded. Any
///   `where` clause is ignored in this mode (put filtering inside the `select`).
/// * Otherwise generate `SELECT <cols> FROM <table> [WHERE <expanded>]`.
///
/// Pagination is added by each datasource plugin's `query_page` in the lib
/// (MySQL/PG `LIMIT/OFFSET`, Oracle `OFFSET ROWS FETCH NEXT`).
fn build_select(source: &SourceConfig, t: &Template, conn_type: &str) -> AppResult<String> {
    if let Some(s) = &source.select {
        let s = s.trim();
        if s.is_empty() {
            return Err(AppError::Config(format!(
                "job source '{}': 'select' must not be empty",
                source.table
            )));
        }
        if source.where_clause.as_deref().is_some_and(|w| !w.trim().is_empty()) {
            log::warn!(
                "job source '{}': 'select' overrides 'where'; the custom select must contain its own filtering (use {{{{state.*}}}} templates)",
                source.table
            );
        }
        return t.expand(s);
    }

    if source.table.is_empty() {
        return Err(AppError::Config(
            "source has no 'table' and no 'select' to build a query from".into(),
        ));
    }

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