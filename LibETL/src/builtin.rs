//! Collection of built-in processors. Plugins may use these, or ship their own.

use crate::error::{EtlError, Result};
use crate::model::{EtlOutputRow, EtlRow};
use crate::trait_def::{EtlContext, EtlProcessor};
use libdatasource::model::{Row, Value};
use serde::{Deserialize, Serialize};

pub fn transform_output(ctx: &EtlContext, _input: &EtlRow, row: Row) -> EtlOutputRow {
    // Use the configured/derived target table name if present, otherwise the
    // job default is applied by the framework (empty table).
    let target_table = ctx
        .target_schema
        .map(|s| s.table.clone())
        .unwrap_or_default();
    EtlOutputRow::new(target_table, row)
}

/// Rename one or more columns: `column: { from: "a", to: "b" }`.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RenameConfig {
    pub column: Option<Vec<RenameItem>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RenameItem {
    pub from: String,
    pub to: String,
}

/// `{ "rename": { "column": [ { "from": "id", "to": "user_id" } ] } }`
#[derive(Debug, Serialize, Deserialize)]
pub struct RenameProcessor {
    pub rename: RenameConfig,
}

impl EtlProcessor for RenameProcessor {
    fn name(&self) -> &str {
        "rename"
    }

    fn process(&self, ctx: &mut EtlContext, input: &EtlRow) -> Result<Vec<EtlOutputRow>> {
        let mut row = input.row.clone();
        for item in self.rename.column.iter().flatten() {
            if let Some(v) = row.data.remove(&item.from) {
                row.data.insert(item.to.clone(), v);
            }
        }
        Ok(vec![transform_output(ctx, input, row)])
    }
}

/// Add constant columns: `{ "set": { "constant": { "app_id": "erp" } } }`
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SetConfig {
    pub constant: Option<std::collections::BTreeMap<String, Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetProcessor {
    pub set: SetConfig,
}

impl EtlProcessor for SetProcessor {
    fn name(&self) -> &str {
        "set"
    }

    fn process(&self, ctx: &mut EtlContext, input: &EtlRow) -> Result<Vec<EtlOutputRow>> {
        let mut row = input.row.clone();
        if let Some(constants) = &self.set.constant {
            for (k, v) in constants {
                row.data.insert(k.clone(), v.clone());
            }
        }
        Ok(vec![transform_output(ctx, input, row)])
    }
}

/// Filter rows: `{ "filter": { "keep": "expr", "drop": "expr" } }`
/// Supported operators: `not`, `null`, `not_null`, `eq`, `ne`, `gt`, `gte`,
/// `lt`, `lte`. Values come from the row by `$column` reference or a literal.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FilterConfig {
    pub keep: Option<String>,
    pub drop: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterProcessor {
    pub filter: FilterConfig,
}

impl EtlProcessor for FilterProcessor {
    fn name(&self) -> &str {
        "filter"
    }

    fn process(&self, ctx: &mut EtlContext, input: &EtlRow) -> Result<Vec<EtlOutputRow>> {
        if let Some(expr) = &self.filter.keep {
            if !eval_condition(expr, &input.row)? {
                return Ok(Vec::new());
            }
        }
        if let Some(expr) = &self.filter.drop {
            if eval_condition(expr, &input.row)? {
                return Ok(Vec::new());
            }
        }
        Ok(vec![EtlOutputRow::new(
            ctx.target_schema.map(|s| s.table.clone()).unwrap_or_default(),
            input.row.clone(),
        )])
    }
}

/// Convert a column to another storage type: `{ "cast": { "column": [ { "name": "age", "type": "int" } ] } }`
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CastConfig {
    pub column: Option<Vec<CastItem>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CastItem {
    pub name: String,
    #[serde(rename = "type")]
    pub target_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CastProcessor {
    pub cast: CastConfig,
}

impl EtlProcessor for CastProcessor {
    fn name(&self) -> &str {
        "cast"
    }

    fn process(&self, ctx: &mut EtlContext, input: &EtlRow) -> Result<Vec<EtlOutputRow>> {
        let mut row = input.row.clone();
        for item in self.cast.column.iter().flatten() {
            if let Some(v) = row.data.get_mut(&item.name) {
                *v = cast_value(v, &item.target_type)?;
            }
        }
        Ok(vec![transform_output(ctx, input, row)])
    }
}

fn cast_value(v: &Value, ty: &str) -> Result<Value> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    match ty.to_ascii_lowercase().as_str() {
        "int" | "integer" | "bigint" => Ok(Value::Int(parse_int(v)?)),
        "float" | "double" => Ok(Value::Float(parse_float(v)?)),
        "bool" | "boolean" => Ok(Value::Bool(parse_bool(v)?)),
        "string" | "varchar" | "text" => Ok(Value::String(match v {
            Value::String(s) => s.clone(),
            Value::Decimal(s) | Value::Date(s) => s.clone(),
            other => format!("{other}"),
        })),
        "decimal" | "numeric" => Ok(Value::Decimal(parse_numeric(v)?)),
        other => Err(EtlError::Config(format!("unsupported cast type '{other}'"))),
    }
}

fn parse_int(v: &Value) -> Result<i64> {
    match v {
        Value::Int(i) => Ok(*i),
        Value::UInt(u) => Ok(*u as i64),
        Value::Float(f) => Ok(*f as i64),
        Value::String(s) | Value::Decimal(s) | Value::Date(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|e| EtlError::Expression(format!("cannot cast '{s}' to int: {e}"))),
        _ => Err(EtlError::Expression(format!("cannot cast {v:?} to int"))),
    }
}

fn parse_float(v: &Value) -> Result<f64> {
    match v {
        Value::Int(i) => Ok(*i as f64),
        Value::UInt(u) => Ok(*u as f64),
        Value::Float(f) => Ok(*f),
        Value::String(s) | Value::Decimal(s) | Value::Date(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|e| EtlError::Expression(format!("cannot cast '{s}' to float: {e}"))),
        _ => Err(EtlError::Expression(format!("cannot cast {v:?} to float"))),
    }
}

fn parse_bool(v: &Value) -> Result<bool> {
    match v {
        Value::Bool(b) => Ok(*b),
        Value::Int(i) => Ok(*i != 0),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "y" => Ok(true),
            "false" | "0" | "no" | "n" => Ok(false),
            _ => Err(EtlError::Expression(format!("cannot cast '{s}' to bool"))),
        },
        _ => Err(EtlError::Expression(format!("cannot cast {v:?} to bool"))),
    }
}

fn parse_numeric(v: &Value) -> Result<String> {
    match v {
        Value::Decimal(s) => Ok(s.clone()),
        Value::Int(i) => Ok(i.to_string()),
        Value::UInt(u) => Ok(u.to_string()),
        Value::Float(f) => Ok(format!("{}", f)),
        Value::String(s) => Ok(s.clone()),
        _ => Err(EtlError::Expression(format!("cannot cast {v:?} to decimal"))),
    }
}

fn eval_condition(expr: &str, row: &Row) -> Result<bool> {
    let expr = expr.trim();
    let toks: Vec<&str> = expr.split_whitespace().collect();
    if toks.len() != 3 {
        return Err(EtlError::Expression(format!(
            "invalid filter expression: '{expr}' (expected '<column> <op> <value>')"
        )));
    }
    let op = toks[1];
    let rhs_lit = resolve_literal(toks[2], row)?;
    let value = row
        .get(toks[0])
        .cloned()
        .unwrap_or(Value::Null);

    use std::cmp::Ordering;
    let cmp = |l: &Value, r: &Value| -> Result<Ordering> {
        // Coerce numeric pairs.
        if let (Some(a), Some(b)) = (as_f64(l), as_f64(r)) {
            return Ok(a.partial_cmp(&b).unwrap_or(Ordering::Equal));
        }
        let ls = l.as_str().unwrap_or_default();
        let rs = r.as_str().unwrap_or_default();
        Ok(ls.cmp(rs))
    };

    match op {
        "eq" => Ok(equal(&value, &rhs_lit)),
        "ne" => Ok(!equal(&value, &rhs_lit)),
        "is" if rhs_lit.is_null() => Ok(value.is_null()),
        "not" if rhs_lit.is_null() => Ok(!value.is_null()),
        "gt" => Ok(cmp(&value, &rhs_lit)? == Ordering::Greater),
        "gte" => Ok(cmp(&value, &rhs_lit)? != Ordering::Less),
        "lt" => Ok(cmp(&value, &rhs_lit)? == Ordering::Less),
        "lte" => Ok(cmp(&value, &rhs_lit)? != Ordering::Greater),
        other => Err(EtlError::Expression(format!(
            "unsupported operator '{other}' in '{expr}'"
        ))),
    }
}

fn equal(l: &Value, r: &Value) -> bool {
    if l.is_null() || r.is_null() {
        return false;
    }
    if let (Some(a), Some(b)) = (as_f64(l), as_f64(r)) {
        return (a - b).abs() < f64::EPSILON;
    }
    l == r
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::UInt(u) => Some(*u as f64),
        Value::Float(f) => Some(*f),
        Value::Decimal(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Resolve an operand: `$col` reads the column value, anything else is a literal.
fn resolve_literal(tok: &str, row: &Row) -> Result<Value> {
    if let Some(name) = tok.strip_prefix('$') {
        return Ok(row.get(name).cloned().unwrap_or(Value::Null));
    }
    match tok {
        "null" | "NULL" => Ok(Value::Null),
        "true" | "TRUE" => Ok(Value::Bool(true)),
        "false" | "FALSE" => Ok(Value::Bool(false)),
        _ => {
            if let Ok(i) = tok.parse::<i64>() {
                Ok(Value::Int(i))
            } else if let Ok(f) = tok.parse::<f64>() {
                Ok(Value::Float(f))
            } else {
                Ok(Value::String(tok.trim_matches('\'').to_string()))
            }
        }
    }
}
