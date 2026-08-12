use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{DatasourceError, Result};

/// The value model is the fixed interchange format between every datasource
/// plugin and the DataTransfer core. Every plugin MUST convert the native
/// database value into one of these variants and back, no matter the
/// underlying database (MySQL / PostgreSQL / Oracle / ...).
///
/// Serialises as an adjacently tagged enum (`{"t":"Int","v":42}`) for the C
/// ABI. Deserialising is lenient: it accepts the tagged form as well as plain
/// JSON literals (`42`, `"text"`, `true`, `null`) so configuration files can
/// use natural YAML values.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "t", content = "v")]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    /// Decimal / NUMERIC: kept as raw string to preserve exact precision.
    Decimal(String),
    String(String),
    Bytes(Vec<u8>),
    /// Date & timestamp values, formatted as `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS[.ffffff]`.
    Date(String),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "t", content = "v")]
enum TaggedValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Decimal(String),
    String(String),
    Bytes(Vec<u8>),
    Date(String),
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;

        let raw = serde_json::Value::deserialize(deserializer)?;
        if raw.as_object().map(|o| o.contains_key("t")).unwrap_or(false) {
            let tagged = serde_json::from_value::<TaggedValue>(raw)
                .map_err(|e| D::Error::custom(format!("invalid tagged value: {e}")))?;
            return Ok(match tagged {
                TaggedValue::Null => Value::Null,
                TaggedValue::Bool(b) => Value::Bool(b),
                TaggedValue::Int(i) => Value::Int(i),
                TaggedValue::UInt(u) => Value::UInt(u),
                TaggedValue::Float(f) => Value::Float(f),
                TaggedValue::Decimal(s) => Value::Decimal(s),
                TaggedValue::String(s) => Value::String(s),
                TaggedValue::Bytes(b) => Value::Bytes(b),
                TaggedValue::Date(s) => Value::Date(s),
            });
        }
        match raw {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Int(i))
                } else if let Some(u) = n.as_u64() {
                    Ok(Value::UInt(u))
                } else {
                    Ok(Value::Float(n.as_f64().unwrap_or(0.0)))
                }
            }
            serde_json::Value::String(s) => Ok(Value::String(s)),
            other => Err(D::Error::custom(format!(
                "cannot convert JSON value to Value: {other}"
            ))),
        }
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) | Value::Decimal(s) | Value::Date(s) => Some(s),
            _ => None,
        }
    }

    /// Render a value for use inside a SQL literal. `None` for values that
    /// must be bound as parameters instead.
    pub fn sql_literal(&self) -> Option<String> {
        match self {
            Value::Null => Some("NULL".to_string()),
            Value::Bool(b) => Some(if *b { "1" } else { "0" }.to_string()),
            Value::Int(i) => Some(i.to_string()),
            Value::UInt(u) => Some(u.to_string()),
            Value::Float(f) => Some(format!("{}", f)),
            Value::Decimal(s) | Value::String(s) | Value::Date(s) => {
                Some(format!("'{}'", s.replace('\'', "''")))
            }
            Value::Bytes(b) => Some(format!("X'{}'", hex(b))),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::UInt(u) => write!(f, "{u}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Decimal(s) | Value::String(s) | Value::Date(s) => write!(f, "{s}"),
            Value::Bytes(b) => write!(f, "0x{}", hex(b)),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02X}", b));
    }
    out
}

/// A single logical row. Values are keyed by column name; ordering is
/// reconstructed from the schema/column list at read/write time.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub data: BTreeMap<String, Value>,
}

impl Row {
    pub fn new() -> Self {
        Row::default()
    }

    pub fn get(&self, col: &str) -> Option<&Value> {
        self.data.get(col)
    }

    pub fn insert(&mut self, col: impl Into<String>, value: Value) {
        self.data.insert(col.into(), value);
    }

    pub fn columns(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    /// Extract values following `columns` order.
    pub fn extract(&self, columns: &[String]) -> Vec<Value> {
        columns
            .iter()
            .map(|c| self.data.get(c).cloned().unwrap_or(Value::Null))
            .collect()
    }

    /// Rebuild a row keeping only `columns`, in `columns` order.
    pub fn project(&self, columns: &[String]) -> Row {
        let mut data = BTreeMap::new();
        for c in columns {
            data.insert(c.clone(), self.data.get(c).cloned().unwrap_or(Value::Null));
        }
        Row { data }
    }
}

/// Column metadata as returned by `Datasource::get_schema`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    /// Native type representation, e.g. `varchar(64)`, `bigint`, `number(10,2)`.
    pub data_type: String,
    pub nullable: bool,
    pub is_primary: bool,
    pub ordinal: usize,
}

/// Table structure returned by `Datasource::get_schema`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub table: String,
    pub columns: Vec<Column>,
}

impl TableSchema {
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn primary_keys(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|c| c.is_primary)
            .map(|c| c.name.clone())
            .collect()
    }

    pub fn get(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// Connection definition read from `datasource.yaml`. The plugin receives the
/// JSON serialisation of this struct in `connect`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub name: String,
    /// `mysql`, `postgresql`, `oracle`, ...
    pub conn_type: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    #[serde(default)]
    pub schema: Option<String>,
    pub username: String,
    pub password: String,
    /// Extra driver options, e.g. `{"ssl_mode": "require", "charset": "utf8mb4"}`.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    #[serde(default = "default_max_pool")]
    pub max_pool_size: u32,
    #[serde(default = "default_timeout")]
    pub connect_timeout_secs: u64,
}

fn default_max_pool() -> u32 {
    4
}

fn default_timeout() -> u64 {
    15
}

impl ConnectionConfig {
    pub fn param(&self, key: &str) -> Option<&str> {
        self.params.get(key).map(|s| s.as_str())
    }
}

/// Write mode for a target table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMode {
    /// TRUNCATE the target table, then append all rows.
    Full,
    /// INSERT ... ON DUPLICATE KEY UPDATE ... (MySQL)
    /// INSERT ... ON CONFLICT (pk) DO UPDATE ... (PostgreSQL/Oracle)
    Upsert,
    /// Plain INSERT, never touches existing rows.
    Append,
}

impl SyncMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncMode::Full => "full",
            SyncMode::Upsert => "upsert",
            SyncMode::Append => "append",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<SyncMode> {
        match s.to_ascii_lowercase().as_str() {
            "full" => Ok(SyncMode::Full),
            "upsert" => Ok(SyncMode::Upsert),
            "append" => Ok(SyncMode::Append),
            other => Err(DatasourceError::InvalidArgument(format!(
                "unknown sync mode: {other}"
            ))),
        }
    }
}
