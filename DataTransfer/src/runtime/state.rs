//! Per-job high-water-mark persistence.
//!
//! State is stored as a small JSON object next to the job, e.g.
//! `state/user_sync.json`:
//!
//! ```json
//! { "max_id": 42, "max_updated": "2026-01-01 10:00:00" }
//! ```
//!
//! Writes go to a temp file first and are renamed into place, so a crash never
//! leaves a truncated state file. `libdatasource::Value` is flattened to plain
//! JSON numbers / strings / null on disk.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use libdatasource::model::Value;

use crate::error::{AppError, AppResult};

pub struct StateStore {
    job: String,
    dir: PathBuf,
    values: BTreeMap<String, Value>,
}

impl StateStore {
    pub fn load(state_dir: &Path, job: &str) -> AppResult<StateStore> {
        std::fs::create_dir_all(state_dir).map_err(|e| {
            AppError::State(format!("cannot create state dir {}: {e}", state_dir.display()))
        })?;
        let path = state_dir.join(safe_file_name(job));
        let mut store = StateStore {
            job: job.to_string(),
            dir: state_dir.to_path_buf(),
            values: BTreeMap::new(),
        };
        if path.is_file() {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| AppError::State(format!("cannot read {}: {e}", path.display())))?;
            let map: BTreeMap<String, serde_json::Value> = serde_json::from_str(&text)
                .map_err(|e| AppError::State(format!("invalid state {}: {e}", path.display())))?;
            for (k, v) in map {
                store.values.insert(k, json_to_value(v));
            }
        }
        store.path()?; // validate filename up front
        Ok(store)
    }

    fn path(&self) -> AppResult<PathBuf> {
        let p = self.dir.join(safe_file_name(&self.job));
        Ok(p)
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.values.get(key)
    }

    pub fn set(&mut self, key: impl Into<String>, value: Value) {
        self.values.insert(key.into(), value);
    }

    /// Persist to disk (atomic: temp file + rename).
    pub fn save(&self) -> AppResult<()> {
        let path = self.path()?;
        let tmp = path.with_extension("json.tmp");
        std::fs::create_dir_all(&self.dir).map_err(|e| {
            AppError::State(format!("cannot create state dir {}: {e}", self.dir.display()))
        })?;
        let mut obj = serde_json::Map::new();
        for (k, v) in &self.values {
            obj.insert(k.clone(), value_to_json(v));
        }
        let text = serde_json::to_string_pretty(&serde_json::Value::Object(obj))?;
        std::fs::write(&tmp, text)
            .map_err(|e| AppError::State(format!("cannot write {}: {e}", tmp.display())))?;
        std::fs::rename(&tmp, &path)
            .map_err(|e| AppError::State(format!("cannot rename {} -> {}: {e}", tmp.display(), path.display())))?;
        Ok(())
    }
}

fn safe_file_name(job: &str) -> String {
    let clean: String = job
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect();
    format!("{}.json", clean)
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::UInt(u) => serde_json::Value::Number((*u).into()),
        Value::Float(f) => serde_json::json!(f),
        Value::Decimal(s) | Value::String(s) | Value::Date(s) => {
            serde_json::Value::String(s.clone())
        }
        Value::Bytes(b) => serde_json::Value::String(hex(b)),
    }
}

fn json_to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::UInt(u)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::String(s),
        _ => Value::String(v.to_string()),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02X}"));
    }
    s
}