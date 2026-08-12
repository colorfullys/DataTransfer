//! Configuration loading for `config.yaml`, `datasource.yaml` and job files.
//!
//! YAML is parsed with `yaml-rust2`; hierarchies are converted by hand (no
//! serde-yaml dependency). Secrets like `${MY_PASSWORD}` are expanded from the
//! process environment at load time so they never end up in state or logs.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use libdatasource::model::{ConnectionConfig, SyncMode};
use yaml_rust2::yaml::Yaml;
use yaml_rust2::YamlLoader;

use crate::error::{AppError, AppResult};

/// Top-level configuration merged from `config.yaml` + `datasource.yaml`.
#[derive(Debug)]
pub struct AppConfig {
    /// datasource type -> plugin file, e.g. `mysql -> plugins/datasource/mysql.so`.
    pub datasource_plugins: BTreeMap<String, String>,
    /// additional ETL plugin cdylibs.
    pub etl_plugin_paths: Vec<String>,
    /// job file paths (relative to the config file directory).
    pub job_files: Vec<String>,
    pub log_level: String,
    pub log_file: Option<String>,
    /// max concurrently running jobs.
    pub workers: usize,
    /// per-pass retry count.
    pub retry: u32,
    /// datasource.yaml location (relative to config dir).
    pub datasource_file: String,
    /// state directory (relative to config dir).
    pub state_dir: String,
    /// source paging size.
    pub page_size: u64,
    /// Named connections from `datasource.yaml`, name -> settings.
    pub connections: BTreeMap<String, ConnectionSettings>,
    /// Directory containing the main config file (used to resolve paths).
    pub base_dir: PathBuf,
}

impl AppConfig {
    /// Load everything: main config + datasource connections. All paths are
    /// resolved relative to `config_path`.
    pub fn load(config_path: &Path) -> AppResult<AppConfig> {
        let base_dir = config_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let text = std::fs::read_to_string(config_path)
            .map_err(|e| AppError::Config(format!("cannot read {}: {e}", config_path.display())))?;
        let docs = YamlLoader::load_from_str(&text)
            .map_err(|e| AppError::Config(format!("invalid yaml in {}: {e}", config_path.display())))?;
        let cfg = &docs[0];

        // ---- datasource plugin paths ----
        let mut datasource_plugins = BTreeMap::new();
        if let Some(h) = cfg["datasource"].as_hash() {
            for (k, v) in h {
                let key = k.as_str().unwrap_or("").to_string();
                let val = v.as_str().unwrap_or("").to_string();
                if !key.is_empty() && !val.is_empty() {
                    datasource_plugins.insert(key, resolve_relative(&base_dir, &val));
                }
            }
        }
        log::info!("datasource plugins: {datasource_plugins:?}");

        // ---- etl plugin paths ----
        let mut etl_plugin_paths = Vec::new();
        if let Some(v) = cfg["etl"].as_vec() {
            for item in v {
                if let Some(s) = item.as_str() {
                    etl_plugin_paths.push(resolve_relative(&base_dir, s));
                }
            }
        }

        // ---- job files ----
        let mut job_files = Vec::new();
        if let Some(v) = cfg["jobs"].as_vec() {
            for item in v {
                if let Some(s) = item.as_str() {
                    job_files.push(resolve_relative(&base_dir, s));
                }
            }
        }

        // ---- logging ----
        let log_level = cfg["logging"]["level"]
            .as_str()
            .unwrap_or("info")
            .to_string();
        let log_file = cfg["logging"]["file"]
            .as_str()
            .map(|s| resolve_relative(&base_dir, s));

        // ---- runtime ----
        let workers = cfg["runtime"]["workers"].as_i64().unwrap_or(4).max(1) as usize;
        let retry = cfg["runtime"]["retry"]
            .as_i64()
            .unwrap_or(3)
            .max(0) as u32;
        let page_size = cfg["runtime"]["page_size"]
            .as_i64()
            .unwrap_or(500)
            .max(1) as u64;

        Ok(AppConfig {
            datasource_plugins,
            etl_plugin_paths,
            job_files,
            log_level,
            log_file,
            workers,
            retry,
            datasource_file: cfg["datasource_file"].as_str().unwrap_or("datasource.yaml").to_string(),
            state_dir: cfg["state_dir"].as_str().unwrap_or("state").to_string(),
            page_size,
            connections: BTreeMap::new(),
            base_dir,
        })
    }

    /// Load `datasource.yaml` (relative to `base_dir`) and merge its
    /// connections into `self`.
    pub fn load_datasources(&mut self, datasource_file: &str) -> AppResult<()> {
        let path = resolve_relative(&self.base_dir, datasource_file);
        let text = std::fs::read_to_string(&path)
            .map_err(|e| AppError::Config(format!("cannot read {path}: {e}")))?;
        let docs = YamlLoader::load_from_str(&text)
            .map_err(|e| AppError::Config(format!("invalid yaml in {path}: {e}")))?;
        let cfg = &docs[0];
        let Some(conns) = cfg["connections"].as_hash() else {
            return Err(AppError::Config(format!(
                "{path}: missing 'connections' section",
            )));
        };

        for (name, v) in conns {
            let name = name
                .as_str()
                .ok_or_else(|| AppError::Config("connection name must be a string".into()))?
                .to_string();
            let conn_type = v["type"]
                .as_str()
                .ok_or_else(|| AppError::Config(format!("connection '{name}': missing type")))?;
            let mut params = BTreeMap::new();
            if let Some(ph) = v["params"].as_hash() {
                for (pk, pv) in ph {
                    params.insert(
                        pk.as_str().unwrap_or("").to_string(),
                        pv.as_str().unwrap_or("").to_string(),
                    );
                }
            }
            let settings = ConnectionSettings {
                name: name.clone(),
                conn_type: conn_type.to_string(),
                host: v["host"].as_str().unwrap_or("localhost").to_string(),
                port: v["port"].as_i64().unwrap_or(0).max(1) as u16,
                database: v["database"].as_str().unwrap_or("").to_string(),
                schema: v["schema"].as_str().map(|s| s.to_string()),
                username: v["username"].as_str().unwrap_or("").to_string(),
                password: scalar_str(&v["password"]),
                params,
                max_pool_size: v["max_pool_size"].as_i64().unwrap_or(4).max(1) as u32,
                connect_timeout_secs: v["connect_timeout_secs"].as_i64().unwrap_or(15).max(1) as u64,
            };
            if settings.host.is_empty() {
                return Err(AppError::Config(format!(
                    "connection '{name}': missing host"
                )));
            }
            self.connections.insert(name, settings);
        }
        log::info!("loaded {} connection(s) from {path}", self.connections.len());
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionSettings {
    pub name: String,
    pub conn_type: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub schema: Option<String>,
    pub username: String,
    pub password: String,
    pub params: BTreeMap<String, String>,
    pub max_pool_size: u32,
    pub connect_timeout_secs: u64,
}

impl ConnectionSettings {
    pub fn to_connection_config(&self) -> ConnectionConfig {
        ConnectionConfig {
            name: self.name.clone(),
            conn_type: self.conn_type.clone(),
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            schema: self.schema.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            params: self.params.clone(),
            max_pool_size: self.max_pool_size,
            connect_timeout_secs: self.connect_timeout_secs,
        }
    }
}

/// Read a YAML scalar as a string, tolerating unquoted numbers/bools.
/// `password: 123456` parses as an integer in YAML; this keeps the value
/// instead of silently dropping it (`as_str()` returns `None` for numbers).
fn scalar_str(y: &Yaml) -> String {
    y.as_str()
        .map(|s| s.to_string())
        .or_else(|| y.as_i64().map(|i| i.to_string()))
        .or_else(|| y.as_f64().map(|f| f.to_string()))
        .or_else(|| y.as_bool().map(|b| b.to_string()))
        .unwrap_or_default()
}

/// Expand `${VAR}` / `${env.VAR}` tokens from the environment, leaving unknown
/// variables untouched so misconfiguration surfaces in the failing code path.
pub fn expand_env(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            out.push_str(&rest[start..]);
            return out;
        };
        let name = &after[..end];
        let lookup = name.strip_prefix("env.").unwrap_or(name);
        match std::env::var(lookup) {
            Ok(v) => out.push_str(&v),
            Err(_) => {
                // leave the token as-is (including braces)
                out.push_str(&rest[start..start + 2 + name.len() + 1]);
            }
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

// ---------------------------------------------------------------------------
// Job file parsing
// ---------------------------------------------------------------------------

/// A single step of a job's ETL pipeline.
#[derive(Debug, Clone)]
pub struct EtlStep {
    /// builtin name (`rename`, `set`, `filter`, `cast`) or loaded plugin name.
    pub name: String,
    pub kind: EtlStepKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EtlStepKind {
    Builtin,
    Plugin,
}

#[derive(Debug, Clone)]
pub struct JobConfig {
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub source: SourceConfig,
    pub target: TargetConfig,
    pub mode: SyncMode,
    /// state-key -> high water mark spec (only `max` supported today).
    pub state: BTreeMap<String, StateSpec>,
    pub cron: String,
    /// Number of parallel writer threads this job uses (rows are routed to
    /// them after the ETL pipeline). `1` is the default single-writer mode.
    pub writers: usize,
    /// ETL pipeline steps. `None` means the identity transform.
    pub etl: Vec<EtlStep>,
    /// Raw config blob for dynamic plugin steps (delivered as JSON).
    pub etl_configs: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct SourceConfig {
    pub connection: String,
    /// Source table. Optional when a custom `select` statement is provided.
    pub table: String,
    pub columns: Vec<String>,
    pub where_clause: Option<String>,
    pub primary_key: Vec<String>,
    /// Max rows fetched per query/paging pass (overrides `runtime.page_size`)
    /// before rows are routed to the writers.
    pub batch_limit: Option<u64>,
    /// Custom full SELECT statement (join support). Replaces the generated
    /// `SELECT <columns> FROM <table>`; `${state.*}` / `${sys.now(...)}`
    /// templates are still expanded inside it.
    pub select: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TargetConfig {
    pub connection: String,
    pub table: String,
    pub columns: Vec<String>,
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct StateSpec {
    pub kind: String,
    pub column: String,
}

impl JobConfig {
    /// Parse a job YAML file. Returns `None`-ish if missing/invalid, surfaced
    /// as an error here so broken jobs are reported at startup.
    pub fn load(path: &Path) -> AppResult<JobConfig> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| AppError::Config(format!("cannot read {}: {e}", path.display())))?;
        let docs = YamlLoader::load_from_str(&text)
            .map_err(|e| AppError::Config(format!("invalid yaml in {}: {e}", path.display())))?;
        let y = &docs[0];

        let name = y["name"]
            .as_str()
            .ok_or_else(|| AppError::Config(format!("{}: missing name", path.display())))?
            .to_string();
        let enabled = y["enabled"].as_bool().unwrap_or(true);

        let src = &y["source"];
        let src_conn = src["connection"]
            .as_str()
            .ok_or_else(|| AppError::Config(format!("job '{name}': source.connection")))?;
        let src_table = src["table"].as_str().unwrap_or("").to_string();
        let has_select = src["select"].as_str().map(|s| !s.trim().is_empty()).unwrap_or(false);
        if src_table.is_empty() && !has_select {
            return Err(AppError::Config(format!(
                "job '{name}': source needs a 'table' or a non-empty 'select'"
            )));
        }
        let source = SourceConfig {
            connection: src_conn.to_string(),
            table: src_table,
            columns: split_csv(src["columns"].as_str()),
            where_clause: src["where"].as_str().map(|s| s.to_string()),
            primary_key: split_csv(src["primary_key"].as_str()),
            batch_limit: src["batch_limit"].as_i64().filter(|v| *v > 0).map(|v| v as u64),
            select: src["select"].as_str().map(|s| s.to_string()),
        };

        let tgt = &y["target"];
        let tgt_conn = tgt["connection"]
            .as_str()
            .ok_or_else(|| AppError::Config(format!("job '{name}': target.connection")))?;
        let tgt_table = tgt["table"]
            .as_str()
            .ok_or_else(|| AppError::Config(format!("job '{name}': target.table")))?;
        let target = TargetConfig {
            connection: tgt_conn.to_string(),
            table: tgt_table.to_string(),
            columns: split_csv(tgt["columns"].as_str()),
            primary_key: split_csv(tgt["primary_key"].as_str()),
        };

        let mode = SyncMode::from_str(
            y["sync"]["mode"].as_str().unwrap_or("upsert"),
        )
        .map_err(|e| AppError::Config(format!("job '{name}': {e}")))?;

        let mut state = BTreeMap::new();
        if let Some(sh) = y["sync"]["state"].as_hash() {
            for (k, v) in sh {
                let key = k.as_str().unwrap_or("").to_string();
                state.insert(
                    key,
                    StateSpec {
                        kind: v["type"].as_str().unwrap_or("max").to_string(),
                        column: v["column"].as_str().unwrap_or("").to_string(),
                    },
                );
            }
        }

        let cron = y["schedule"]["cron"]
            .as_str()
            .ok_or_else(|| AppError::Config(format!("job '{name}': schedule.cron")))?;

        let writers = y["writers"].as_i64().unwrap_or(1).max(1) as usize;

        // ---- etl steps ----
        let mut etl = Vec::new();
        let mut etl_configs = Vec::new();
        if let Some(steps) = y["etl"].as_vec() {
            for step in steps {
                let (kind, config) = parse_step(step)?;
                etl_configs.push(yaml_to_json(&config).unwrap_or(serde_json::Value::Null));
                etl.push(kind);
            }
        } else if y["etl"].as_hash().is_some() {
            // single step form
            let (kind, config) = parse_step(y)?;
            etl_configs.push(yaml_to_json(&config).unwrap_or(serde_json::Value::Null));
            etl.push(kind);
        }

        Ok(JobConfig {
            name,
            description: y["description"].as_str().unwrap_or("").to_string(),
            enabled,
            source,
            target,
            mode,
            state,
            cron: cron.to_string(),
            writers,
            etl,
            etl_configs,
        })
    }
}

/// Parse one ETL step. Supported shapes:
/// * `{ builtin: rename, config: {...} }`
/// * `{ plugin: my_etl, config: {...} }`
/// * `{ rename: {...} }`  (builtin keyed by name)
/// * a bare name string `rename`
fn parse_step(step: &Yaml) -> AppResult<(EtlStep, Yaml)> {
    if let Some(h) = step.as_hash() {
        if let Some(plugin) = h.get(&Yaml::String("plugin".into())) {
            let name = plugin.as_str().unwrap_or("");
            if name.is_empty() {
                return Err(AppError::Config("etl step: plugin name empty".into()));
            }
            let config = h
                .get(&Yaml::String("config".into()))
                .cloned()
                .unwrap_or(Yaml::Null);
            return Ok((EtlStep { name: name.into(), kind: EtlStepKind::Plugin }, config));
        }
        if let Some(builtin) = h.get(&Yaml::String("builtin".into())) {
            let name = builtin.as_str().unwrap_or("");
            let config = h
                .get(&Yaml::String("config".into()))
                .cloned()
                .unwrap_or(Yaml::Null);
            let kind = if is_builtin(name) { EtlStepKind::Builtin } else { EtlStepKind::Plugin };
            return Ok((EtlStep { name: name.into(), kind }, config));
        }
        // single-key form: { rename: {...} }
        if let Some((k, v)) = h.iter().next() {
            if let Some(name) = k.as_str() {
                let kind = if is_builtin(name) { EtlStepKind::Builtin } else { EtlStepKind::Plugin };
                return Ok((EtlStep { name: name.into(), kind }, v.clone()));
            }
        }
        return Err(AppError::Config(format!(
            "etl step must be {{builtin|plugin|name: ...}}, got {step:?}"
        )));
    }
    if let Some(name) = step.as_str() {
        let kind = if is_builtin(name) { EtlStepKind::Builtin } else { EtlStepKind::Plugin };
        return Ok((
            EtlStep { name: name.into(), kind },
            Yaml::Null,
        ));
    }
    Err(AppError::Config(format!("invalid etl step: {step:?}")))
}

fn is_builtin(name: &str) -> bool {
    matches!(name, "rename" | "set" | "filter" | "cast")
}

pub fn split_csv(s: Option<&str>) -> Vec<String> {
    match s {
        Some(s) => s
            .split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect(),
        None => Vec::new(),
    }
}

/// Best-effort conversion of a yaml-rust2 node to `serde_json::Value`.
pub fn yaml_to_json(y: &Yaml) -> Option<serde_json::Value> {
    match y {
        Yaml::Real(s) => s.parse::<f64>().ok().map(serde_json::Value::from),
        Yaml::Integer(i) => Some(serde_json::Value::from(*i)),
        Yaml::String(s) => Some(serde_json::Value::String(s.clone())),
        Yaml::Boolean(b) => Some(serde_json::Value::Bool(*b)),
        Yaml::Array(items) => {
            let out: Vec<serde_json::Value> =
                items.iter().filter_map(yaml_to_json).collect();
            Some(serde_json::Value::Array(out))
        }
        Yaml::Hash(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                if let Some(v) = yaml_to_json(v) {
                    obj.insert(
                        k.as_str().unwrap_or("").to_string(),
                        v,
                    );
                }
            }
            Some(serde_json::Value::Object(obj))
        }
        Yaml::Null => Some(serde_json::Value::Null),
        _ => None,
    }
}

fn resolve_relative(base: &Path, p: &str) -> String {
    let pb = PathBuf::from(p);
    if pb.is_absolute() {
        return p.to_string();
    }
    base.join(pb).to_string_lossy().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_etl_steps() {
        let docs = YamlLoader::load_from_str(
            "etl:\n  - rename:\n      column:\n        - {from: a, to: b}\n  - plugin: xyz\n    config: {k: v}\n  - filter: {keep: \"a gt 0\"}\n",
        )
        .unwrap();
        let steps = docs[0]["etl"].as_vec().unwrap();
        let (s1, _c1) = parse_step(&steps[0]).unwrap();
        assert_eq!(s1.name, "rename");
        assert_eq!(s1.kind, EtlStepKind::Builtin);
        let (s2, c2) = parse_step(&steps[1]).unwrap();
        assert_eq!(s2.name, "xyz");
        assert_eq!(s2.kind, EtlStepKind::Plugin);
        assert_eq!(c2["k"].as_str(), Some("v"));
        let (s3, _) = parse_step(&steps[2]).unwrap();
        assert_eq!(s3.name, "filter");
        assert_eq!(s3.kind, EtlStepKind::Builtin);
    }

    #[test]
    fn expand_env_works() {
        // SAFETY: test-only unique var.
        unsafe { std::env::set_var("DT_CFG_PW", "sekret") };
        assert_eq!(expand_env("${DT_CFG_PW}"), "sekret");
        assert_eq!(expand_env("${env.DT_CFG_PW}"), "sekret");
        // unknown vars are left untouched
        assert_eq!(expand_env("x${NOT_SET_ANYWHERE}"), "x${NOT_SET_ANYWHERE}");
    }

    #[test]
    fn csv_split() {
        assert_eq!(split_csv(Some(" id , name ,")), vec!["id", "name"]);
        assert_eq!(split_csv(None), Vec::<String>::new());
    }
}