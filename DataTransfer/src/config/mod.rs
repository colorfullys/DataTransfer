//! Configuration loading for `config.yaml`, `datasource.yaml` and job files.
//!
//! YAML is parsed with `yaml-rust2`; hierarchies are converted by hand (no
//! serde-yaml dependency). Secrets like `${MY_PASSWORD}` are expanded from the
//! process environment at load time so they never end up in state or logs.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use libdatasource::model::ConnectionConfig;
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

fn resolve_relative(base: &Path, p: &str) -> String {
    let pb = PathBuf::from(p);
    if pb.is_absolute() {
        return p.to_string();
    }
    base.join(pb).to_string_lossy().to_string()
}

mod job;

pub use job::{EtlStepKind, JobConfig, SourceConfig, StateSpec};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_env_works() {
        // SAFETY: test-only unique var.
        unsafe { std::env::set_var("DT_CFG_PW", "sekret") };
        assert_eq!(expand_env("${DT_CFG_PW}"), "sekret");
        assert_eq!(expand_env("${env.DT_CFG_PW}"), "sekret");
        // unknown vars are left untouched
        assert_eq!(expand_env("x${NOT_SET_ANYWHERE}"), "x${NOT_SET_ANYWHERE}");
    }
}
