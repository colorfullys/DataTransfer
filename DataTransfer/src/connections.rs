//! Holds the datasource plugin registry and one connected instance per named
//! connection from `datasource.yaml`.

use std::collections::HashMap;
use std::sync::Arc;

use libdatasource::datasource::Datasource;
use libdatasource::plugin_registry::PluginRegistry;

use crate::config::{expand_env, AppConfig, ConnectionSettings};
use crate::error::{AppError, AppResult};

pub struct ConnectionManager {
    plugins: PluginRegistry,
    settings: HashMap<String, ConnectionSettings>,
    conns: HashMap<String, Arc<dyn Datasource>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        ConnectionManager {
            plugins: PluginRegistry::new(),
            settings: HashMap::new(),
            conns: HashMap::new(),
        }
    }

    /// Load every datasource plugin listed in `config.yaml`.
    pub fn load_plugins(&mut self, cfg: &AppConfig) -> AppResult<()> {
        for (plugin_type, path) in &cfg.datasource_plugins {
            match self.plugins.load_file(std::path::Path::new(path)) {
                Ok(name) => {
                    if &name != plugin_type {
                        log::warn!(
                            "plugin '{}' declares name '{}' but is configured as '{}'",
                            path,
                            name,
                            plugin_type
                        );
                    }
                }
                Err(e) => {
                    return Err(AppError::Config(format!(
                        "datasource plugin '{plugin_type}' ({path}): {e}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Connect every connection. Fails fast on the first connection error so
    /// misconfiguration is visible at startup.
    pub fn connect_all(
        &mut self,
        connections: &std::collections::BTreeMap<String, ConnectionSettings>,
    ) -> AppResult<()> {
        for (name, settings) in connections {
            let mut settings = settings.clone();
            settings.password = expand_env(&settings.password);
            for v in settings.params.values_mut() {
                *v = expand_env(v);
            }
            let ds = self.connect_one(&settings)?;
            log::info!(
                "connected '{}' ({}@{})",
                name,
                settings.conn_type,
                settings.host
            );
            self.settings.insert(name.clone(), settings);
            self.conns.insert(name.clone(), ds);
        }
        Ok(())
    }

    fn connect_one(&self, settings: &ConnectionSettings) -> AppResult<Arc<dyn Datasource>> {
        let cfg = settings.to_connection_config();
        self.plugins
            .create_datasource(&cfg)
            .map_err(|e| AppError::Datasource(format!("connect '{}': {e}", settings.name)))
    }

    pub fn get(&self, name: &str) -> AppResult<Arc<dyn Datasource>> {
        self.conns
            .get(name)
            .cloned()
            .ok_or_else(|| AppError::Config(format!("connection '{name}' is not connected")))
    }

    /// Datasource type (`mysql`, `postgresql`, `oracle`, ...) for a connection.
    pub fn conn_type(&self, name: &str) -> AppResult<&str> {
        self.settings
            .get(name)
            .map(|s| s.conn_type.as_str())
            .ok_or_else(|| AppError::Config(format!("connection '{name}' is not connected")))
    }

    pub fn names(&self) -> Vec<String> {
        self.conns.keys().cloned().collect()
    }
}