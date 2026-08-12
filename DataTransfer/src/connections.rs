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

    /// Create and connect one datasource instance with a hard connect timeout.
    ///
    /// The call is moved to a worker thread and awaited for at most
    /// `connect_timeout_secs`. Not every driver honours its own connect
    /// timeout reliably (notably Oracle), so the host enforces it here. If the
    /// timeout fires, the worker keeps waiting in the background but startup
    /// fails immediately with a clear error.
    fn connect_one(&self, settings: &ConnectionSettings) -> AppResult<Arc<dyn Datasource>> {
        use std::sync::mpsc;
        use std::time::Duration;

        let cfg = settings.to_connection_config();
        let timeout = Duration::from_secs(cfg.connect_timeout_secs.max(1));
        let handle = self
            .plugins
            .get(&cfg.conn_type)
            .map_err(|e| AppError::Datasource(format!("connect '{}': {e}", settings.name)))?;

        log::info!(
            "connecting '{}' ({}@{}:{}, {}-s timeout)",
            settings.name,
            cfg.conn_type,
            cfg.host,
            cfg.port,
            cfg.connect_timeout_secs
        );

        let handle = handle.clone();
        let cfg_for_connect = cfg.clone();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(handle.create_datasource(&cfg_for_connect));
        });

        match rx.recv_timeout(timeout) {
            Ok(Ok(ds)) => Ok(ds),
            Ok(Err(e)) => Err(AppError::Datasource(format!(
                "connect '{}': {e}",
                settings.name
            ))),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(AppError::Datasource(format!(
                "connect '{}' ({}@{}:{}) timed out after {}s",
                settings.name,
                cfg.conn_type,
                cfg.host,
                cfg.port,
                cfg.connect_timeout_secs
            ))),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(AppError::Datasource(format!(
                    "connect '{}': worker thread panicked",
                    settings.name
                )))
            }
        }
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

    /// Connection pool size of a connection (drivers that pool their
    /// connections use this as the max concurrent connections).
    pub fn pool_size(&self, name: &str) -> Option<usize> {
        self.settings.get(name).map(|s| s.max_pool_size as usize)
    }

    pub fn names(&self) -> Vec<String> {
        self.conns.keys().cloned().collect()
    }
}