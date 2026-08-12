//! Host-side plugin loading using `libloading`.
//!
//! `PluginRegistry` loads datasource cdylibs, verifies the ABI version and
//! hands out independent connection instances (`Arc<dyn Datasource>`).

use std::collections::HashMap;
use std::ffi::{c_char, CStr, CString};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use libloading::Library;

use crate::datasource::Datasource;
use crate::error::{DatasourceError, Result};
use crate::ffi::{C_OK, DatasourceCapi};
use crate::model::ConnectionConfig;

/// Registry of loaded datasource plugin libraries.
#[derive(Default)]
pub struct PluginRegistry {
    plugins: HashMap<String, PluginHandle>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        PluginRegistry {
            plugins: HashMap::new(),
        }
    }

    /// Load one plugin file. The plugin is keyed by the name it declares
    /// (`mysql`, `postgresql`, ...) and must be unique.
    pub fn load_file(&mut self, path: &Path) -> Result<String> {
        let resolved = resolve_plugin_path(path).ok_or_else(|| {
            DatasourceError::Connection(format!(
                "datasource plugin not found: {}",
                path.display()
            ))
        })?;

        let loaded = unsafe { load_capi(&resolved)? };
        let name = loaded.name.clone();
        if self.plugins.contains_key(&name) {
            log::warn!(
                "datasource plugin '{}' already registered, ignoring duplicate from {}",
                name,
                resolved.display()
            );
            return Ok(name);
        }
        log::info!("loaded datasource plugin '{}' from {}", name, resolved.display());
        let handle = PluginHandle {
            name: name.clone(),
            library: loaded.library.clone(),
            capi: loaded.capi,
        };
        self.plugins.insert(name.clone(), handle);
        Ok(name)
    }

    /// Scan a directory and load every matching plugin file.
    pub fn load_dir(&mut self, dir: &Path) -> Result<Vec<String>> {
        let mut loaded = Vec::new();
        if !dir.is_dir() {
            return Ok(loaded);
        }
        let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| is_plugin_file(p))
            .collect();
        entries.sort();
        for entry in entries {
            if let Ok(name) = self.load_file(&entry) {
                loaded.push(name);
            }
        }
        Ok(loaded)
    }

    /// Look up a loaded plugin by datasource type name.
    pub fn get(&self, conn_type: &str) -> Result<PluginHandle> {
        self.plugins
            .get(conn_type)
            .cloned()
            .ok_or_else(|| {
                DatasourceError::Connection(format!(
                    "no datasource plugin registered for type '{}' (loaded: {:?})",
                    conn_type,
                    self.plugins.keys().collect::<Vec<_>>()
                ))
            })
    }

    pub fn names(&self) -> Vec<String> {
        self.plugins.keys().cloned().collect()
    }

    /// Create and connect a new datasource instance for `cfg`.
    pub fn create_datasource(&self, cfg: &ConnectionConfig) -> Result<Arc<dyn Datasource>> {
        let handle = self.get(&cfg.conn_type)?;
        handle.create_datasource(cfg)
    }
}

/// A loaded plugin library, clonable so many jobs can share it.
#[derive(Clone)]
pub struct PluginHandle {
    name: String,
    library: Arc<Library>,
    capi: &'static DatasourceCapi,
}

impl PluginHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn create_datasource(&self, cfg: &ConnectionConfig) -> Result<Arc<dyn Datasource>> {
        let capi = self.capi;
        let instance = unsafe { (capi.create)() };
        if instance.is_null() {
            return Err(DatasourceError::Connection(format!(
                "plugin '{}' failed to create instance",
                self.name
            )));
        }

        let cfg_json = serde_json::to_string(cfg)?;
        let cfg_c = CString::new(cfg_json).map_err(|e| {
            DatasourceError::InvalidArgument(format!("connection config: {e}"))
        })?;

        let mut err_ptr: *const c_char = std::ptr::null();
        let rc = unsafe { (capi.connect)(instance, cfg_c.as_ptr(), &mut err_ptr) };
        if rc != C_OK {
            let msg = unsafe { read_out_err(err_ptr, capi) };
            unsafe { (capi.destroy)(instance) };
            return Err(DatasourceError::Connection(format!(
                "connect to '{}' failed: {}",
                cfg.name, msg
            )));
        }
        unsafe { free_out_err(err_ptr, capi) };

        Ok(Arc::new(LoadedDatasource {
            _library: Arc::clone(&self.library),
            name: leak_str(&self.name),
            capi,
            instance,
        }))
    }
}

/// Host-side wrapper implementing `Datasource` over the C ABI.
pub struct LoadedDatasource {
    /// Keeps the plugin library alive for as long as this instance lives.
    _library: Arc<Library>,
    name: &'static str,
    capi: &'static DatasourceCapi,
    instance: *mut std::ffi::c_void,
}

// The plugin instance is internally guarded by a `Mutex`, so sharing the raw
// pointer across threads is safe.
unsafe impl Send for LoadedDatasource {}
unsafe impl Sync for LoadedDatasource {}

impl Drop for LoadedDatasource {
    fn drop(&mut self) {
        unsafe { (self.capi.destroy)(self.instance) };
    }
}

impl Datasource for LoadedDatasource {
    fn name(&self) -> &'static str {
        self.name
    }

    fn connect(&mut self, _cfg: &ConnectionConfig) -> Result<()> {
        Err(DatasourceError::Unsupported(
            "LoadedDatasource is already connected".into(),
        ))
    }

    fn get_schema(&self, table: &str) -> Result<crate::model::TableSchema> {
        let table_c = CString::new(table).map_err(|e| DatasourceError::InvalidArgument(e.to_string()))?;
        let mut out: *const c_char = std::ptr::null();
        let rc = unsafe { (self.capi.get_schema)(self.instance, table_c.as_ptr(), &mut out) };
        self.read_out::<crate::model::TableSchema>(rc, out)
    }

    fn query(&self, sql: &str) -> Result<Vec<crate::model::Row>> {
        let sql_c = CString::new(sql).map_err(|e| DatasourceError::InvalidArgument(e.to_string()))?;
        let mut out: *const c_char = std::ptr::null();
        let rc = unsafe { (self.capi.query)(self.instance, sql_c.as_ptr(), &mut out) };
        self.read_out::<Vec<crate::model::Row>>(rc, out)
    }

    fn query_page(&self, sql: &str, offset: u64, limit: u64) -> Result<Vec<crate::model::Row>> {
        let sql_c = CString::new(sql).map_err(|e| DatasourceError::InvalidArgument(e.to_string()))?;
        let mut out: *const c_char = std::ptr::null();
        let rc = unsafe {
            (self.capi.query_page)(self.instance, sql_c.as_ptr(), offset, limit, &mut out)
        };
        self.read_out::<Vec<crate::model::Row>>(rc, out)
    }

    fn query_params(&self, sql: &str, params: &[crate::model::Value]) -> Result<Vec<crate::model::Row>> {
        let sql_c = CString::new(sql).map_err(|e| DatasourceError::InvalidArgument(e.to_string()))?;
        let params_json = serde_json::to_string(params)?;
        let params_c = CString::new(params_json).map_err(|e| DatasourceError::Serialize(e.to_string()))?;
        let mut out: *const c_char = std::ptr::null();
        let rc = unsafe {
            (self.capi.query_params)(self.instance, sql_c.as_ptr(), params_c.as_ptr(), &mut out)
        };
        self.read_out::<Vec<crate::model::Row>>(rc, out)
    }

    fn execute(&self, sql: &str, params: &[crate::model::Value]) -> Result<u64> {
        let sql_c = CString::new(sql).map_err(|e| DatasourceError::InvalidArgument(e.to_string()))?;
        let params_json = serde_json::to_string(params)?;
        let params_c = CString::new(params_json).map_err(|e| DatasourceError::Serialize(e.to_string()))?;
        let mut affected: u64 = 0;
        let mut out: *const c_char = std::ptr::null();
        let rc = unsafe {
            (self.capi.execute)(
                self.instance,
                sql_c.as_ptr(),
                params_c.as_ptr(),
                &mut affected,
                &mut out,
            )
        };
        self.check(rc, out)?;
        Ok(affected)
    }

    fn batch_insert(
        &self,
        table: &str,
        columns: &[String],
        rows: &[Vec<crate::model::Value>],
        mode: crate::model::SyncMode,
        pk_columns: &[String],
    ) -> Result<u64> {
        let table_c = CString::new(table).map_err(|e| DatasourceError::InvalidArgument(e.to_string()))?;
        let columns_c = CString::new(serde_json::to_string(columns)?)
            .map_err(|e| DatasourceError::Serialize(e.to_string()))?;
        let rows_c = CString::new(serde_json::to_string(rows)?)
            .map_err(|e| DatasourceError::Serialize(e.to_string()))?;
        let mode_c = CString::new(mode.as_str()).unwrap();
        let pk_c = CString::new(serde_json::to_string(pk_columns)?)
            .map_err(|e| DatasourceError::Serialize(e.to_string()))?;

        let mut affected: u64 = 0;
        let mut out: *const c_char = std::ptr::null();
        let rc = unsafe {
            (self.capi.batch_insert)(
                self.instance,
                table_c.as_ptr(),
                columns_c.as_ptr(),
                rows_c.as_ptr(),
                mode_c.as_ptr(),
                pk_c.as_ptr(),
                &mut affected,
                &mut out,
            )
        };
        self.check(rc, out)?;
        Ok(affected)
    }

    fn truncate(&self, table: &str) -> Result<()> {
        let table_c = CString::new(table).map_err(|e| DatasourceError::InvalidArgument(e.to_string()))?;
        let mut out: *const c_char = std::ptr::null();
        let rc = unsafe { (self.capi.truncate)(self.instance, table_c.as_ptr(), &mut out) };
        self.check(rc, out)
    }

    fn ping(&self) -> Result<()> {
        let mut out: *const c_char = std::ptr::null();
        let rc = unsafe { (self.capi.ping)(self.instance, &mut out) };
        self.check(rc, out)
    }
}

impl LoadedDatasource {
    /// Free an out error pointer if present.
    fn free_out(&self, ptr: *const c_char) {
        if !ptr.is_null() {
            unsafe { (self.capi.free_string)(ptr) };
        }
    }

    /// Verify a return code and free the out pointer; produce an error if failed.
    fn check(&self, rc: i32, out: *const c_char) -> Result<()> {
        if rc != C_OK {
            let msg = unsafe { read_out_err(out, self.capi) };
            self.free_out(out);
            Err(DatasourceError::Database(msg))
        } else {
            self.free_out(out);
            Ok(())
        }
    }

    /// Verify a return code and deserialize the returned JSON payload.
    fn read_out<T: serde::de::DeserializeOwned>(&self, rc: i32, out: *const c_char) -> Result<T> {
        if rc != C_OK {
            let msg = unsafe { read_out_err(out, self.capi) };
            self.free_out(out);
            return Err(DatasourceError::Database(msg));
        }
        if out.is_null() {
            self.free_out(out);
            return Err(DatasourceError::Serialize("NULL result".into()));
        }
        let s = unsafe { CStr::from_ptr(out) }
            .to_str()
            .map_err(|e| DatasourceError::Serialize(e.to_string()))?
            .to_string();
        self.free_out(out);
        serde_json::from_str(&s).map_err(|e| DatasourceError::Serialize(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn is_plugin_file(p: &Path) -> bool {
    let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
    name.starts_with("lib") && name.ends_with(".so")
        || name.ends_with(".dylib")
        || name.ends_with(".dll")
}

/// Resolve a plugin path from configuration, trying common suffixes.
pub fn resolve_plugin_path(path: &Path) -> Option<PathBuf> {
    if path.is_file() {
        return Some(path.to_path_buf());
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let stem = path.file_stem().and_then(|s| s.to_str())?;
    let mut candidates: Vec<PathBuf> = Vec::new();
    for ext in ["so", "dylib", "dll"] {
        candidates.push(parent.join(format!("{stem}.{ext}")));
        candidates.push(parent.join(format!("lib{stem}.{ext}")));
    }
    candidates.into_iter().find(|c| c.is_file())
}

struct LoadedCapi {
    name: String,
    library: Arc<Library>,
    capi: &'static DatasourceCapi,
}

unsafe fn load_capi(path: &Path) -> Result<LoadedCapi> {
    let lib = unsafe { Library::new(path) }.map_err(|e| {
        DatasourceError::Connection(format!("cannot open plugin {}: {e}", path.display()))
    })?;

    let get_api: libloading::Symbol<
        unsafe extern "C" fn() -> *const DatasourceCapi,
    > = unsafe { lib.get(b"ds_get_api") }.map_err(|e| {
        DatasourceError::Connection(format!(
            "{} is not a datasource plugin (missing ds_get_api): {e}",
            path.display()
        ))
    })?;

    let capi_ptr = unsafe { get_api() };
    if capi_ptr.is_null() {
        return Err(DatasourceError::Connection(format!(
            "{}: ds_get_api returned NULL",
            path.display()
        )));
    }
    let capi = unsafe { &*capi_ptr };
    if capi.version != crate::ffi::ABI_VERSION {
        return Err(DatasourceError::Connection(format!(
            "{}: ABI version mismatch (plugin {}, host {})",
            path.display(),
            capi.version,
            crate::ffi::ABI_VERSION
        )));
    }
    let name_ptr = unsafe { (capi.name)() };
    let name = unsafe { CStr::from_ptr(name_ptr) }
        .to_str()
        .map_err(|e| DatasourceError::Serialize(e.to_string()))?
        .to_string();

    // The raw `get_api` symbol must outlive this function only; the capi
    // itself lives in the loaded code. Bind it so the borrow ends here and the
    // library stays referenced only by the Arc we return.
    let _ = get_api;
    Ok(LoadedCapi {
        name,
        library: Arc::new(lib),
        capi,
    })
}

unsafe fn read_out_err(ptr: *const c_char, _capi: &'static DatasourceCapi) -> String {
    if ptr.is_null() {
        return "unknown error".into();
    }
    let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
    if s.is_empty() {
        "unknown error".into()
    } else {
        s
    }
}

unsafe fn free_out_err(ptr: *const c_char, capi: &'static DatasourceCapi) {
    if !ptr.is_null() {
        unsafe { (capi.free_string)(ptr) };
    }
}

fn leak_str(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}
