//! Host-side loader and registry for dynamic ETL plugin cdylibs.

use std::ffi::{c_char, c_void, CStr, CString};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use libloading::Library;

use crate::error::{EtlError, Result};
use crate::ffi::{CLookupFn, EtlCapi, C_OK};
use crate::model::{EtlInput, EtlOutputRow};
use crate::plugin_api::CbLookup;
use crate::trait_def::{EtlContext, EtlProcessor};

/// Registry of loaded ETL plugin libraries, keyed by declared name.
#[derive(Default)]
pub struct EtlRegistry {
    plugins: std::collections::HashMap<String, Arc<EtlHandle>>,
}

impl EtlRegistry {
    pub fn new() -> Self {
        EtlRegistry {
            plugins: std::collections::HashMap::new(),
        }
    }

    pub fn load_file(&mut self, path: &Path) -> Result<String> {
        let resolved = resolve_etl_path(path).ok_or_else(|| {
            EtlError::Config(format!("etl plugin not found: {}", path.display()))
        })?;
        let (name, handle) = unsafe { load_etl(&resolved)? };
        if self.plugins.contains_key(&name) {
            log::warn!("etl plugin '{}' already registered, ignoring {}", name, resolved.display());
            return Ok(name);
        }
        self.plugins.insert(name.clone(), Arc::new(handle));
        log::info!("loaded etl plugin '{}' from {}", name, resolved.display());
        Ok(name)
    }

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
        for e in entries {
            if let Ok(name) = self.load_file(&e) {
                loaded.push(name);
            }
        }
        Ok(loaded)
    }

    pub fn get(&self, name: &str) -> Result<Arc<EtlHandle>> {
        self.plugins
            .get(name)
            .cloned()
            .ok_or_else(|| EtlError::Config(format!("no etl plugin named '{name}' loaded")))
    }

    pub fn names(&self) -> Vec<String> {
        self.plugins.keys().cloned().collect()
    }
}

/// A loaded ETL plugin, clonable so many jobs can share it. Instances are
/// created per job so configuration and execution are independent.
pub struct EtlHandle {
    name: String,
    library: Arc<Library>,
    capi: &'static EtlCapi,
}

impl EtlHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn instantiate(
        &self,
        config: &serde_json::Value,
        lookup_fn: CLookupFn,
        lookup_ctx: *mut c_void,
    ) -> Result<Arc<dyn EtlProcessor>> {
        let capi = self.capi;
        let proc = unsafe { (capi.create)() };
        if proc.is_null() {
            return Err(EtlError::Config("etl plugin create failed".into()));
        }

        if !config.is_null() && *config != serde_json::Value::Null {
            let cfg_json = config.to_string();
            let cfg_c = CString::new(cfg_json).map_err(|e| EtlError::Config(e.to_string()))?;
            let mut err: *const c_char = std::ptr::null();
            let rc = unsafe { (capi.configure)(proc, cfg_c.as_ptr(), &mut err) };
            if rc != C_OK {
                let msg = read_err(err);
                unsafe { (capi.destroy)(proc) };
                return Err(EtlError::Config(format!(
                    "etl plugin '{}' configure failed: {}",
                    self.name, msg
                )));
            }
            unsafety_free(err, capi);
        }

        Ok(Arc::new(LoadedProcessor {
            _library: Arc::clone(&self.library),
            name: Box::leak(self.name.clone().into_boxed_str()),
            capi,
            proc,
            lookup: CbLookup {
                f: lookup_fn,
                ctx: lookup_ctx,
            },
        }))
    }
}

struct LoadedProcessor {
    _library: Arc<Library>,
    name: &'static str,
    capi: &'static EtlCapi,
    proc: *mut std::ffi::c_void,
    lookup: CbLookup,
}

unsafe impl Send for LoadedProcessor {}
unsafe impl Sync for LoadedProcessor {}

impl Drop for LoadedProcessor {
    fn drop(&mut self) {
        unsafe { (self.capi.destroy)(self.proc) };
    }
}

impl EtlProcessor for LoadedProcessor {
    fn name(&self) -> &str {
        self.name
    }

    fn process(&self, _ctx: &mut EtlContext, input: &crate::model::EtlRow) -> Result<Vec<EtlOutputRow>> {
        let input = EtlInput::from(input.clone());
        let json = serde_json::to_string(&input)?;
        let json_c = CString::new(json).map_err(|e| EtlError::Serialize(e.to_string()))?;

        let mut out: *const c_char = std::ptr::null();
        let rc = unsafe {
            (self.capi.process)(self.proc, self.lookup.f, self.lookup.ctx, json_c.as_ptr(), &mut out)
        };
        if rc != C_OK {
            let msg = read_err(out);
            unsafe { (self.capi.free_string)(out) };
            return Err(EtlError::Processing(format!(
                "etl plugin '{}' failed: {}",
                self.name, msg
            )));
        }
        if out.is_null() {
            return Err(EtlError::Processing("etl plugin returned NULL result".into()));
        }
        let out_str = unsafe { CStr::from_ptr(out) }.to_string_lossy().into_owned();
        unsafe { (self.capi.free_string)(out) };
        serde_json::from_str(&out_str).map_err(|e| EtlError::Serialize(e.to_string()))
    }
}

fn is_plugin_file(p: &Path) -> bool {
    let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
    name.starts_with("lib") && (name.ends_with(".so") || name.ends_with(".dylib") || name.ends_with(".dll"))
}

fn resolve_etl_path(path: &Path) -> Option<PathBuf> {
    if path.is_file() {
        return Some(path.to_path_buf());
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let stem = path.file_stem().and_then(|s| s.to_str())?;
    let stem = stem.trim_start_matches("lib");
    for ext in ["so", "dylib", "dll"] {
        let cand = parent.join(format!("lib{stem}.{ext}"));
        if cand.is_file() {
            return Some(cand);
        }
    }
    None
}

unsafe fn load_etl(path: &Path) -> Result<(String, EtlHandle)> {
    let lib = unsafe { Library::new(path) }
        .map_err(|e| EtlError::Config(format!("cannot open etl plugin {}: {e}", path.display())))?;

    let get_api: libloading::Symbol<unsafe extern "C" fn() -> *const EtlCapi> =
        unsafe { lib.get(b"etl_get_api") }
            .map_err(|e| EtlError::Config(format!("{} is not an etl plugin: {e}", path.display())))?;

    let capi_ptr = unsafe { get_api() };
    if capi_ptr.is_null() {
        return Err(EtlError::Config(format!("{}: etl_get_api NULL", path.display())));
    }
    let capi = unsafe { &*capi_ptr };
    if capi.version != crate::ffi::ABI_VERSION {
        return Err(EtlError::Config(format!(
            "{}: etl ABI mismatch ({} vs {})",
            path.display(),
            capi.version,
            crate::ffi::ABI_VERSION
        )));
    }
    let name = unsafe { CStr::from_ptr((capi.name)()) }.to_string_lossy().into_owned();
    let _ = get_api;
    Ok((
        name.clone(),
        EtlHandle {
            name,
            library: Arc::new(lib),
            capi,
        },
    ))
}

fn read_err(ptr: *const c_char) -> String {
    if ptr.is_null() {
        return "unknown error".into();
    }
    let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
    if s.is_empty() { "unknown error".into() } else { s }
}

fn unsafety_free(ptr: *const c_char, capi: &'static EtlCapi) {
    if !ptr.is_null() {
        unsafe { (capi.free_string)(ptr) };
    }
}
