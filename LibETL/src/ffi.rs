//! C ABI shared by the ETL host and any ETL plugin. Payloads cross the
//! boundary as NUL-terminated JSON strings; out strings are owned by the
//! plugin and released via `free_string`.

use std::ffi::{c_char, c_int, c_uint, c_void, CStr, CString};

use crate::error::EtlError;

pub const C_OK: i32 = 0;
pub const ABI_VERSION: c_uint = 2;

pub type CProcPtr = *mut c_void;
pub type CStrPtr = *const c_char;
pub type CStrOut = *mut CStrPtr;

/// Host-provided `TableLookup` callback. The plugin forwards opaque args
/// (`lookup_ctx`) back to the host; only the host knows the concrete type.
/// Columns / where-clause / params are JSON: `null`, `["a","b"]`, `"..."`.
pub type CLookupFn = unsafe extern "C" fn(
    lookup_ctx: *mut c_void,
    conn: CStrPtr,
    table: CStrPtr,
    columns: CStrPtr,
    where_clause: CStrPtr,
    params: CStrPtr,
    limit: u64,
    out: CStrOut,
) -> i32;

/// Host-provided log callback (`log::Level` as `c_int`, NUL-terminated message).
/// Lets a plugin emit diagnostics through the host logger (plugin-side
/// `log::info!`/`warn!`/... normally cannot reach it because each cdylib
/// statically links its own `log` crate).
pub type CLogFn = unsafe extern "C" fn(log_ctx: *mut c_void, level: c_int, msg: CStrPtr);

/// Function pointer table an ETL plugin provides.
#[repr(C)]
pub struct EtlCapi {
    pub version: c_uint,
    /// Static plugin name.
    pub name: unsafe extern "C" fn() -> CStrPtr,
    /// Create a processor instance.
    pub create: unsafe extern "C" fn() -> CProcPtr,
    /// Destroy an instance.
    pub destroy: unsafe extern "C" fn(CProcPtr),
    /// Configure. `config` is the JSON blob from the job's `etl` block.
    /// `log_fn`/`log_ctx` expose the host logger so the plugin can emit logs.
    pub configure: unsafe extern "C" fn(CProcPtr, CStrPtr, CLogFn, *mut c_void, CStrOut) -> i32,
    /// Transform one row. `input` is JSON `EtlInput`, `out` receives JSON `Vec<EtlOutputRow>`.
    /// `lookup_fn`/`lookup_ctx` expose the host `TableLookup` for cross-table reads,
    /// `log_fn`/`log_ctx` expose the host logger.
    pub process: unsafe extern "C" fn(CProcPtr, CLookupFn, *mut c_void, CLogFn, *mut c_void, CStrPtr, CStrOut) -> i32,
    /// Free a string produced by this plugin.
    pub free_string: unsafe extern "C" fn(CStrPtr),
}

/// Convert a C string into an owned Rust string.
///
/// # Safety
///
/// `ptr` must be NULL or a NUL-terminated string valid for reads for the call.
pub unsafe fn to_string(ptr: CStrPtr) -> crate::error::Result<String> {
    if ptr.is_null() {
        return Err(EtlError::Processing("NULL pointer".into()));
    }
    Ok(unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|e| EtlError::Serialize(e.to_string()))?
        .to_string())
}

/// Allocate a C string the caller releases via `free_string`.
///
/// # Safety
///
/// Returns a heap pointer that must eventually be passed to `free_string`.
pub unsafe fn alloc_string(s: &str) -> CStrPtr {
    match CString::new(s) {
        Ok(c) => c.into_raw(),
        Err(_) => CString::new("\"internal error\"").unwrap().into_raw(),
    }
}

/// Write an error message into an out slot.
///
/// # Safety
///
/// `out` must be NULL or a writable `CStrPtr` slot.
pub unsafe fn set_out_err(out: CStrOut, err: &EtlError) {
    if out.is_null() {
        return;
    }
    let json = serde_json::json!({ "error": err.to_string() }).to_string();
    unsafe { *out = alloc_string(&json) };
}
