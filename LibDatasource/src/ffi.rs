//! C ABI contract shared by every datasource plugin.
//!
//! Both the plugin cdylibs and the DataTransfer host link against this crate
//! (the SDK). The host loads a plugin with `libloading`, resolves the single
//! exported symbol `ds_get_api` and obtains a `&'static DatasourceCapi` whose
//! layout is defined here. All payloads cross the boundary as NUL-terminated
//! JSON strings; ownership of every out string belongs to the plugin and must
//! be released through `free_string`.

use std::ffi::{c_char, c_int, c_uint, c_void, CStr, CString};

use crate::error::{DatasourceError, Result};

/// Return code convention: `0` success, any non-zero value failure.
/// On failure the plugin MUST write a JSON error object into `out_err`.
pub const C_OK: c_int = 0;
pub const C_ERR: c_int = 1;

/// Version of the ABI. Bump only for breaking layout changes.
pub const ABI_VERSION: c_uint = 1;

/// Alias to keep the `extern "C"` signatures readable.
pub type CStrPtr = *const c_char;
pub type CInstPtr = *mut c_void;
pub type CStrOut = *mut CStrPtr;
pub type CRowsOut = *mut u64;

/// The function pointer table a datasource plugin must provide.
#[repr(C)]
pub struct DatasourceCapi {
    pub version: c_uint,
    /// Static, NUL-terminated datasource type name (`mysql`, `postgresql`, ...).
    pub name: unsafe extern "C" fn() -> CStrPtr,
    /// Create an instance (initially not connected).
    pub create: unsafe extern "C" fn() -> CInstPtr,
    /// Destroy an instance created by `create`.
    pub destroy: unsafe extern "C" fn(CInstPtr),
    /// Connect to the database described by the JSON `ConnectionConfig`.
    pub connect: unsafe extern "C" fn(CInstPtr, CStrPtr, CStrOut) -> c_int,
    /// Read table structure. `out` receives the JSON `TableSchema`.
    pub get_schema: unsafe extern "C" fn(CInstPtr, CStrPtr, CStrOut) -> c_int,
    /// Execute a read-only SQL. `out` receives the JSON `Vec<Row>`.
    pub query: unsafe extern "C" fn(CInstPtr, CStrPtr, CStrOut) -> c_int,
    /// Execute a read-only SQL with offset/limit paging.
    pub query_page: unsafe extern "C" fn(CInstPtr, CStrPtr, u64, u64, CStrOut) -> c_int,
    /// Execute a read-only SQL with JSON `Vec<Value>` positional params.
    pub query_params: unsafe extern "C" fn(CInstPtr, CStrPtr, CStrPtr, CStrOut) -> c_int,
    /// Execute a write SQL with JSON `Vec<Row>` params. `affected` receives rows.
    pub execute: unsafe extern "C" fn(CInstPtr, CStrPtr, CStrPtr, CRowsOut, CStrOut) -> c_int,
    /// Batch insert. Args: table, JSON columns, JSON rows, mode, JSON pk columns.
    pub batch_insert: unsafe extern "C" fn(
        CInstPtr,
        CStrPtr,
        CStrPtr,
        CStrPtr,
        CStrPtr,
        CStrPtr,
        CRowsOut,
        CStrOut,
    ) -> c_int,
    /// Remove all rows of a table.
    pub truncate: unsafe extern "C" fn(CInstPtr, CStrPtr, CStrOut) -> c_int,
    /// Liveness check.
    pub ping: unsafe extern "C" fn(CInstPtr, CStrOut) -> c_int,
    /// Release a string previously returned through a `CStrOut`.
    pub free_string: unsafe extern "C" fn(CStrPtr),
}

// ---------------------------------------------------------------------------
// Helpers shared by plugin implementations
// ---------------------------------------------------------------------------

/// Convert a C string argument into an owned Rust string.
///
/// # Safety
///
/// `ptr` must either be NULL or point to a NUL-terminated string that is valid
/// for reads for the whole call.
pub unsafe fn c_to_string(ptr: CStrPtr) -> Result<String> {
    if ptr.is_null() {
        return Err(DatasourceError::InvalidArgument(
            "NULL pointer where a string was expected".into(),
        ));
    }
    let s = unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|e| DatasourceError::Serialize(e.to_string()))?;
    Ok(s.to_string())
}

/// Allocate a C string from JSON bytes; the caller owns the returned pointer.
///
/// # Safety
///
/// Returns a heap pointer the caller must release with the plugin's
/// `free_string`. The returned pointer is valid for the lifetime of the
/// process, or until freed.
pub unsafe fn alloc_string(s: &str) -> CStrPtr {
    match CString::new(s) {
        Ok(c) => c.into_raw(),
        Err(_) => {
            // Cannot contain interior NUL from serde_json output; defensive.
            let fallback = CString::new("\"internal error: interior NUL\"").unwrap();
            fallback.into_raw()
        }
    }
}

/// Write an error message into an out parameter.
///
/// # Safety
///
/// `out_err` must be NULL or point to a writable `CStrPtr` slot.
pub unsafe fn set_out_err(out_err: CStrOut, err: &DatasourceError) {
    if out_err.is_null() {
        return;
    }
    let json = serde_json::json!({ "error": err.to_string() }).to_string();
    unsafe { *out_err = alloc_string(&json) };
}

/// Decode a JSON payload received from the host.
pub fn decode_json<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_str(s).map_err(|e| DatasourceError::Serialize(e.to_string()))
}

/// Encode a value into JSON for delivery to the host.
pub fn encode_json<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(|e| DatasourceError::Serialize(e.to_string()))
}
