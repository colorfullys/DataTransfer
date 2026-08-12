//! Generic C ABI glue compiled into *plugin* cdylibs. These functions
//! implement the `EtlCapi` table for a concrete processor type `T` and are
//! wired up by `export_etl_plugin!`.
//!
//! All exported functions are unsafe C ABI entry points; their safety
//! contracts (NULL handling, string ownership) mirror `ffi::EtlCapi` and are
//! enforced here.
#![allow(clippy::missing_safety_doc)]

use std::ffi::{c_char, c_void, CStr, CString};

use crate::error::{EtlError, Result};
use crate::ffi::{to_string, CLookupFn, C_OK, CProcPtr, CStrOut, CStrPtr};
use crate::model::EtlInput;
use crate::trait_def::{EtlConfigure, EtlContext, EtlProcessor, TableLookup};
use libdatasource::model::Row;

pub unsafe extern "C" fn etl_create<T: Default>() -> CProcPtr {
    Box::into_raw(Box::<T>::default()) as CProcPtr
}

pub unsafe extern "C" fn etl_destroy<T>(ptr: CProcPtr) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr as *mut T) });
    }
}

pub unsafe extern "C" fn etl_configure<T: EtlConfigure>(
    ptr: CProcPtr,
    config: CStrPtr,
    out: CStrOut,
) -> i32 {
    let s = match unsafe { to_string(config) } {
        Ok(s) => s,
        Err(e) => {
            unsafe { crate::ffi::set_out_err(out, &e) };
            return 1;
        }
    };
    let cfg: serde_json::Value = serde_json::from_str(&s).unwrap_or(serde_json::Value::Null);
    let proc = unsafe { &mut *(ptr as *mut T) };
    match proc.configure(&cfg) {
        Ok(()) => C_OK,
        Err(e) => {
            unsafe { crate::ffi::set_out_err(out, &e) };
            1
        }
    }
}

pub unsafe extern "C" fn etl_process<T: EtlProcessor>(
    ptr: CProcPtr,
    lookup_fn: CLookupFn,
    lookup_ctx: *mut c_void,
    input: CStrPtr,
    out: CStrOut,
) -> i32 {
    let s = match unsafe { to_string(input) } {
        Ok(s) => s,
        Err(e) => {
            unsafe { crate::ffi::set_out_err(out, &e) };
            return 1;
        }
    };
    let input: EtlInput = match serde_json::from_str(&s) {
        Ok(v) => v,
        Err(e) => {
            let err = EtlError::Serialize(e.to_string());
            unsafe { crate::ffi::set_out_err(out, &err) };
            return 1;
        }
    };
    let row = input.to_etl_row();
    let cb = CbLookup {
        f: lookup_fn,
        ctx: lookup_ctx,
    };
    let mut ctx = EtlContext::from_row(&row, &cb);
    let proc = unsafe { &*(ptr as *const T) };
    let result = proc.process(&mut ctx, &row);
    let json = match result {
        Ok(rows) => match serde_json::to_string(&rows) {
            Ok(j) => j,
            Err(e) => {
                let err = EtlError::Serialize(e.to_string());
                unsafe { crate::ffi::set_out_err(out, &err) };
                return 1;
            }
        },
        Err(e) => {
            unsafe { crate::ffi::set_out_err(out, &e) };
            return 1;
        }
    };
    match CString::new(json) {
        Ok(c) => {
            unsafe { *out = c.into_raw() };
            C_OK
        }
        Err(_) => 1,
    }
}

pub unsafe extern "C" fn etl_free_string(ptr: CStrPtr) {
    if !ptr.is_null() {
        unsafe { drop(CString::from_raw(ptr as *mut c_char)) };
    }
}

/// A `TableLookup` that forwards to the host over the C callback. Compiled
/// into both host (DataTransfer) and plugin builds of LibETL; only the host
/// knows the concrete type behind `ctx`.
pub struct CbLookup {
    pub f: CLookupFn,
    pub ctx: *mut c_void,
}

impl CbLookup {
    fn cstr_opt(s: Option<&str>) -> CString {
        CString::new(s.unwrap_or("null")).unwrap_or_else(|_| CString::new("null").unwrap())
    }
}

impl TableLookup for CbLookup {
    fn lookup(
        &self,
        connection: &str,
        table: &str,
        columns: Option<&[String]>,
        where_clause: Option<&str>,
        params: &[libdatasource::model::Value],
        limit: u64,
    ) -> Result<Vec<Row>> {
        let conn = match CString::new(connection) {
            Ok(c) => c,
            Err(e) => return Err(EtlError::Lookup(e.to_string())),
        };
        let table_c = match CString::new(table) {
            Ok(c) => c,
            Err(e) => return Err(EtlError::Lookup(e.to_string())),
        };
        let cols = match columns {
            Some(cs) => serde_json::to_string(cs).map_err(|e| EtlError::Serialize(e.to_string()))?,
            None => "null".into(),
        };
        let cols_c = Self::cstr_opt(Some(&cols));
        let where_c = Self::cstr_opt(where_clause);
        let params_json =
            serde_json::to_string(params).map_err(|e| EtlError::Serialize(e.to_string()))?;
        let params_c = Self::cstr_opt(Some(&params_json));

        let mut out: *const c_char = std::ptr::null();
        let rc = unsafe {
            (self.f)(
                self.ctx,
                conn.as_ptr(),
                table_c.as_ptr(),
                cols_c.as_ptr(),
                where_c.as_ptr(),
                params_c.as_ptr(),
                limit,
                &mut out,
            )
        };
        if rc != C_OK {
            let msg = if out.is_null() {
                "unknown lookup error".into()
            } else {
                let s = unsafe { CStr::from_ptr(out) }.to_string_lossy().into_owned();
                if s.is_empty() {
                    "unknown lookup error".into()
                } else {
                    s
                }
            };
            return Err(EtlError::Lookup(msg));
        }
        if out.is_null() {
            return Err(EtlError::Lookup("host returned NULL result".into()));
        }
        let s = unsafe { CStr::from_ptr(out) }.to_string_lossy().into_owned();
        serde_json::from_str(&s).map_err(|e| EtlError::Serialize(e.to_string()))
    }
}
