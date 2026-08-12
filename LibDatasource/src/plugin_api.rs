//! Generic, monomorphised `extern "C"` entry points used by datasource plugins.
//!
//! The `export_datasource_plugin!` macro wires these generic functions to the
//! concrete plugin type. Every instance is stored behind a `Mutex<T>` so the
//! exported functions are safe to call from several threads (one logical
//! connection may be used by several writer workers).
//!
//! Every exported function is an unsafe C ABI entry point: all safety
//! contracts (NULL handling, string ownership, JSON payload ownership) are
//! documented on the `ffi::DatasourceCapi` function pointers and enforced
//! here (NULL checks + `free_string` ownership rules).
#![allow(clippy::missing_safety_doc)]

use std::ffi::CString;
use std::sync::Mutex;
use std::sync::MutexGuard;

use crate::datasource::Datasource;
use crate::error::{DatasourceError, Result};
use crate::ffi::{self, CInstPtr, CRowsOut, CStrOut, CStrPtr};
use crate::model::{ConnectionConfig, Row, SyncMode, TableSchema, Value};

/// Type used internally behind every instance pointer.
type Instance<T> = Mutex<T>;

fn instance<T: 'static>(ptr: CInstPtr) -> Result<&'static Instance<T>> {
    if ptr.is_null() {
        return Err(DatasourceError::InvalidArgument(
            "NULL datasource instance".into(),
        ));
    }
    Ok(unsafe { &*(ptr as *const Instance<T>) })
}

fn guard<T: 'static>(ptr: CInstPtr) -> Result<MutexGuard<'static, T>> {
    instance::<T>(ptr).map(|m| m.lock().unwrap_or_else(|e| e.into_inner()))
}

/// Shared scaffold for entry points that return a serialisable value.
unsafe fn ds_call<T, R, F>(ptr: CInstPtr, out: CStrOut, f: F) -> i32
where
    T: Datasource + 'static,
    R: serde::Serialize,
    F: FnOnce(&T) -> Result<R>,
{
    let guard = match guard::<T>(ptr) {
        Ok(g) => g,
        Err(e) => {
            unsafe { ffi::set_out_err(out, &e) };
            return 1;
        }
    };
    match f(&guard) {
        Ok(v) => match ffi::encode_json(&v) {
            Ok(json) => {
                unsafe { *out = ffi::alloc_string(&json) };
                0
            }
            Err(e) => {
                unsafe { ffi::set_out_err(out, &e) };
                1
            }
        },
        Err(e) => {
            unsafe { ffi::set_out_err(out, &e) };
            1
        }
    }
}

/// Scaffold for entry points that return no payload.
unsafe fn ds_call_void<T, F>(ptr: CInstPtr, out: CStrOut, f: F) -> i32
where
    T: Datasource + 'static,
    F: FnOnce(&T) -> Result<()>,
{
    unsafe { ds_call::<T, (), _>(ptr, out, f) }
}

// ---------------------------------------------------------------------------
// create / destroy / name / free
// ---------------------------------------------------------------------------

pub unsafe extern "C" fn ds_create<T: Datasource + Default + 'static>() -> CInstPtr {
    let m = Box::new(Instance::new(T::default()));
    Box::into_raw(m) as CInstPtr
}

pub unsafe extern "C" fn ds_destroy<T: Datasource + 'static>(ptr: CInstPtr) {
    if ptr.is_null() {
        return;
    }
    let m = unsafe { Box::from_raw(ptr as *mut Instance<T>) };
    if let Ok(t) = m.lock() {
        t.close();
    }
    drop(m);
}

pub unsafe extern "C" fn ds_free_string(ptr: CStrPtr) {
    if !ptr.is_null() {
        drop(unsafe { CString::from_raw(ptr.cast_mut()) });
    }
}

// ---------------------------------------------------------------------------
// connect / schema / read
// ---------------------------------------------------------------------------

pub unsafe extern "C" fn ds_connect<T: Datasource + 'static>(
    ptr: CInstPtr,
    cfg: CStrPtr,
    out_err: CStrOut,
) -> i32 {
    let cfg = match unsafe { ffi::c_to_string(cfg) } {
        Ok(s) => s,
        Err(e) => {
            unsafe { ffi::set_out_err(out_err, &e) };
            return 1;
        }
    };
    let cfg: ConnectionConfig = match ffi::decode_json(&cfg) {
        Ok(c) => c,
        Err(e) => {
            unsafe { ffi::set_out_err(out_err, &e) };
            return 1;
        }
    };
    // NOTE: connect needs `&mut self`, so it locks the instance mutex directly
    // instead of going through `ds_call` (which would deadlock on the lock it
    // already holds).
    let mut g = match guard::<T>(ptr) {
        Ok(g) => g,
        Err(e) => {
            unsafe { ffi::set_out_err(out_err, &e) };
            return 1;
        }
    };
    match g.connect(&cfg) {
        Ok(()) => 0,
        Err(e) => {
            unsafe { ffi::set_out_err(out_err, &e) };
            1
        }
    }
}

pub unsafe extern "C" fn ds_get_schema<T: Datasource + 'static>(
    ptr: CInstPtr,
    table: CStrPtr,
    out: CStrOut,
) -> i32 {
    let table_name = match unsafe { ffi::c_to_string(table) } {
        Ok(s) => s,
        Err(e) => {
            unsafe { ffi::set_out_err(out, &e) };
            return 1;
        }
    };
    unsafe { ds_call::<T, TableSchema, _>(ptr, out, |t| t.get_schema(&table_name)) }
}

pub unsafe extern "C" fn ds_query<T: Datasource + 'static>(
    ptr: CInstPtr,
    sql: CStrPtr,
    out: CStrOut,
) -> i32 {
    let sql_owned = match unsafe { ffi::c_to_string(sql) } {
        Ok(s) => s,
        Err(e) => {
            unsafe { ffi::set_out_err(out, &e) };
            return 1;
        }
    };
    unsafe { ds_call::<T, Vec<Row>, _>(ptr, out, |t| t.query(&sql_owned)) }
}

pub unsafe extern "C" fn ds_query_page<T: Datasource + 'static>(
    ptr: CInstPtr,
    sql: CStrPtr,
    offset: u64,
    limit: u64,
    out: CStrOut,
) -> i32 {
    let sql_owned = match unsafe { ffi::c_to_string(sql) } {
        Ok(s) => s,
        Err(e) => {
            unsafe { ffi::set_out_err(out, &e) };
            return 1;
        }
    };
    unsafe { ds_call::<T, Vec<Row>, _>(ptr, out, |t| t.query_page(&sql_owned, offset, limit)) }
}

pub unsafe extern "C" fn ds_query_params<T: Datasource + 'static>(
    ptr: CInstPtr,
    sql: CStrPtr,
    params: CStrPtr,
    out: CStrOut,
) -> i32 {
    let sql_owned = match unsafe { ffi::c_to_string(sql) } {
        Ok(s) => s,
        Err(e) => {
            unsafe { ffi::set_out_err(out, &e) };
            return 1;
        }
    };
    let params = match unsafe { ffi::c_to_string(params) } {
        Ok(s) => s,
        Err(e) => {
            unsafe { ffi::set_out_err(out, &e) };
            return 1;
        }
    };
    let params: Vec<Value> = match ffi::decode_json(&params) {
        Ok(p) => p,
        Err(e) => {
            unsafe { ffi::set_out_err(out, &e) };
            return 1;
        }
    };
    unsafe { ds_call::<T, Vec<Row>, _>(ptr, out, |t| t.query_params(&sql_owned, &params)) }
}

// ---------------------------------------------------------------------------
// write
// ---------------------------------------------------------------------------

pub unsafe extern "C" fn ds_execute<T: Datasource + 'static>(
    ptr: CInstPtr,
    sql: CStrPtr,
    params: CStrPtr,
    affected: CRowsOut,
    out_err: CStrOut,
) -> i32 {
    let f = || -> Result<u64> {
        let sql = unsafe { ffi::c_to_string(sql)? };
        let params: Vec<Value> = ffi::decode_json(&unsafe { ffi::c_to_string(params)? })?;
        let guard = guard::<T>(ptr)?;
        guard.execute(&sql, &params)
    };
    unsafe {
        let n = f();
        match n {
            Ok(n) => {
                *affected = n;
                *out_err = ffi::alloc_string("");
                0
            }
            Err(e) => {
                ffi::set_out_err(out_err, &e);
                1
            }
        }
    }
}

pub unsafe extern "C" fn ds_batch_insert<T: Datasource + 'static>(
    ptr: CInstPtr,
    table: CStrPtr,
    columns: CStrPtr,
    rows: CStrPtr,
    mode: CStrPtr,
    pk: CStrPtr,
    affected: CRowsOut,
    out_err: CStrOut,
) -> i32 {
    let f = || -> Result<u64> {
        let table = unsafe { ffi::c_to_string(table)? };
        let columns: Vec<String> =
            ffi::decode_json(&unsafe { ffi::c_to_string(columns)? })?;
        let rows: Vec<Vec<Value>> = ffi::decode_json(&unsafe { ffi::c_to_string(rows)? })?;
        let mode: SyncMode = SyncMode::from_str(&unsafe { ffi::c_to_string(mode)? })?;
        let pk: Vec<String> = ffi::decode_json(&unsafe { ffi::c_to_string(pk)? })?;
        let guard = guard::<T>(ptr)?;
        guard.batch_insert(&table, &columns, &rows, mode, &pk)
    };
    unsafe {
        match f() {
            Ok(n) => {
                *affected = n;
                *out_err = ffi::alloc_string("");
                0
            }
            Err(e) => {
                ffi::set_out_err(out_err, &e);
                1
            }
        }
    }
}

pub unsafe extern "C" fn ds_truncate<T: Datasource + 'static>(
    ptr: CInstPtr,
    table: CStrPtr,
    out_err: CStrOut,
) -> i32 {
    let table = match unsafe { ffi::c_to_string(table) } {
        Ok(s) => s,
        Err(e) => {
            unsafe { ffi::set_out_err(out_err, &e) };
            return 1;
        }
    };
    // truncate needs `&mut self`; lock the instance mutex directly (ds_call
    // would deadlock re-locking the guard it already holds).
    let g = match guard::<T>(ptr) {
        Ok(g) => g,
        Err(e) => {
            unsafe { ffi::set_out_err(out_err, &e) };
            return 1;
        }
    };
    match g.truncate(&table) {
        Ok(()) => 0,
        Err(e) => {
            unsafe { ffi::set_out_err(out_err, &e) };
            1
        }
    }
}

pub unsafe extern "C" fn ds_ping<T: Datasource + 'static>(
    ptr: CInstPtr,
    out_err: CStrOut,
) -> i32 {
    unsafe { ds_call_void::<T, _>(ptr, out_err, |t| t.ping()) }
}
