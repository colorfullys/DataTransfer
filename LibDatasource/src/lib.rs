//! # LibDatasource SDK
//!
//! The datasource standard: a fixed data model, a `Datasource` trait, and a
//! C ABI that lets DataTransfer load database plugins (MySQL, PostgreSQL,
//! Oracle, ...) as dynamic libraries without ever depending on a concrete
//! driver.
//!
//! * **Plugins** implement [`Datasource`], convert native types to [`Value`]
//!   and register themselves with [`export_datasource_plugin!`].
//! * **DataTransfer** uses [`PluginRegistry`] to load plugin `.so` files and
//!   obtain `Arc<dyn Datasource>` connections.
//!
//! Adding a new datasource type = implementing `Datasource` in a new cdylib
//! crate and pointing `config.yaml` at the built artifact. No change to the
//! core is required.

pub mod datasource;
pub mod error;
pub mod ffi;
pub mod model;
#[cfg(feature = "plugin-host")]
pub mod plugin_registry;
pub mod plugin_api;

pub use datasource::Datasource;
pub use error::{DatasourceError, Result};
pub use model::{Column, ConnectionConfig, Row, SyncMode, TableSchema, Value};

#[cfg(feature = "plugin-host")]
pub use plugin_registry::{PluginHandle, PluginRegistry};

/// Register a datasource plugin.
///
/// Must be called from a cdylib crate after implementing [`Datasource`]:
///
/// ```ignore
/// libdatasource::export_datasource_plugin!(MyMysql, "mysql");
/// ```
///
/// The macro exports the single required symbol `ds_get_api`.
#[macro_export]
macro_rules! export_datasource_plugin {
    ($ds_type:ty, $name:literal) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn ds_get_api() -> *const $crate::ffi::DatasourceCapi {
            unsafe extern "C" fn ds_plugin_name() -> *const ::std::ffi::c_char {
                use ::std::sync::OnceLock;
                static NAME: OnceLock<&'static ::std::ffi::CStr> = OnceLock::new();
                NAME.get_or_init(|| {
                    ::std::boxed::Box::leak(
                        ::std::ffi::CString::new(::std::concat!($name))
                            .expect("plugin name cannot contain NUL")
                            .into_boxed_c_str(),
                    )
                })
                .as_ptr()
            }

            static CAP: $crate::ffi::DatasourceCapi = $crate::ffi::DatasourceCapi {
                version: $crate::ffi::ABI_VERSION,
                name: ds_plugin_name,
                create: $crate::plugin_api::ds_create::<$ds_type>,
                destroy: $crate::plugin_api::ds_destroy::<$ds_type>,
                connect: $crate::plugin_api::ds_connect::<$ds_type>,
                get_schema: $crate::plugin_api::ds_get_schema::<$ds_type>,
                query: $crate::plugin_api::ds_query::<$ds_type>,
                query_page: $crate::plugin_api::ds_query_page::<$ds_type>,
                query_params: $crate::plugin_api::ds_query_params::<$ds_type>,
                execute: $crate::plugin_api::ds_execute::<$ds_type>,
                batch_insert: $crate::plugin_api::ds_batch_insert::<$ds_type>,
                truncate: $crate::plugin_api::ds_truncate::<$ds_type>,
                ping: $crate::plugin_api::ds_ping::<$ds_type>,
                free_string: $crate::plugin_api::ds_free_string,
            };
            &CAP
        }
    };
}
