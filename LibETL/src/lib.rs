//! # LibETL SDK
//!
//! ETL transformation framework for DataTransfer:
//!
//! * the [`EtlProcessor`] trait and [`EtlContext`] (which exposes the current
//!   table/connection and a [`TableLookup`] handle to read other tables);
//! * built-in processors (rename / set / filter / cast);
//! * a C ABI so custom ETL logic can be shipped as a dynamic plugin.

pub mod builtin;
pub mod error;
pub mod ffi;
pub mod model;
pub mod plugin_api;
#[cfg(feature = "plugin-host")]
pub mod registry;
pub mod trait_def;

pub use error::{EtlError, Result};
pub use model::{EtlInput, EtlOutputRow, EtlRow};
pub use trait_def::{EtlConfigure, EtlContext, EtlProcessor, TableLookup};

#[cfg(feature = "plugin-host")]
pub use registry::{EtlHandle, EtlRegistry};

/// Register an ETL plugin.
///
/// The plugin must implement [`EtlProcessor`], [`Default`] (a default
/// instance is created, then `configure` is called with the job config) and
/// [`trait_def::EtlConfigure`].
#[macro_export]
macro_rules! export_etl_plugin {
    ($proc_type:ty, $name:literal) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn etl_get_api() -> *const $crate::ffi::EtlCapi {
            unsafe extern "C" fn etl_plugin_name() -> *const ::std::ffi::c_char {
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

            static CAP: $crate::ffi::EtlCapi = $crate::ffi::EtlCapi {
                version: $crate::ffi::ABI_VERSION,
                name: etl_plugin_name,
                create: $crate::plugin_api::etl_create::<$proc_type>,
                destroy: $crate::plugin_api::etl_destroy::<$proc_type>,
                configure: $crate::plugin_api::etl_configure::<$proc_type>,
                process: $crate::plugin_api::etl_process::<$proc_type>,
                free_string: $crate::plugin_api::etl_free_string,
            };
            &CAP
        }
    };
}