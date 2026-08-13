//! Host side of the ETL framework: pipeline execution and the table-lookup
//! glue handed to dynamic ETL plugins over the C ABI.

pub mod pipeline;

pub use pipeline::{AppLookup, EtlPipeline};
