//! Datasource layer: plugin loading / connection management, paginated source
//! reading, batch target writing and row routing to parallel writers.

pub mod connections;
pub mod reader;
pub mod router;
pub mod writer;

pub use connections::ConnectionManager;
pub use router::Router;
pub use writer::Writer;
