//! Local Parquet source + destination. arrow-rs `parquet` crate.

mod batch_size;
mod compression;
mod destination;
mod source;

pub use compression::Compression;
pub use destination::{ParquetConfig, ParquetDestination};
pub use source::{ParquetSource, ParquetSourceConfig};
