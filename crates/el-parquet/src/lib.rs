//! Local Parquet source + destination. arrow-rs `parquet` crate.

mod compression;
mod destination;
mod source;

pub use compression::Compression;
pub use destination::ParquetDestination;
pub use source::ParquetSource;
