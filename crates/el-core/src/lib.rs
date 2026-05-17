//! `el-core` — connector-agnostic types: traits, error type, run report.

mod error;
mod report;
mod traits;

pub use error::ElError;
pub use report::{Coercion, CoercionLevel, RunReport};
pub use traits::{Batches, Destination, Source};

pub type Result<T> = std::result::Result<T, ElError>;
