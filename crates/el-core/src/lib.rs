//! `el-core` — connector-agnostic types: traits, error type, run report.

mod error;
mod report;
#[cfg(feature = "dev")]
pub mod test_utils;
mod traits;
mod transfer;

pub use error::ElError;
pub use report::{Coercion, CoercionLevel, RunReport};
pub use traits::{BatchStream, Destination, Source};
pub use transfer::Transfer;

pub type Result<T> = std::result::Result<T, ElError>;
