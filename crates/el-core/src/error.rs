use thiserror::Error;

/// Root error type. Every fallible operation in `el` returns `Result<T, ElError>`.
/// Maps to Python `el.ElError` at the FFI boundary.
#[derive(Debug, Error)]
pub enum ElError {
    #[error("source error: {0}")]
    Source(String),

    #[error("destination error: {0}")]
    Destination(String),

    #[error("schema mismatch: {0}")]
    Schema(String),

    #[error("type coercion refused: {0}")]
    Coercion(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("{0}")]
    Other(String),
}

impl ElError {
    pub fn source<S: Into<String>>(msg: S) -> Self {
        Self::Source(msg.into())
    }

    pub fn destination<S: Into<String>>(msg: S) -> Self {
        Self::Destination(msg.into())
    }

    pub fn schema<S: Into<String>>(msg: S) -> Self {
        Self::Schema(msg.into())
    }

    pub fn coercion<S: Into<String>>(msg: S) -> Self {
        Self::Coercion(msg.into())
    }

    pub fn other<S: Into<String>>(msg: S) -> Self {
        Self::Other(msg.into())
    }
}
