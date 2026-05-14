use std::future::Future;
use std::pin::Pin;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use futures::stream::BoxStream;

use crate::{ElError, RunReport};

/// Async, owning, type-erased stream of Arrow batches.
pub type BatchStream = BoxStream<'static, Result<RecordBatch, ElError>>;

/// Boxed `Send` future returning `Result<T, ElError>`.
/// Used as the return type of async trait methods that must stay dyn-compatible.
pub type FutureResult<T> = Pin<Box<dyn Future<Output = Result<T, ElError>> + Send>>;

/// A data source. Resolves a schema, then yields Arrow batches once.
pub trait Source: Send {
    /// Inspect-only: resolves the Arrow schema
    fn schema(&self) -> FutureResult<SchemaRef>;

    /// Consume the source and produce its batch stream. Single-shot.
    fn stream(self: Box<Self>) -> BatchStream;
}

/// A destination. Atomically writes a schema + batch stream and reports stats.
pub trait Destination: Send {
    /// Consume the destination and write the stream. Single-shot.
    fn write(self: Box<Self>, schema: SchemaRef, batches: BatchStream) -> FutureResult<RunReport>;
}
