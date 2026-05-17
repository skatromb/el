use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::{ElError, RunReport};

/// Boxed `Stream` of Arrow batches.
pub type Batches = BoxStream<'static, Result<RecordBatch, ElError>>;

/// A data source. Resolves a schema, then yields Arrow batches once.
#[async_trait]
pub trait Source: Send {
    /// Inspect-only: resolves the Arrow schema.
    async fn schema(&self) -> Result<SchemaRef, ElError>;

    /// Consume the source and produce its batches. Single-shot.
    fn batches(self: Box<Self>) -> Batches;
}

/// A destination. Atomically writes a schema + batches and reports stats.
#[async_trait]
pub trait Destination: Send {
    /// Consume the destination and write the batches. Single-shot.
    async fn write(
        self: Box<Self>,
        schema: SchemaRef,
        batches: Batches,
    ) -> Result<RunReport, ElError>;
}
