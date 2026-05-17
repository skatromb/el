use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::{ElError, RunReport};

/// Boxed `Stream` of Arrow batches — one partition's data.
pub type BatchStream = BoxStream<'static, Result<RecordBatch, ElError>>;

/// A data source. Resolves a schema, then yields one or more partitions of Arrow batches.
#[async_trait]
pub trait Source: Send {
    /// Inspect-only: resolves the Arrow schema.
    async fn schema(&self) -> Result<SchemaRef, ElError>;

    /// Consume the source and produce its partitions. Single-shot.
    /// Non-partitionable sources return a single-element `Vec`.
    async fn partitions(self: Box<Self>) -> Result<Vec<BatchStream>, ElError>;
}

/// A destination. Atomically writes a schema + partitions and reports stats.
#[async_trait]
pub trait Destination: Send {
    /// Consume the destination and write the partitions. Single-shot.
    async fn write(
        self: Box<Self>,
        schema: SchemaRef,
        partitions: Vec<BatchStream>,
    ) -> Result<RunReport, ElError>;
}
