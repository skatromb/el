use std::path::PathBuf;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use transferred_core::{BatchStream, ElError, Source};
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;

/// Local single-file Parquet source. Yields `RecordBatch` via async Parquet reader.
#[derive(Debug, Clone)]
pub struct ParquetSource {
    pub path: PathBuf,
}

impl ParquetSource {
    /// Build a source. No I/O performed.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl Source for ParquetSource {
    async fn schema(&self) -> Result<SchemaRef, ElError> {
        let file = File::open(&self.path).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .map_err(|e| ElError::source(format!("parquet reader init: {e}")))?;
        Ok(builder.schema().clone())
    }

    /// Currently reads only sequentially.
    async fn partitions(self: Box<Self>) -> Result<Vec<BatchStream>, ElError> {
        let file = File::open(&self.path).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .map_err(|err| ElError::source(format!("parquet reader init: {err}")))?;
        let stream = builder
            .build()
            .map_err(|err| ElError::source(format!("parquet reader build: {err}")))?
            .map(|result| result.map_err(|e| ElError::source(format!("parquet read: {e}"))));

        Ok(vec![Box::pin(stream)])
    }
}
