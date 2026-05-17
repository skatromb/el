use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use el_core::{Batches, ElError, Source};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::batch_size::{TARGET_BYTES, channel_capacity, estimate_rows};

/// Static config for a Parquet source.
#[derive(Debug, Clone)]
pub struct ParquetSourceConfig {
    pub path: PathBuf,
}

/// Local single-file Parquet source. Streams `RecordBatch` via `ParquetRecordBatchReader`.
pub struct ParquetSource {
    cfg: ParquetSourceConfig,
}

impl ParquetSource {
    /// Build a source from config. No I/O performed.
    #[must_use]
    pub fn new(cfg: ParquetSourceConfig) -> Self {
        Self { cfg }
    }
}

#[async_trait]
impl Source for ParquetSource {
    async fn schema(&self) -> Result<SchemaRef, ElError> {
        let path = self.cfg.path.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(&path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| ElError::source(format!("parquet reader init: {e}")))?;
            Ok::<SchemaRef, ElError>(builder.schema().clone())
        })
        .await
        .map_err(|e| ElError::source(format!("schema task panicked: {e}")))?
    }

    fn batches(self: Box<Self>) -> Batches {
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ElError>>(channel_capacity());
        let path = self.cfg.path;

        tokio::task::spawn_blocking(move || {
            let result: Result<(), ElError> = (|| {
                let file = std::fs::File::open(&path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|e| ElError::source(format!("parquet reader init: {e}")))?;
                let batch_rows = estimate_rows(builder.schema(), TARGET_BYTES);
                let builder = builder.with_batch_size(batch_rows);
                let reader = builder
                    .build()
                    .map_err(|e| ElError::source(format!("parquet reader build: {e}")))?;

                for batch in reader {
                    let batch =
                        batch.map_err(|e| ElError::source(format!("parquet read: {e}")))?;
                    if tx.blocking_send(Ok(batch)).is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            })();

            if let Err(e) = result {
                let _ = tx.blocking_send(Err(e));
            }
        });

        Box::pin(ReceiverStream::new(rx))
    }
}
