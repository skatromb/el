use std::path::{Path, PathBuf};
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use el_core::{Batches, Destination, ElError, RunReport};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tokio::sync::mpsc;
use tracing::warn;

use crate::batch_size::channel_capacity;
use crate::compression::Compression;

/// Static config for a Parquet destination: path + compression codec.
#[derive(Debug, Clone)]
pub struct ParquetConfig {
    pub path: PathBuf,
    pub compression: Compression,
}

/// Local single-file Parquet destination. Writes via tmp + atomic rename.
pub struct ParquetDestination {
    cfg: ParquetConfig,
}

impl ParquetDestination {
    /// Build a destination from config. No I/O performed.
    #[must_use]
    pub fn new(cfg: ParquetConfig) -> Self {
        Self { cfg }
    }
}

#[async_trait]
impl Destination for ParquetDestination {
    async fn write(
        self: Box<Self>,
        schema: SchemaRef,
        batches: Batches,
    ) -> Result<RunReport, ElError> {
        run(self.cfg, schema, batches).await
    }
}

/// Best-effort cleanup of the tmp file. Logs but doesn't propagate errors.
fn cleanup_tmp(tmp: &Path) {
    if let Err(e) = std::fs::remove_file(tmp)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %tmp.display(), error = %e, "failed to remove tmp parquet file");
    }
}

/// Drive the full write: spawn sync writer, pump async stream into it, atomic rename.
async fn run(
    cfg: ParquetConfig,
    schema: SchemaRef,
    mut batches: Batches,
) -> Result<RunReport, ElError> {
    let start = Instant::now();
    let final_path = cfg.path.clone();
    let tmp_path = tmp_path_for(&final_path);
    let compression = cfg.compression;

    let (tx, rx) = mpsc::channel::<RecordBatch>(channel_capacity());
    let writer_handle = tokio::task::spawn_blocking({
        let schema = schema.clone();
        let tmp = tmp_path.clone();
        move || writer_loop(&tmp, schema, compression, rx)
    });

    // Pump source batches into the writer channel.
    let pump_result: Result<(), ElError> = async {
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            tx.send(batch)
                .await
                .map_err(|_| ElError::destination("parquet writer task dropped channel"))?;
        }
        Ok(())
    }
    .await;
    drop(tx);

    let writer_result = writer_handle
        .await
        .map_err(|e| ElError::destination(format!("parquet writer task panicked: {e}")))?;

    let (rows, bytes) = match (pump_result, writer_result) {
        (Ok(()), Ok(stats)) => stats,
        (Err(e), _) | (_, Err(e)) => {
            cleanup_tmp(&tmp_path);
            return Err(e);
        }
    };

    if let Err(e) = tokio::fs::rename(&tmp_path, &final_path).await {
        cleanup_tmp(&tmp_path);
        return Err(ElError::from(e));
    }

    Ok(RunReport {
        rows,
        bytes_written: bytes,
        duration: start.elapsed(),
        coercions: vec![],
    })
}

/// Compute the staging path: append `.tmp` to the final file name.
fn tmp_path_for(final_path: &Path) -> PathBuf {
    let mut name = final_path
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(".tmp");
    final_path.with_file_name(name)
}

/// Sync writer loop running on a blocking thread.
/// Receives batches, writes Parquet, returns (`rows_written`, `bytes_written`).
fn writer_loop(
    tmp: &Path,
    schema: SchemaRef,
    compression: Compression,
    mut rx: mpsc::Receiver<RecordBatch>,
) -> Result<(u64, u64), ElError> {
    let file = std::fs::File::create(tmp)?;
    let props = WriterProperties::builder()
        .set_compression(compression.into())
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| ElError::destination(format!("ArrowWriter init: {e}")))?;

    let mut rows: u64 = 0;
    while let Some(batch) = rx.blocking_recv() {
        rows += batch.num_rows() as u64;
        writer
            .write(&batch)
            .map_err(|e| ElError::destination(format!("ArrowWriter::write: {e}")))?;
    }

    let _ = writer
        .close()
        .map_err(|e| ElError::destination(format!("ArrowWriter::close: {e}")))?;

    let bytes = std::fs::metadata(tmp)?.len();
    Ok((rows, bytes))
}
