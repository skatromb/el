use std::path::{Path, PathBuf};
use std::time::Instant;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use transferred_core::{BatchStream, Destination, ElError, RunReport};
use futures::StreamExt;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use tokio::fs::File;
use tracing::warn;

use crate::compression::Compression;

/// Local single-file Parquet destination. Writes via tmp + atomic rename.
#[derive(Debug, Clone)]
pub struct ParquetDestination {
    pub path: PathBuf,
    pub compression: Compression,
}

impl ParquetDestination {
    /// Build a destination. No I/O performed.
    #[must_use]
    pub fn new(path: PathBuf, compression: Compression) -> Self {
        Self { path, compression }
    }
}

#[async_trait]
impl Destination for ParquetDestination {
    async fn write(
        self: Box<Self>,
        schema: SchemaRef,
        batches: Vec<BatchStream>,
    ) -> Result<RunReport, ElError> {
        run(*self, schema, batches).await
    }
}

async fn run(
    destination: ParquetDestination,
    schema: SchemaRef,
    batches: Vec<BatchStream>,
) -> Result<RunReport, ElError> {
    let start = Instant::now();
    let tmp = tmp_path(&destination.path);

    let result = write_all(&tmp, schema, destination.compression, batches).await;

    let (rows, bytes) = match result {
        Ok(stats) => stats,
        Err(err) => {
            cleanup_tmp(&tmp).await;
            return Err(err);
        }
    };

    if let Err(err) = tokio::fs::rename(&tmp, &destination.path).await {
        cleanup_tmp(&tmp).await;
        return Err(ElError::from(err));
    }

    Ok(RunReport {
        rows,
        bytes_written: bytes,
        duration: start.elapsed(),
        coercions: vec![],
    })
}

/// Currently supports only sequential 
async fn write_all(
    tmp: &Path,
    schema: SchemaRef,
    compression: Compression,
    batches: Vec<BatchStream>,
) -> Result<(u64, u64), ElError> {
    let file = File::create(tmp).await?;
    let props = WriterProperties::builder()
        .set_compression(compression.into())
        .build();
    let mut writer = AsyncArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| ElError::destination(format!("AsyncArrowWriter init: {e}")))?;

    let mut rows: u64 = 0;
    for mut partition in batches {
        while let Some(batch) = partition.next().await {
            let batch = batch?;
            rows += batch.num_rows() as u64;
            writer
                .write(&batch)
                .await
                .map_err(|e| ElError::destination(format!("AsyncArrowWriter::write: {e}")))?;
        }
    }
    writer
        .close()
        .await
        .map_err(|e| ElError::destination(format!("AsyncArrowWriter::close: {e}")))?;

    let bytes = tokio::fs::metadata(tmp).await?.len();
    Ok((rows, bytes))
}

async fn cleanup_tmp(tmp: &Path) {
    if let Err(err) = tokio::fs::remove_file(tmp).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %tmp.display(), error = %err, "failed to remove tmp parquet file");
    }
}

fn tmp_path(final_path: &Path) -> PathBuf {
    let mut name = final_path
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(".tmp");
    final_path.with_file_name(name)
}
