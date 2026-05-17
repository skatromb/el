//! Dogfood: synthetic batches → `ParquetDestination` → `ParquetSource` → equality.

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use el_core::{BatchStream, Destination, Source};
use el_parquet::{Compression, ParquetDestination, ParquetSource};
use futures::stream::{self, StreamExt};
use tempfile::tempdir;

fn sample_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn sample_batches(schema: &Arc<Schema>) -> Vec<RecordBatch> {
    let b1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();
    let b2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec![Some("d"), Some("e")])),
        ],
    )
    .unwrap();
    vec![b1, b2]
}

#[tokio::test]
async fn parquet_dogfood() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("out.parquet");

    let schema = sample_schema();
    let written = sample_batches(&schema);

    let stream_in: BatchStream = Box::pin(stream::iter(written.clone()).map(Ok));

    let dest = Box::new(ParquetDestination::new(path.clone(), Compression::Zstd));
    let report = dest
        .write(schema.clone(), vec![stream_in])
        .await
        .unwrap();

    assert_eq!(report.rows, 5);
    assert!(report.bytes_written > 0);
    assert!(path.exists());

    let src = ParquetSource::new(path.clone());
    let read_schema = src.schema().await.unwrap();
    assert_eq!(read_schema.fields(), schema.fields());

    let src = Box::new(ParquetSource::new(path));
    let partitions = src.partitions().await.unwrap();
    let mut read: Vec<RecordBatch> = Vec::new();
    for mut p in partitions {
        while let Some(b) = p.next().await {
            read.push(b.unwrap());
        }
    }

    let total_rows: usize = read.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 5);

    let concat_written = arrow::compute::concat_batches(&schema, &written).unwrap();
    let concat_read = arrow::compute::concat_batches(&read_schema, &read).unwrap();
    assert_eq!(concat_written, concat_read);
}
