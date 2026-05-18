//! Dogfood: in-memory batches → `ParquetDestination` → `ParquetSource` → in-memory collector,
//! both legs orchestrated by `Transfer`. Wide schema of round-trip-safe Arrow types.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::redundant_closure_for_method_calls
)]

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
    ListArray, StringArray, TimestampMicrosecondArray, UInt16Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use transferred_core::Transfer;
use transferred_core::test_utils::{TestDestination, TestSource};
use transferred_parquet::{Compression, ParquetDestination, ParquetSource};
use tempfile::tempdir;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("i32", DataType::Int32, false),
        Field::new("i64", DataType::Int64, true),
        Field::new("u16", DataType::UInt16, false),
        Field::new("f64", DataType::Float64, true),
        Field::new("bool", DataType::Boolean, true),
        Field::new("utf8", DataType::Utf8, true),
        Field::new("bin", DataType::Binary, true),
        Field::new("date", DataType::Date32, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "list",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
    ]))
}

fn batch(schema: &Arc<Schema>, rows: usize, offset: i64) -> RecordBatch {
    let i32_arr = Arc::new(Int32Array::from(
        (0..rows)
            .map(|i| i as i32 + offset as i32)
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let i64_arr = Arc::new(Int64Array::from(
        (0..rows)
            .map(|i| {
                if i % 3 == 0 {
                    None
                } else {
                    Some(i as i64 + offset)
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let u16_arr = Arc::new(UInt16Array::from(
        (0..rows).map(|i| i as u16).collect::<Vec<_>>(),
    )) as ArrayRef;
    let f64_arr = Arc::new(Float64Array::from(
        (0..rows)
            .map(|i| {
                if i % 2 == 0 {
                    Some(i as f64 * 1.25)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let bool_arr = Arc::new(BooleanArray::from(
        (0..rows)
            .map(|i| match i % 3 {
                0 => Some(true),
                1 => Some(false),
                _ => None,
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let utf8_arr = Arc::new(StringArray::from(
        (0..rows)
            .map(|i| {
                if i % 4 == 0 {
                    None
                } else {
                    Some(format!("s{}", i + offset as usize))
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let bin_arr = Arc::new(BinaryArray::from_opt_vec(
        (0..rows)
            .map(|i| {
                if i % 2 == 0 {
                    Some([i as u8, (i + 1) as u8, (i + 2) as u8])
                } else {
                    None
                }
            })
            .collect::<Vec<Option<[u8; 3]>>>()
            .iter()
            .map(|o| o.as_ref().map(|a| a.as_slice()))
            .collect(),
    )) as ArrayRef;
    let date_arr = Arc::new(Date32Array::from(
        (0..rows)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(19_000 + i as i32)
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let ts_arr = Arc::new(
        TimestampMicrosecondArray::from(
            (0..rows)
                .map(|i| Some(1_700_000_000_000_000 + i as i64 * 1_000_000))
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    ) as ArrayRef;

    let list_values = Int32Array::from((0..(rows * 2) as i32).collect::<Vec<_>>());
    let list_offsets = OffsetBuffer::from_lengths((0..rows).map(|_| 2usize));
    let list_field = Arc::new(Field::new("item", DataType::Int32, true));
    let list_arr = Arc::new(ListArray::new(
        list_field,
        list_offsets,
        Arc::new(list_values),
        None,
    )) as ArrayRef;

    RecordBatch::try_new(
        schema.clone(),
        vec![
            i32_arr, i64_arr, u16_arr, f64_arr, bool_arr, utf8_arr, bin_arr, date_arr, ts_arr,
            list_arr,
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn parquet_dogfood() {
    // Arrange
    let dir = tempdir().unwrap();
    let path = dir.path().join("out.parquet");
    let schema = schema();
    let input = vec![batch(&schema, 5, 0), batch(&schema, 3, 100)];
    let total_rows: usize = input.iter().map(RecordBatch::num_rows).sum();
    let memory_destination = TestDestination::new();
    let collected = memory_destination.batches.clone();

    // Act
    // Dump to file
    let write_report = Transfer::new(
        Box::new(TestSource::new(schema.clone(), input.clone())),
        Box::new(ParquetDestination::new(path.clone(), Compression::Zstd)),
    )
    .run()
    .await
    .unwrap();

    // Read from file back to memory
    let read_report = Transfer::new(
        Box::new(ParquetSource::new(path.clone())),
        Box::new(memory_destination),
    )
    .run()
    .await
    .unwrap();

    // Assert
    assert!(path.exists());
    assert_eq!(write_report.rows as usize, total_rows);
    assert!(write_report.bytes_written > 0);
    assert_eq!(read_report.rows as usize, total_rows);

    let read = collected.lock().unwrap();
    let read_schema = read[0].schema();
    assert_eq!(read_schema.fields(), schema.fields());

    let concat_in = arrow::compute::concat_batches(&schema, &input).unwrap();
    let concat_read = arrow::compute::concat_batches(&read_schema, read.iter()).unwrap();
    assert_eq!(concat_in, concat_read);
}
