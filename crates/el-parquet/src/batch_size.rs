use arrow_schema::{DataType, Field, IntervalUnit, SchemaRef};

/// Target bytes per Arrow `RecordBatch`. 16 MiB matches the project-wide default.
pub(crate) const TARGET_BYTES: usize = 16 * 1024 * 1024;

/// Minimum rows per batch even if the estimate exceeds the byte target.
const MIN_ROWS: usize = 1024;

/// Channel capacity between producer and consumer tasks: `available_parallelism * 2`,
/// fallback 8. Keeps each worker fed with one batch in flight + headroom.
pub(crate) fn channel_capacity() -> usize {
    std::thread::available_parallelism().map_or(8, |n| n.get() * 2)
}

/// Estimate rows per batch for a given schema and byte target.
pub(crate) fn estimate_rows(schema: &SchemaRef, target_bytes: usize) -> usize {
    let est_row_bytes: usize = schema
        .fields()
        .iter()
        .map(|f| estimate_field_bytes(f.as_ref()))
        .sum::<usize>()
        .max(1);
    (target_bytes / est_row_bytes).max(MIN_ROWS)
}

/// Estimated bytes per value for one Arrow field.
fn estimate_field_bytes(field: &Field) -> usize {
    estimate_type_bytes(field.data_type())
}

/// Estimated bytes per value for an Arrow data type. Variable-width types use 32 B as a guess.
fn estimate_type_bytes(dt: &DataType) -> usize {
    match dt {
        DataType::Null => 0,
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => 2,

        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Decimal32(_, _)
        | DataType::Interval(IntervalUnit::YearMonth) => 4,

        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Decimal64(_, _)
        | DataType::Interval(IntervalUnit::DayTime) => 8,

        DataType::Decimal128(_, _)
        | DataType::Union(_, _)
        | DataType::Interval(IntervalUnit::MonthDayNano) => 16,

        DataType::Decimal256(_, _)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => 32,

        DataType::FixedSizeBinary(n) => usize::try_from(*n).unwrap_or(0),

        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner)
        | DataType::Map(inner, _) => 4 * estimate_field_bytes(inner.as_ref()),

        DataType::FixedSizeList(inner, n) => {
            usize::try_from(*n).unwrap_or(0) * estimate_field_bytes(inner.as_ref())
        }

        DataType::Struct(fields) => fields
            .iter()
            .map(|f| estimate_field_bytes(f.as_ref()))
            .sum(),

        DataType::Dictionary(key, _) => estimate_type_bytes(key.as_ref()),
        DataType::RunEndEncoded(_, values) => estimate_field_bytes(values.as_ref()),
    }
}
