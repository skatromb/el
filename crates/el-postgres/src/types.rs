//! Postgres `Type` → Arrow `DataType` mapping (v0 primitives).

use arrow_schema::DataType;
use el_core::ElError;
use tokio_postgres::types::Type;

pub fn pg_to_arrow(pg: &Type) -> Result<DataType, ElError> {
    Ok(match *pg {
        Type::BOOL => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => DataType::Utf8,
        Type::BYTEA => DataType::Binary,
        ref other => {
            return Err(ElError::Coercion(format!(
                "Postgres type `{}` (oid {}) not supported in 0.1",
                other.name(),
                other.oid()
            )));
        }
    })
}
