//! Shared type conversion utilities for PostgreSQL → JSON

use pgrx::pg_sys;
use pgrx::datum::{Date, Time, Timestamp, TimestampWithTimeZone, Interval};
use pgrx::AnyNumeric;

/// Convert PostgreSQL datum to JSON by type OID (for SPI rows)
#[inline]
pub fn datum_to_json(row: &pgrx::spi::SpiHeapTupleData, ordinal: usize, type_oid: pg_sys::Oid) -> serde_json::Value {
    use serde_json::Value::{Bool, Null, Number, String as JString};
    
    macro_rules! get {
        ($t:ty) => { row.get::<$t>(ordinal).ok().flatten() };
        ($t:ty, $map:expr) => { get!($t).map($map).unwrap_or(Null) };
    }
    
    match type_oid {
        pg_sys::BOOLOID => get!(bool, Bool),
        pg_sys::INT2OID => get!(i16, |v| Number(v.into())),
        pg_sys::INT4OID => get!(i32, |v| Number(v.into())),
        pg_sys::INT8OID => get!(i64, |v| Number(v.into())),
        pg_sys::FLOAT4OID => get!(f32).map(|v| serde_json::json!(v)).unwrap_or(Null),
        pg_sys::FLOAT8OID => get!(f64).map(|v| serde_json::json!(v)).unwrap_or(Null),
        pg_sys::NUMERICOID => get!(AnyNumeric, |v| JString(v.to_string())),
        pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID | pg_sys::NAMEOID => get!(String, JString),
        pg_sys::UUIDOID => get!(pgrx::Uuid, |v| JString(uuid::Uuid::from_bytes(*v.as_bytes()).to_string())),
        pg_sys::DATEOID => get!(Date, |v| JString(v.to_string())),
        pg_sys::TIMEOID => get!(Time, |v| JString(v.to_string())),
        pg_sys::TIMESTAMPOID => get!(Timestamp, |v| JString(v.to_string())),
        pg_sys::TIMESTAMPTZOID => get!(TimestampWithTimeZone, |v| JString(v.to_string())),
        pg_sys::INTERVALOID => get!(Interval, |v| JString(v.to_string())),
        pg_sys::JSONOID => get!(pgrx::Json).map(|v| v.0).unwrap_or(Null),
        pg_sys::JSONBOID => get!(pgrx::JsonB).map(|v| v.0).unwrap_or(Null),
        pg_sys::INT4ARRAYOID => get!(Vec<i32>).map(|v| serde_json::json!(v)).unwrap_or(Null),
        pg_sys::INT8ARRAYOID => get!(Vec<i64>).map(|v| serde_json::json!(v)).unwrap_or(Null),
        pg_sys::TEXTARRAYOID => get!(Vec<String>).map(|v| serde_json::json!(v)).unwrap_or(Null),
        pg_sys::BOOLARRAYOID => get!(Vec<bool>).map(|v| serde_json::json!(v)).unwrap_or(Null),
        _ => fallback_spi(row, ordinal),
    }
}

#[inline]
fn fallback_spi(row: &pgrx::spi::SpiHeapTupleData, ordinal: usize) -> serde_json::Value {
    row.get::<String>(ordinal).ok().flatten().map(serde_json::Value::String)
        .or_else(|| row.get::<i64>(ordinal).ok().flatten().map(|v| serde_json::Value::Number(v.into())))
        .or_else(|| row.get::<f64>(ordinal).ok().flatten().map(|v| serde_json::json!(v)))
        .or_else(|| row.get::<bool>(ordinal).ok().flatten().map(serde_json::Value::Bool))
        .or_else(|| row.get::<pgrx::JsonB>(ordinal).ok().flatten().map(|v| v.0))
        .or_else(|| row.get::<AnyNumeric>(ordinal).ok().flatten().map(|v| serde_json::Value::String(v.to_string())))
        .unwrap_or(serde_json::Value::Null)
}

/// Convert PostgreSQL heap tuple attribute to JSON (for triggers)
#[inline]
pub fn tuple_attr_to_json<'a, A: pgrx::WhoAllocated>(
    tuple: &pgrx::heap_tuple::PgHeapTuple<'a, A>,
    attnum: usize,
    type_oid: pg_sys::Oid,
) -> serde_json::Value {
    use serde_json::Value::{Bool, Null, Number, String as JString};
    use std::num::NonZeroUsize;
    
    let idx = match NonZeroUsize::new(attnum) {
        Some(i) => i,
        None => return Null,
    };
    
    macro_rules! get {
        ($t:ty) => { tuple.get_by_index::<$t>(idx).ok().flatten() };
        ($t:ty, $map:expr) => { get!($t).map($map).unwrap_or(Null) };
    }
    
    match type_oid {
        pg_sys::BOOLOID => get!(bool, Bool),
        pg_sys::INT2OID => get!(i16, |v| Number(v.into())),
        pg_sys::INT4OID => get!(i32, |v| Number(v.into())),
        pg_sys::INT8OID => get!(i64, |v| Number(v.into())),
        pg_sys::FLOAT4OID => get!(f32).map(|v| serde_json::json!(v)).unwrap_or(Null),
        pg_sys::FLOAT8OID => get!(f64).map(|v| serde_json::json!(v)).unwrap_or(Null),
        pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID => get!(String, JString),
        pg_sys::JSONBOID => get!(pgrx::JsonB).map(|v| v.0).unwrap_or(Null),
        pg_sys::JSONOID => get!(pgrx::Json).map(|v| v.0).unwrap_or(Null),
        pg_sys::TIMESTAMPOID => get!(Timestamp, |v| JString(v.to_string())),
        pg_sys::TIMESTAMPTZOID => get!(TimestampWithTimeZone, |v| JString(v.to_string())),
        pg_sys::DATEOID => get!(Date, |v| JString(v.to_string())),
        pg_sys::UUIDOID => get!(pgrx::Uuid, |v| JString(uuid::Uuid::from_bytes(*v.as_bytes()).to_string())),
        pg_sys::NUMERICOID => get!(AnyNumeric, |v| JString(v.to_string())),
        _ => get!(String, JString),
    }
}
