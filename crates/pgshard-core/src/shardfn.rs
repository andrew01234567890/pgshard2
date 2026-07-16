use thiserror::Error;
use xxhash_rust::xxh64::xxh64;

use crate::keyspace::KeyspaceId;

/// A shard-key value in canonical form. Integer widths all widen to i64 so
/// that e.g. `int4 5` and `int8 5` hash identically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarValue {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Text(String),
    Uuid([u8; 16]),
    Bytea(Vec<u8>),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ShardFnError {
    #[error("unknown shard function {0:?}")]
    Unknown(String),
}

/// Maps a shard-key value to its keyspace id. Implementations are versioned
/// by name (recorded in the vschema); the mapping must never change for a
/// given name.
pub trait ShardFunction: Send + Sync {
    fn name(&self) -> &'static str;
    fn keyspace_id(&self, key: &ScalarValue) -> KeyspaceId;
}

/// xxHash64 (seed 0) over the canonical byte encoding:
/// integers as 8-byte big-endian two's complement, text as UTF-8 bytes,
/// uuid as its 16 bytes, bytea as-is.
pub struct XxHash64V1;

pub const XXHASH64_V1: XxHash64V1 = XxHash64V1;

impl ShardFunction for XxHash64V1 {
    fn name(&self) -> &'static str {
        "xxhash64_v1"
    }

    fn keyspace_id(&self, key: &ScalarValue) -> KeyspaceId {
        let id = match key {
            ScalarValue::Int16(v) => xxh64(&i64::from(*v).to_be_bytes(), 0),
            ScalarValue::Int32(v) => xxh64(&i64::from(*v).to_be_bytes(), 0),
            ScalarValue::Int64(v) => xxh64(&v.to_be_bytes(), 0),
            ScalarValue::Text(v) => xxh64(v.as_bytes(), 0),
            ScalarValue::Uuid(v) => xxh64(v, 0),
            ScalarValue::Bytea(v) => xxh64(v, 0),
        };
        KeyspaceId(id)
    }
}

pub fn shard_function(name: &str) -> Result<&'static dyn ShardFunction, ShardFnError> {
    match name {
        "xxhash64_v1" => Ok(&XXHASH64_V1),
        other => Err(ShardFnError::Unknown(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_widths_hash_identically() {
        let f = shard_function("xxhash64_v1").unwrap();
        let id64 = f.keyspace_id(&ScalarValue::Int64(42));
        assert_eq!(f.keyspace_id(&ScalarValue::Int32(42)), id64);
        assert_eq!(f.keyspace_id(&ScalarValue::Int16(42)), id64);
        assert_ne!(f.keyspace_id(&ScalarValue::Int64(-42)), id64);
    }

    /// Cross-language golden vectors: the Go operator's keyspace-id
    /// implementation asserts these exact values. Never change them —
    /// xxhash64_v1 is versioned by name and frozen.
    #[test]
    fn golden_vectors_are_frozen() {
        let f = shard_function("xxhash64_v1").unwrap();
        let cases: [(ScalarValue, u64); 10] = [
            (ScalarValue::Int64(0), 0x34c96acdcadb1bbb),
            (ScalarValue::Int64(1), 0x9f1ffc793b8a47da),
            (ScalarValue::Int64(42), 0xa2c396223f8bdbdf),
            (ScalarValue::Int64(-1), 0x85d136adb773c6c9),
            (ScalarValue::Int64(i64::MAX), 0x043de1bbaf341994),
            (ScalarValue::Text(String::new()), 0xef46db3751d8e999),
            (ScalarValue::Text("hello".into()), 0x26c7827d889f6da3),
            (
                ScalarValue::Text("customer-12345".into()),
                0xab540c504509fb51,
            ),
            (
                ScalarValue::Uuid([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
                0x44b6ef2fb84169f7,
            ),
            (
                ScalarValue::Bytea(vec![0xde, 0xad, 0xbe, 0xef]),
                0x2ff5cfb6af9aaf68,
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(f.keyspace_id(&value), KeyspaceId(expected), "{value:?}");
        }
    }

    #[test]
    fn unknown_function_is_rejected() {
        let Err(err) = shard_function("md5") else {
            panic!("md5 must be rejected");
        };
        assert_eq!(err, ShardFnError::Unknown("md5".into()));
    }
}
