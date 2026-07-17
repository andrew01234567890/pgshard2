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

/// The declared type of a shard-key column. It is used to coerce a literal read
/// from SQL into the canonical [`ScalarValue`] the shard function hashes, so that
/// syntactically different spellings of the same value route identically (an
/// integer column matched with `customer_id = '1'` must hash the same keyspace id
/// as `customer_id = 1`). Integer widths share one variant because they already
/// hash identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    Int,
    Text,
    Uuid,
    Bytea,
}

impl ScalarType {
    /// Coerce a literal — as read from SQL, where an integer literal arrives as
    /// [`ScalarValue::Int64`] and a string literal as [`ScalarValue::Text`] — into
    /// the canonical value for this column type. Returns `None` when the literal
    /// is not a valid value of the type (PostgreSQL itself would reject it): the
    /// planner then treats the key as unroutable rather than hashing a value the
    /// stored row could never carry.
    pub fn coerce(self, value: &ScalarValue) -> Option<ScalarValue> {
        match (self, value) {
            (ScalarType::Int, ScalarValue::Int16(v)) => Some(ScalarValue::Int64(i64::from(*v))),
            (ScalarType::Int, ScalarValue::Int32(v)) => Some(ScalarValue::Int64(i64::from(*v))),
            (ScalarType::Int, ScalarValue::Int64(v)) => Some(ScalarValue::Int64(*v)),
            (ScalarType::Int, ScalarValue::Text(s)) => coerce_int(s).map(ScalarValue::Int64),
            (ScalarType::Text, ScalarValue::Text(s)) => Some(ScalarValue::Text(s.clone())),
            (ScalarType::Uuid, ScalarValue::Text(s)) => coerce_uuid(s).map(ScalarValue::Uuid),
            (ScalarType::Uuid, ScalarValue::Uuid(v)) => Some(ScalarValue::Uuid(*v)),
            (ScalarType::Bytea, ScalarValue::Text(s)) => coerce_bytea(s).map(ScalarValue::Bytea),
            (ScalarType::Bytea, ScalarValue::Bytea(v)) => Some(ScalarValue::Bytea(v.clone())),
            _ => None,
        }
    }
}

/// Parse an integer literal the way PostgreSQL's integer input does: optional
/// surrounding whitespace and a leading sign. Anything else (a decimal point, a
/// non-digit, an out-of-range magnitude) is not a valid integer key.
fn coerce_int(s: &str) -> Option<i64> {
    s.trim().parse::<i64>().ok()
}

/// Decode a UUID literal to its 16 bytes, accepting the forms PostgreSQL does:
/// optional surrounding braces, hyphens anywhere, and either case of hex. The 16
/// bytes are what a stored `uuid` hashes as, so any spelling of the same UUID
/// routes to one shard.
fn coerce_uuid(s: &str) -> Option<[u8; 16]> {
    let t = s.trim();
    let t = match (t.starts_with('{'), t.ends_with('}')) {
        (true, true) => &t[1..t.len() - 1],
        (false, false) => t,
        _ => return None,
    };
    let hex: String = t.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Decode a `bytea` literal in PostgreSQL hex format (`\x` followed by an even
/// number of hex digits). The escape format is not accepted in v1; an unparsable
/// literal is left unroutable rather than hashed as its text form.
fn coerce_bytea(s: &str) -> Option<Vec<u8>> {
    let hex = s.strip_prefix("\\x")?;
    if hex.len() % 2 != 0 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
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

    #[test]
    fn int_coercion_makes_quoted_and_bare_integers_identical() {
        // The whole point: `customer_id = '1'` (a string literal) and
        // `customer_id = 1` must resolve to the same canonical value.
        assert_eq!(
            ScalarType::Int.coerce(&ScalarValue::Text("1".into())),
            Some(ScalarValue::Int64(1))
        );
        assert_eq!(
            ScalarType::Int.coerce(&ScalarValue::Int64(1)),
            Some(ScalarValue::Int64(1))
        );
        // Widths widen to i64 (they already hash identically).
        assert_eq!(
            ScalarType::Int.coerce(&ScalarValue::Int32(1)),
            Some(ScalarValue::Int64(1))
        );
        // PostgreSQL trims whitespace and accepts a leading sign.
        assert_eq!(
            ScalarType::Int.coerce(&ScalarValue::Text("  -42 ".into())),
            Some(ScalarValue::Int64(-42))
        );
        // A value that is not a valid integer is unroutable, not misrouted.
        for bad in ["abc", "1.5", "", "9999999999999999999999"] {
            assert_eq!(
                ScalarType::Int.coerce(&ScalarValue::Text(bad.into())),
                None,
                "{bad}"
            );
        }
    }

    #[test]
    fn text_coercion_is_identity_and_rejects_bare_integers() {
        assert_eq!(
            ScalarType::Text.coerce(&ScalarValue::Text("1".into())),
            Some(ScalarValue::Text("1".into()))
        );
        // `text_col = 1` has no `text = int` operator in PostgreSQL: unroutable.
        assert_eq!(ScalarType::Text.coerce(&ScalarValue::Int64(1)), None);
    }

    #[test]
    fn uuid_coercion_is_spelling_independent() {
        let canonical = ScalarType::Uuid.coerce(&ScalarValue::Text(
            "00010203-0405-0607-0809-0a0b0c0d0e0f".into(),
        ));
        assert_eq!(
            canonical,
            Some(ScalarValue::Uuid([
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
            ]))
        );
        // Uppercase, no hyphens, and braces all name the same UUID -> same bytes.
        for spelling in [
            "00010203-0405-0607-0809-0A0B0C0D0E0F",
            "000102030405060708090a0b0c0d0e0f",
            "{00010203-0405-0607-0809-0a0b0c0d0e0f}",
        ] {
            assert_eq!(
                ScalarType::Uuid.coerce(&ScalarValue::Text(spelling.into())),
                canonical,
                "{spelling}"
            );
        }
        // Wrong length / unbalanced brace / non-hex: unroutable.
        for bad in [
            "not-a-uuid",
            "0001",
            "{00010203-0405-0607-0809-0a0b0c0d0e0f",
        ] {
            assert_eq!(
                ScalarType::Uuid.coerce(&ScalarValue::Text(bad.into())),
                None,
                "{bad}"
            );
        }
    }

    #[test]
    fn bytea_coercion_reads_hex_format() {
        assert_eq!(
            ScalarType::Bytea.coerce(&ScalarValue::Text("\\xdeadbeef".into())),
            Some(ScalarValue::Bytea(vec![0xde, 0xad, 0xbe, 0xef]))
        );
        // Missing `\x`, odd digit count, or non-hex: unroutable.
        for bad in ["deadbeef", "\\xabc", "\\xzz"] {
            assert_eq!(
                ScalarType::Bytea.coerce(&ScalarValue::Text(bad.into())),
                None,
                "{bad}"
            );
        }
    }

    #[test]
    fn coerced_spellings_hash_to_the_same_keyspace_id() {
        let f = shard_function("xxhash64_v1").unwrap();
        let bare = ScalarType::Int.coerce(&ScalarValue::Int64(12345)).unwrap();
        let quoted = ScalarType::Int
            .coerce(&ScalarValue::Text("12345".into()))
            .unwrap();
        assert_eq!(f.keyspace_id(&bare), f.keyspace_id(&quoted));
    }
}
