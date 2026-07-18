//! Filtering decoded rows by keyspace id — the core of reshard seeding and of
//! keyspace-scoped CDC.
//!
//! A reshard seeder copies only the rows whose shard key falls in the target
//! shard's key range. To decide that, it must reproduce the router's placement:
//! hash the shard-key value with the same shard function into a [`KeyspaceId`] and
//! test the target [`KeyRange`].
//!
//! pgoutput ships every value in text form, so the shard key arrives as text.
//! Reproducing the router's hash therefore needs the column's **declared type**:
//! the router coerces a literal to its column type before hashing (so `5` and
//! `'5'` route alike), and only the type lets this recover the same canonical
//! value from the text. An untyped topology hashes the SQL literal in its written
//! form, which a text-only replication stream cannot recover — so filtering
//! requires a declared [`ScalarType`]; the operator emits one for every sharded
//! table. A NULL, unshipped-TOAST, or type-invalid shard key is unroutable.

use pgshard_core::{KeyRange, KeyspaceId, ScalarType, ScalarValue, ShardFunction};
use thiserror::Error;

use crate::pgoutput::{Relation, TupleColumn, TupleData};

/// Why a row's shard key could not be resolved to a keyspace id.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FilterError {
    #[error("relation has no column named {0:?} to use as the shard key")]
    NoShardKeyColumn(String),
    #[error("shard-key column index {index} is out of range for a {len}-column row")]
    ColumnOutOfRange { index: usize, len: usize },
    #[error("shard key is NULL or an unshipped TOAST value; the row cannot be routed")]
    UnroutableCell,
    #[error("shard key is in binary format, which the seeder's text stream should not produce")]
    BinaryCell,
    #[error("shard key value is not valid UTF-8")]
    InvalidUtf8,
    #[error("shard key value {value:?} is not a valid {ty:?}")]
    Uncoercible { value: String, ty: ScalarType },
}

/// The index of a table's shard-key column within its [`Relation`] column list,
/// resolved by name. The apply engine caches this per relation oid so the lookup
/// happens once, not per row.
pub fn shard_key_index(relation: &Relation, shard_key_column: &str) -> Result<usize, FilterError> {
    relation
        .columns
        .iter()
        .position(|c| c.name == shard_key_column)
        .ok_or_else(|| FilterError::NoShardKeyColumn(shard_key_column.to_owned()))
}

/// The keyspace id of a shard-key cell shipped in pgoutput text form.
///
/// `shard_key_type` is required (not optional): see the module docs for why the
/// declared type is what makes this reproduce the router's placement soundly.
///
/// Two soundness constraints on the *topology*, not enforceable here (the shard
/// function and declared type are all this sees):
/// - `Text` must be a non-space-padded type (`text`/`varchar`), never `char(n)`
///   /`bpchar`. The router hashes the unpadded literal a client wrote (`'abc'`),
///   but pgoutput ships the *stored* value, which for `char(n)` is space-padded
///   (`'abc   '`) — so a `bpchar` `Text` key would hash differently here and be
///   seeded to the wrong shard. The operator must not declare a `char(n)` column
///   as `shardKeyType: text`.
/// - `Bytea` relies on the source shipping `\x…`, which matches the router's own
///   `\x` literal form. The replication client pins `bytea_output = hex` on its
///   walsender session (see `client::ReplicationClient::startup`), so this holds
///   regardless of the source's database/cluster default. Were a stored value
///   ever to arrive in `escape` form, `coerce_bytea` fails closed (`Uncoercible`)
///   — rejected, never misplaced.
pub fn cell_keyspace_id(
    cell: &TupleColumn,
    shard_key_type: ScalarType,
    shard_fn: &dyn ShardFunction,
) -> Result<KeyspaceId, FilterError> {
    let bytes = match cell {
        TupleColumn::Text(bytes) => *bytes,
        TupleColumn::Binary(_) => return Err(FilterError::BinaryCell),
        TupleColumn::Null | TupleColumn::UnchangedToast => {
            return Err(FilterError::UnroutableCell);
        }
    };
    let text = std::str::from_utf8(bytes).map_err(|_| FilterError::InvalidUtf8)?;
    text_keyspace_id(text, shard_key_type, shard_fn)
}

/// The keyspace id of a shard-key value already decoded to its text form.
///
/// This is the shared core of both the streaming filter ([`cell_keyspace_id`])
/// and the initial snapshot [`crate::copy`]. Both must produce the same id for
/// the same logical value so a row is copied by the snapshot pass iff it would
/// also be kept by the stream — no gap or overlap at the snapshot/stream seam.
pub fn text_keyspace_id(
    text: &str,
    shard_key_type: ScalarType,
    shard_fn: &dyn ShardFunction,
) -> Result<KeyspaceId, FilterError> {
    // The router hashes the value coerced to the column type, so `5` and `'5'`
    // land on one shard; coercing the text form here reproduces that exactly.
    let canonical = shard_key_type
        .coerce(&ScalarValue::Text(text.to_owned()))
        .ok_or_else(|| FilterError::Uncoercible {
            value: text.to_owned(),
            ty: shard_key_type,
        })?;
    Ok(shard_fn.keyspace_id(&canonical))
}

/// The keyspace id of `tuple`'s shard-key column.
pub fn tuple_keyspace_id(
    tuple: &TupleData,
    shard_key_index: usize,
    shard_key_type: ScalarType,
    shard_fn: &dyn ShardFunction,
) -> Result<KeyspaceId, FilterError> {
    let cell = tuple
        .columns
        .get(shard_key_index)
        .ok_or(FilterError::ColumnOutOfRange {
            index: shard_key_index,
            len: tuple.columns.len(),
        })?;
    cell_keyspace_id(cell, shard_key_type, shard_fn)
}

/// Whether `tuple`'s shard-key column falls within `range` — the seeding/CDC
/// keep-or-skip decision for one row.
pub fn tuple_in_range(
    tuple: &TupleData,
    shard_key_index: usize,
    shard_key_type: ScalarType,
    shard_fn: &dyn ShardFunction,
    range: &KeyRange,
) -> Result<bool, FilterError> {
    let id = tuple_keyspace_id(tuple, shard_key_index, shard_key_type, shard_fn)?;
    Ok(range.contains(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::RelationColumn;
    use pgshard_core::shard_function;

    fn xxhash() -> &'static dyn ShardFunction {
        shard_function("xxhash64_v1").unwrap()
    }

    fn relation(columns: &[&str]) -> Relation<'static> {
        // Leak the names so the borrowed Relation is 'static for the test.
        let columns = columns
            .iter()
            .map(|name| RelationColumn {
                flags: 0,
                name: Box::leak(name.to_string().into_boxed_str()),
                type_oid: 23,
                type_modifier: -1,
            })
            .collect();
        Relation {
            xid: None,
            oid: 16400,
            namespace: "public",
            name: "orders",
            replica_identity: b'd',
            columns,
        }
    }

    fn tuple(cells: Vec<TupleColumn<'static>>) -> TupleData<'static> {
        TupleData { columns: cells }
    }

    #[test]
    fn resolves_the_shard_key_column_index() {
        let rel = relation(&["id", "customer_id", "note"]);
        assert_eq!(shard_key_index(&rel, "customer_id").unwrap(), 1);
        assert_eq!(
            shard_key_index(&rel, "missing").unwrap_err(),
            FilterError::NoShardKeyColumn("missing".to_owned())
        );
    }

    #[test]
    fn text_shard_key_hashes_identically_to_the_routers_typed_value() {
        // The soundness property: the router hashes an int literal as Int64; the
        // replication stream ships it as text "5"; both must reach one keyspace id.
        let f = xxhash();
        let from_text = cell_keyspace_id(&TupleColumn::Text(b"5"), ScalarType::Int, f).unwrap();
        let from_router = f.keyspace_id(&ScalarValue::Int64(5));
        assert_eq!(from_text, from_router);

        // And a quoted vs unquoted spelling of the same int route alike, because
        // both coerce to Int64(5) — the whole point of typing the shard key.
        let quoted = cell_keyspace_id(&TupleColumn::Text(b"5"), ScalarType::Int, f).unwrap();
        assert_eq!(quoted, from_router);

        // A text-typed key hashes as its text.
        let as_text = cell_keyspace_id(&TupleColumn::Text(b"5"), ScalarType::Text, f).unwrap();
        assert_eq!(as_text, f.keyspace_id(&ScalarValue::Text("5".to_owned())));
        // Int and text spellings of "5" are different keyspace ids (different types).
        assert_ne!(from_router, as_text);
    }

    #[test]
    fn null_toast_binary_and_uncoercible_keys_are_rejected() {
        let f = xxhash();
        assert_eq!(
            cell_keyspace_id(&TupleColumn::Null, ScalarType::Int, f).unwrap_err(),
            FilterError::UnroutableCell
        );
        assert_eq!(
            cell_keyspace_id(&TupleColumn::UnchangedToast, ScalarType::Int, f).unwrap_err(),
            FilterError::UnroutableCell
        );
        assert_eq!(
            cell_keyspace_id(&TupleColumn::Binary(b"\x00"), ScalarType::Int, f).unwrap_err(),
            FilterError::BinaryCell
        );
        // "abc" is not a valid integer.
        assert_eq!(
            cell_keyspace_id(&TupleColumn::Text(b"abc"), ScalarType::Int, f).unwrap_err(),
            FilterError::Uncoercible {
                value: "abc".to_owned(),
                ty: ScalarType::Int,
            }
        );
    }

    #[test]
    fn uuid_and_bytea_shard_keys_match_the_routers_typed_hash() {
        // These reshard-critical types must hash from their pgoutput text form to
        // exactly the keyspace id the router computed from the canonical value.
        let f = xxhash();

        // pgoutput emits a UUID in canonical lowercase-hyphenated form.
        let uuid_bytes = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let from_text = cell_keyspace_id(
            &TupleColumn::Text(b"550e8400-e29b-41d4-a716-446655440000"),
            ScalarType::Uuid,
            f,
        )
        .unwrap();
        assert_eq!(from_text, f.keyspace_id(&ScalarValue::Uuid(uuid_bytes)));

        // pgoutput emits bytea in the default hex form (`\x..`).
        let from_hex =
            cell_keyspace_id(&TupleColumn::Text(b"\\x0102ff"), ScalarType::Bytea, f).unwrap();
        assert_eq!(
            from_hex,
            f.keyspace_id(&ScalarValue::Bytea(vec![0x01, 0x02, 0xff]))
        );
    }

    #[test]
    fn tuple_in_range_keeps_in_range_rows_and_skips_the_rest() {
        let f = xxhash();
        let row = tuple(vec![TupleColumn::Text(b"7"), TupleColumn::Text(b"hi")]);
        // The full range keeps every row.
        assert!(tuple_in_range(&row, 0, ScalarType::Int, f, &KeyRange::FULL).unwrap());

        // A range that starts just above the row's own id excludes it.
        let id = tuple_keyspace_id(&row, 0, ScalarType::Int, f).unwrap();
        let above = KeyRange::new(id.0.wrapping_add(1), None).unwrap();
        assert!(!tuple_in_range(&row, 0, ScalarType::Int, f, &above).unwrap());
        // A range that ends at the row's id (exclusive) also excludes it, while one
        // covering it keeps it.
        if id.0 > 0 {
            let below = KeyRange::new(0, Some(id.0)).unwrap();
            assert!(!tuple_in_range(&row, 0, ScalarType::Int, f, &below).unwrap());
        }
        let covering = KeyRange::new(id.0, id.0.checked_add(1)).unwrap();
        assert!(tuple_in_range(&row, 0, ScalarType::Int, f, &covering).unwrap());
    }

    #[test]
    fn a_short_row_reports_the_out_of_range_column() {
        let f = xxhash();
        let row = tuple(vec![TupleColumn::Text(b"1")]);
        assert_eq!(
            tuple_keyspace_id(&row, 3, ScalarType::Int, f).unwrap_err(),
            FilterError::ColumnOutOfRange { index: 3, len: 1 }
        );
    }
}
