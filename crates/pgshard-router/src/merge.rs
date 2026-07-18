//! Sound cross-shard `ORDER BY` merge.
//!
//! Each shard runs the same query — including its `ORDER BY` — so it returns its
//! own rows already sorted. To combine them the router decodes *only* the
//! sort-key columns, by their real PostgreSQL type, and orders the combined rows
//! with a comparator that matches PostgreSQL. This is sound only because the
//! router now sees real column type OIDs (the verbatim backend): a text-mode
//! byte-order sort would misorder numbers (`"10" < "9"`).
//!
//! Slice 1 supports sort keys of the types where PostgreSQL's on-shard order
//! equals the router's decoded order *unconditionally* — integers, floats, and
//! booleans. Text is deliberately excluded: a byte-order merge of text sorted
//! under a non-C collation produces provably wrong output, so an `ORDER BY` on an
//! unsupported column type is rejected upstream (`0A000`).

use std::cmp::Ordering;

use pgwire::api::Type;
use pgwire::api::results::{FieldFormat, FieldInfo};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;

/// A sort key parsed from an `ORDER BY` item.
#[derive(Debug, PartialEq, Eq)]
pub struct SortKey {
    pub column: SortColumn,
    pub descending: bool,
    pub nulls_first: bool,
}

/// Which output column a sort key names.
#[derive(Debug, PartialEq, Eq)]
pub enum SortColumn {
    /// By name (`ORDER BY note`, `ORDER BY t.note` — the trailing component).
    Name(String),
    /// By 1-based output position (`ORDER BY 2`).
    Position(usize),
}

/// A sort key resolved against the result schema: the column index to read, how
/// to decode it, and its direction / null placement.
#[derive(Debug)]
struct ResolvedKey {
    index: usize,
    kind: SortKind,
    descending: bool,
    nulls_first: bool,
}

/// The decoded form the router can compare — one per supported orderable type
/// family. An unsupported type never reaches here (rejected in [`sort_kind`]).
#[derive(Clone, Copy, Debug)]
enum SortKind {
    /// int2 / int4 / int8.
    Int,
    /// float4 / float8.
    Float,
    Bool,
}

/// The decoded value of a sort-key cell.
enum SortValue {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
}

fn reject(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

/// The merge kind for a column type, or `None` for a type this slice cannot sort
/// soundly across shards (text/numeric/uuid/timestamp/…). Text is excluded
/// because a byte-order merge disagrees with a non-C collation's on-shard order.
fn sort_kind(ty: &Type) -> Option<SortKind> {
    match *ty {
        Type::INT2 | Type::INT4 | Type::INT8 => Some(SortKind::Int),
        Type::FLOAT4 | Type::FLOAT8 => Some(SortKind::Float),
        Type::BOOL => Some(SortKind::Bool),
        _ => None,
    }
}

/// Resolve each [`SortKey`] against the result schema to a column index and
/// decode kind. Rejects (`0A000`) a key that names no output column, an
/// ambiguous name, an out-of-range position, or a column whose type this slice
/// cannot merge.
fn resolve_keys(schema: &[FieldInfo], keys: &[SortKey]) -> PgWireResult<Vec<ResolvedKey>> {
    keys.iter()
        .map(|key| {
            let index = match &key.column {
                SortColumn::Position(p) => p
                    .checked_sub(1)
                    .filter(|&i| i < schema.len())
                    .ok_or_else(|| {
                        reject(
                            "0A000",
                            format!("ORDER BY position {p} is out of range for the result"),
                        )
                    })?,
                SortColumn::Name(name) => {
                    let mut hits = schema.iter().enumerate().filter(|(_, f)| f.name() == name);
                    let first = hits.next().ok_or_else(|| {
                        reject(
                            "0A000",
                            format!(
                                "ORDER BY column {name:?} is not in the SELECT list; \
                                 sorting by a non-output column is not supported in a scatter read"
                            ),
                        )
                    })?;
                    if hits.next().is_some() {
                        return Err(reject(
                            "0A000",
                            format!("ORDER BY column {name:?} is ambiguous in the result"),
                        ));
                    }
                    first.0
                }
            };
            // The decode path parses the text wire form; a binary-format column
            // would be misread. Simple-query results are always text, so this only
            // guards against an unexpected backend rather than a real query shape.
            if schema[index].format() != FieldFormat::Text {
                return Err(reject(
                    "0A000",
                    "cross-shard ORDER BY on a binary-format column is not supported".to_owned(),
                ));
            }
            let ty = schema[index].datatype();
            let kind = sort_kind(ty).ok_or_else(|| {
                reject(
                    "0A000",
                    format!("cross-shard ORDER BY on a column of type {ty} is not supported yet"),
                )
            })?;
            Ok(ResolvedKey {
                index,
                kind,
                descending: key.descending,
                nulls_first: key.nulls_first,
            })
        })
        .collect()
}

/// Merge already-per-shard-sorted `rows` into one globally ordered stream
/// matching `keys`. The sort columns are decoded once per row, then the combined
/// set is ordered — so the result is correct even if a shard's local order were
/// somehow off. Returns `0A000` if `keys` cannot be resolved against `schema`.
pub fn merge_ordered(
    schema: &[FieldInfo],
    rows: Vec<DataRow>,
    keys: &[SortKey],
) -> PgWireResult<Vec<DataRow>> {
    let resolved = resolve_keys(schema, keys)?;
    let mut decoded: Vec<(Vec<SortValue>, DataRow)> = rows
        .into_iter()
        .map(|row| {
            let values = resolved
                .iter()
                .map(|k| decode(field_bytes(&row, k.index)?, k.kind))
                .collect::<PgWireResult<Vec<_>>>()?;
            Ok((values, row))
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    // A stable sort keeps rows equal on all keys in their arrival order.
    decoded.sort_by(|(a, _), (b, _)| cmp_rows(a, b, &resolved));
    Ok(decoded.into_iter().map(|(_, row)| row).collect())
}

/// Compare two rows' decoded key tuples left to right, first difference wins.
fn cmp_rows(a: &[SortValue], b: &[SortValue], keys: &[ResolvedKey]) -> Ordering {
    for (i, key) in keys.iter().enumerate() {
        let ord = cmp_value(&a[i], &b[i], key.descending, key.nulls_first);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Order two decoded values as PostgreSQL would: NULL placement is independent of
/// the ASC/DESC direction, and present values are reversed when descending.
fn cmp_value(a: &SortValue, b: &SortValue, descending: bool, nulls_first: bool) -> Ordering {
    use SortValue::{Bool, Float, Int, Null};
    let present = match (a, b) {
        (Null, Null) => return Ordering::Equal,
        (Null, _) => {
            return if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (_, Null) => {
            return if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (Int(x), Int(y)) => x.cmp(y),
        (Bool(x), Bool(y)) => x.cmp(y),
        (Float(x), Float(y)) => cmp_f64(*x, *y),
        // The schema agrees across shards, so both sides are the same variant;
        // guard defensively rather than panic on the impossible.
        _ => Ordering::Equal,
    };
    if descending {
        present.reverse()
    } else {
        present
    }
}

/// PostgreSQL float ordering: `NaN` sorts greater than every number (and above
/// `Infinity`), all `NaN`s are equal, and `-0.0` equals `0.0`.
fn cmp_f64(x: f64, y: f64) -> Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        // Neither is NaN, so a total order exists.
        _ => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
    }
}

/// Decode a sort-key cell's raw text bytes (`None` = SQL NULL) by its kind. A
/// parse failure is impossible for bytes PostgreSQL produced, so it is an
/// internal error rather than silently mis-sorted data.
fn decode(bytes: Option<&[u8]>, kind: SortKind) -> PgWireResult<SortValue> {
    let Some(bytes) = bytes else {
        return Ok(SortValue::Null);
    };
    let text = std::str::from_utf8(bytes)
        .map_err(|e| reject("XX000", format!("non-UTF-8 sort key from backend: {e}")))?;
    Ok(match kind {
        SortKind::Int => SortValue::Int(text.parse().map_err(|e| {
            reject(
                "XX000",
                format!("unparsable integer sort key {text:?}: {e}"),
            )
        })?),
        SortKind::Float => SortValue::Float(
            text.parse()
                .map_err(|e| reject("XX000", format!("unparsable float sort key {text:?}: {e}")))?,
        ),
        SortKind::Bool => SortValue::Bool(match text {
            "t" => true,
            "f" => false,
            other => {
                return Err(reject(
                    "XX000",
                    format!("unparsable boolean sort key {other:?}"),
                ));
            }
        }),
    })
}

/// The raw text bytes of column `idx` in `row`, `Ok(None)` for a SQL NULL cell.
/// Walks the wire row body: each field is a big-endian `i32` length (`-1` = NULL)
/// followed by that many bytes. A row that ends before `idx`, carries a length
/// that overruns the buffer, or states a length below `-1` is malformed — this
/// fails closed (`XX000`) rather than mistaking a truncated cell for a NULL that
/// would silently sort into the wrong place.
fn field_bytes(row: &DataRow, idx: usize) -> PgWireResult<Option<&[u8]>> {
    let data: &[u8] = &row.data;
    let mut offset = 0usize;
    for field in 0..=idx {
        let len_end = offset.checked_add(4).ok_or_else(malformed_row)?;
        let len_bytes = data.get(offset..len_end).ok_or_else(malformed_row)?;
        let len = i32::from_be_bytes(len_bytes.try_into().map_err(|_| malformed_row())?);
        offset = len_end;
        if len == -1 {
            if field == idx {
                return Ok(None);
            }
            // A NULL field carries no value bytes; move to the next field.
        } else if len < -1 {
            return Err(malformed_row());
        } else {
            let val_end = offset
                .checked_add(len as usize)
                .filter(|&end| end <= data.len())
                .ok_or_else(malformed_row)?;
            if field == idx {
                return Ok(Some(&data[offset..val_end]));
            }
            offset = val_end;
        }
    }
    // The schema promised a column at `idx` the row does not contain.
    Err(malformed_row())
}

fn malformed_row() -> PgWireError {
    reject(
        "XX000",
        "malformed DataRow from backend: field is truncated or out of range".to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::api::results::{DataRowEncoder, FieldFormat};
    use std::sync::Arc;

    fn field(name: &str, ty: Type) -> FieldInfo {
        FieldInfo::new(name.to_owned(), None, None, ty, FieldFormat::Text)
    }

    /// Build a text-format DataRow from optional string cells.
    fn row(cells: &[Option<&str>]) -> DataRow {
        let schema = Arc::new(vec![field("c", Type::TEXT); cells.len()]);
        let mut encoder = DataRowEncoder::new(schema);
        for cell in cells {
            encoder.encode_field(cell).unwrap();
        }
        encoder.take_row()
    }

    #[test]
    fn field_bytes_walks_to_the_column_including_nulls() {
        let r = row(&[Some("9"), None, Some("hello")]);
        assert_eq!(field_bytes(&r, 0).unwrap(), Some(b"9".as_slice()));
        assert_eq!(field_bytes(&r, 1).unwrap(), None); // SQL NULL
        assert_eq!(field_bytes(&r, 2).unwrap(), Some(b"hello".as_slice()));
    }

    #[test]
    fn field_bytes_fails_closed_on_a_missing_or_truncated_field() {
        // The row has two cells; asking for a third must error, not read as NULL —
        // otherwise a short row would silently sort under NULL ordering.
        let short = row(&[Some("1"), Some("2")]);
        assert!(field_bytes(&short, 2).is_err());
        // A field whose declared length overruns the buffer is malformed.
        let mut truncated = row(&[Some("hello")]);
        truncated.data.truncate(6); // 4-byte length says 5, only 2 value bytes remain
        assert!(field_bytes(&truncated, 0).is_err());
    }

    #[test]
    fn bool_decode_rejects_non_boolean_text() {
        assert!(matches!(
            decode(Some(b"t"), SortKind::Bool).unwrap(),
            SortValue::Bool(true)
        ));
        assert!(matches!(
            decode(Some(b"f"), SortKind::Bool).unwrap(),
            SortValue::Bool(false)
        ));
        // Anything else fails closed rather than decoding as `false`.
        assert!(decode(Some(b"x"), SortKind::Bool).is_err());
        assert!(decode(Some(b"true"), SortKind::Bool).is_err());
    }

    #[test]
    fn integers_sort_numerically_not_by_bytes() {
        // The headline trap: by bytes "10" < "9"; numerically 10 > 9.
        let ten = decode(Some(b"10"), SortKind::Int).unwrap();
        let nine = decode(Some(b"9"), SortKind::Int).unwrap();
        assert_eq!(cmp_value(&ten, &nine, false, false), Ordering::Greater);
        assert_eq!(cmp_value(&ten, &nine, true, false), Ordering::Less); // DESC
    }

    #[test]
    fn nulls_placement_is_independent_of_direction() {
        let null = SortValue::Null;
        let one = SortValue::Int(1);
        // NULLS FIRST: null sorts before a value regardless of ASC/DESC.
        assert_eq!(cmp_value(&null, &one, false, true), Ordering::Less);
        assert_eq!(cmp_value(&null, &one, true, true), Ordering::Less);
        // NULLS LAST: null sorts after a value regardless of ASC/DESC.
        assert_eq!(cmp_value(&null, &one, false, false), Ordering::Greater);
        assert_eq!(cmp_value(&null, &one, true, false), Ordering::Greater);
    }

    #[test]
    fn floats_order_like_postgres_with_nan_greatest() {
        let nan = decode(Some(b"NaN"), SortKind::Float).unwrap();
        let inf = decode(Some(b"Infinity"), SortKind::Float).unwrap();
        let one = decode(Some(b"1.5"), SortKind::Float).unwrap();
        assert_eq!(cmp_value(&nan, &inf, false, false), Ordering::Greater);
        assert_eq!(cmp_value(&nan, &nan, false, false), Ordering::Equal);
        assert_eq!(cmp_value(&one, &inf, false, false), Ordering::Less);
    }

    #[test]
    fn unsupported_sort_types_are_rejected() {
        assert!(sort_kind(&Type::INT4).is_some());
        assert!(sort_kind(&Type::FLOAT8).is_some());
        assert!(sort_kind(&Type::BOOL).is_some());
        assert!(sort_kind(&Type::TEXT).is_none());
        assert!(sort_kind(&Type::NUMERIC).is_none());
        assert!(sort_kind(&Type::UUID).is_none());
        assert!(sort_kind(&Type::TIMESTAMP).is_none());
    }

    #[test]
    fn resolve_keys_maps_names_and_positions_and_rejects_the_rest() {
        let schema = vec![field("customer_id", Type::INT4), field("note", Type::TEXT)];
        // By name and by position both resolve to the int column.
        let by_name = resolve_keys(
            &schema,
            &[SortKey {
                column: SortColumn::Name("customer_id".into()),
                descending: false,
                nulls_first: false,
            }],
        )
        .unwrap();
        assert_eq!(by_name[0].index, 0);
        let by_pos = resolve_keys(
            &schema,
            &[SortKey {
                column: SortColumn::Position(1),
                descending: true,
                nulls_first: true,
            }],
        )
        .unwrap();
        assert_eq!(by_pos[0].index, 0);
        // A text sort column, an out-of-range position, and an unknown name all reject.
        for key in [
            SortColumn::Name("note".into()),
            SortColumn::Position(9),
            SortColumn::Name("missing".into()),
        ] {
            let err = resolve_keys(
                &schema,
                &[SortKey {
                    column: key,
                    descending: false,
                    nulls_first: false,
                }],
            )
            .unwrap_err();
            assert!(matches!(err, PgWireError::UserError(_)));
        }
    }

    #[test]
    fn merge_orders_rows_across_shards_numerically() {
        let schema = vec![field("id", Type::INT4)];
        // Rows as they might arrive interleaved from two shards.
        let rows = vec![
            row(&[Some("2")]),
            row(&[Some("100")]),
            row(&[Some("9")]),
            row(&[Some("10")]),
        ];
        let keys = [SortKey {
            column: SortColumn::Position(1),
            descending: false,
            nulls_first: false,
        }];
        let merged = merge_ordered(&schema, rows, &keys).unwrap();
        let ids: Vec<i64> = merged
            .iter()
            .map(|r| {
                std::str::from_utf8(field_bytes(r, 0).unwrap().unwrap())
                    .unwrap()
                    .parse()
                    .unwrap()
            })
            .collect();
        assert_eq!(ids, vec![2, 9, 10, 100]);
    }
}
