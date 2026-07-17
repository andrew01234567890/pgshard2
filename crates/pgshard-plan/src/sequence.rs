//! Filling omitted global-sequence columns into an INSERT.
//!
//! A table can bind a column to a global sequence (e.g. `id`). When a client
//! omits that column, the router allocates the next id(s) from the sequence and
//! rewrites the statement to include them — the sharded equivalent of a serial
//! default, but with ids that never collide across shards.
//!
//! Detection ([`insert_sequence_injections`]) is pure and needs only the
//! vschema; the router allocates the ids and calls [`rewrite_insert`] to splice
//! them in. Only an INSERT with an explicit column list and literal `VALUES`
//! rows is rewritten; anything else (positional, `INSERT ... SELECT`, or a
//! column already listed) is left untouched — and when it involves the shard
//! key it was already rejected upstream, so a rewritten INSERT always carries
//! its shard key and routes by it.

use std::collections::HashSet;

use pg_query::NodeEnum;
use pg_query::protobuf::{AConst, Float, InsertStmt, Node, ParseResult, ResTarget, a_const};
use thiserror::Error;

use pgshard_core::{TableDef, VSchema};
use pgshard_sql::Parsed;

use crate::extract::range_var_table;

/// A sequence-bound column omitted from an INSERT, to be filled by the router.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceInjection {
    pub column: String,
    pub sequence: String,
}

/// The (column, sequence) pairs an INSERT omits and the router should fill.
/// Empty unless the statement is an INSERT into a sharded table with sequence
/// bindings, an explicit column list, and literal `VALUES` rows, where at least
/// one bound column is not listed.
pub fn insert_sequence_injections(node: &NodeEnum, vschema: &VSchema) -> Vec<SequenceInjection> {
    let NodeEnum::InsertStmt(ins) = node else {
        return Vec::new();
    };
    let Some(rv) = ins.relation.as_ref() else {
        return Vec::new();
    };
    let sequences = match vschema.get(&range_var_table(rv)) {
        Some(TableDef::Sharded { sequences, .. }) => sequences,
        _ => return Vec::new(),
    };
    if sequences.is_empty() || !has_value_rows(ins) {
        return Vec::new();
    }
    let listed: HashSet<&str> = ins
        .cols
        .iter()
        .filter_map(|c| match c.node.as_ref() {
            Some(NodeEnum::ResTarget(rt)) => Some(rt.name.as_str()),
            _ => None,
        })
        .collect();
    // A positional INSERT (no column list) gives no place to add a column, and
    // one into a sharded table was already rejected upstream.
    if listed.is_empty() {
        return Vec::new();
    }
    sequences
        .iter()
        .filter(|b| !listed.contains(b.column.as_str()))
        .map(|b| SequenceInjection {
            column: b.column.clone(),
            sequence: b.sequence.clone(),
        })
        .collect()
}

/// The number of `VALUES` rows in an INSERT, so the router allocates one id per
/// row. Zero for anything that is not an INSERT with a `VALUES` list.
pub fn value_row_count(node: &NodeEnum) -> usize {
    let NodeEnum::InsertStmt(ins) = node else {
        return 0;
    };
    match ins.select_stmt.as_deref().and_then(|n| n.node.as_ref()) {
        Some(NodeEnum::SelectStmt(s)) => s.values_lists.len(),
        _ => 0,
    }
}

fn has_value_rows(ins: &InsertStmt) -> bool {
    matches!(
        ins.select_stmt.as_deref().and_then(|n| n.node.as_ref()),
        Some(NodeEnum::SelectStmt(s)) if !s.values_lists.is_empty()
    )
}

/// One column to splice into an INSERT, with the id for each `VALUES` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InjectedColumn {
    pub column: String,
    /// One id per `VALUES` row, in row order.
    pub ids: Vec<i64>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RewriteError {
    #[error("statement is not a single INSERT with VALUES rows")]
    NotInjectable,
    #[error("{ids} ids for column {column:?} but the INSERT has {rows} value rows")]
    RowCountMismatch {
        column: String,
        ids: usize,
        rows: usize,
    },
    #[error("deparse failed: {0}")]
    Deparse(String),
}

/// Rewrite the single INSERT in `parsed` to add each injected column and its
/// per-row id, returning the new SQL. Ids must already be allocated (one per
/// `VALUES` row). Fails without mutating anything on a shape mismatch, so the
/// router never forwards a half-rewritten statement.
pub fn rewrite_insert(
    parsed: &Parsed,
    injected: &[InjectedColumn],
) -> Result<String, RewriteError> {
    let mut pb: ParseResult = parsed.result().protobuf.clone();
    let [stmt] = pb.stmts.as_mut_slice() else {
        return Err(RewriteError::NotInjectable);
    };
    let Some(NodeEnum::InsertStmt(ins)) = stmt.stmt.as_mut().and_then(|n| n.node.as_mut()) else {
        return Err(RewriteError::NotInjectable);
    };
    let Some(NodeEnum::SelectStmt(sel)) =
        ins.select_stmt.as_deref_mut().and_then(|n| n.node.as_mut())
    else {
        return Err(RewriteError::NotInjectable);
    };
    let rows = sel.values_lists.len();
    if rows == 0 {
        return Err(RewriteError::NotInjectable);
    }
    // Validate every injection before mutating, so a mismatch leaves `pb`
    // untouched (it is a discarded clone anyway, but the function stays total).
    for inj in injected {
        if inj.ids.len() != rows {
            return Err(RewriteError::RowCountMismatch {
                column: inj.column.clone(),
                ids: inj.ids.len(),
                rows,
            });
        }
    }
    for inj in injected {
        ins.cols.push(res_target(&inj.column));
        for (row, id) in sel.values_lists.iter_mut().zip(&inj.ids) {
            let Some(NodeEnum::List(list)) = row.node.as_mut() else {
                return Err(RewriteError::NotInjectable);
            };
            list.items.push(int_const(*id));
        }
    }
    pg_query::deparse(&pb).map_err(|e| RewriteError::Deparse(e.to_string()))
}

fn res_target(name: &str) -> Node {
    Node {
        node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
            name: name.to_owned(),
            indirection: Vec::new(),
            val: None,
            location: -1,
        }))),
    }
}

/// A bare integer literal. `Integer.ival` is only i32, so a sequence id (i64,
/// routinely past i32) is emitted as a numeric-string `Float` const, which
/// deparses to the bare number for any i64.
fn int_const(id: i64) -> Node {
    Node {
        node: Some(NodeEnum::AConst(AConst {
            val: Some(a_const::Val::Fval(Float {
                fval: id.to_string(),
            })),
            isnull: false,
            location: -1,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgshard_core::{ScalarType, SequenceBinding, TableName};

    fn vschema() -> VSchema {
        let mut v = VSchema::default();
        v.insert(
            TableName::new("public", "orders"),
            TableDef::Sharded {
                shard_key_column: "customer_id".into(),
                shard_key_type: Some(ScalarType::Int),
                shard_function: "xxhash64_v1".into(),
                sequences: vec![SequenceBinding {
                    column: "id".into(),
                    sequence: "orders_id".into(),
                }],
            },
        )
        .unwrap();
        v.insert(
            TableName::new("public", "events"),
            TableDef::Sharded {
                shard_key_column: "customer_id".into(),
                shard_key_type: Some(ScalarType::Int),
                shard_function: "xxhash64_v1".into(),
                sequences: Vec::new(),
            },
        )
        .unwrap();
        v
    }

    fn node(sql: &str) -> Parsed {
        pgshard_sql::parse(sql).unwrap()
    }

    fn injections(sql: &str) -> Vec<SequenceInjection> {
        let parsed = node(sql);
        let n = parsed.result().protobuf.stmts[0]
            .stmt
            .as_ref()
            .unwrap()
            .node
            .as_ref()
            .unwrap();
        insert_sequence_injections(n, &vschema())
    }

    #[test]
    fn detects_an_omitted_sequence_column() {
        assert_eq!(
            injections("INSERT INTO orders (customer_id) VALUES (1)"),
            vec![SequenceInjection {
                column: "id".into(),
                sequence: "orders_id".into(),
            }]
        );
    }

    #[test]
    fn no_injection_when_the_column_is_listed() {
        assert!(injections("INSERT INTO orders (customer_id, id) VALUES (1, 5)").is_empty());
    }

    #[test]
    fn no_injection_for_tables_without_sequences_or_non_inserts() {
        assert!(injections("INSERT INTO events (customer_id) VALUES (1)").is_empty());
        assert!(injections("INSERT INTO widgets (customer_id) VALUES (1)").is_empty());
        // Positional and INSERT ... SELECT cannot be safely rewritten.
        assert!(injections("INSERT INTO orders VALUES (1, 2)").is_empty());
        assert!(injections("INSERT INTO orders (customer_id) SELECT id FROM t").is_empty());
    }

    #[test]
    fn rewrite_fills_one_id_per_row() {
        let parsed = node("INSERT INTO orders (customer_id, note) VALUES (1, 'a'), (2, 'b')");
        let sql = rewrite_insert(
            &parsed,
            &[InjectedColumn {
                column: "id".into(),
                ids: vec![100, 101],
            }],
        )
        .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO orders (customer_id, note, id) VALUES (1, 'a', 100), (2, 'b', 101)"
        );
    }

    #[test]
    fn rewrite_handles_ids_beyond_i32() {
        let parsed = node("INSERT INTO orders (customer_id) VALUES (1)");
        let sql = rewrite_insert(
            &parsed,
            &[InjectedColumn {
                column: "id".into(),
                ids: vec![i64::MAX],
            }],
        )
        .unwrap();
        assert_eq!(
            sql,
            format!(
                "INSERT INTO orders (customer_id, id) VALUES (1, {})",
                i64::MAX
            )
        );
    }

    #[test]
    fn rewrite_rejects_a_row_count_mismatch() {
        let parsed = node("INSERT INTO orders (customer_id) VALUES (1), (2)");
        let err = rewrite_insert(
            &parsed,
            &[InjectedColumn {
                column: "id".into(),
                ids: vec![100],
            }],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            RewriteError::RowCountMismatch {
                rows: 2,
                ids: 1,
                ..
            }
        ));
    }
}
