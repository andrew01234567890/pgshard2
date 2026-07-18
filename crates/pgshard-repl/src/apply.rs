//! Transactional apply of a decoded logical-replication stream to a target
//! database, with an exactly-once checkpoint.
//!
//! The applier consumes decoded [`LogicalRepMsg`]s (from the client + decoder),
//! buffers each source transaction, and at its commit applies the changes to the
//! target **and** advances a checkpoint row (`pgshard.repl_progress`) in the same
//! target transaction. On restart it loads that checkpoint and skips any commit
//! at or before it — so a slot replay after a crash re-applies exactly the
//! un-committed tail, no loss and no duplication. The consumer feeds the
//! checkpoint back to the source via [`crate::client::ReplicationClient::confirm`]
//! so the slot never advances past the durable position.
//!
//! Inserts, updates, and deletes are applied (updates and deletes match by the
//! table's replica-identity key). Truncate and streamed/two-phase transactions
//! are rejected (fail closed) rather than silently skipped, so the target never
//! diverges; those, plus replication origins for loop prevention, are follow-ups.

use std::collections::HashMap;

use pgshard_core::Lsn;
use thiserror::Error;
use tokio_postgres::Client;

use crate::pgoutput::{LogicalRepMsg, Oid, TupleColumn};

/// Why applying a change failed.
#[derive(Debug, Error)]
pub enum ApplyError {
    #[error("target database error: {0}")]
    Db(#[from] tokio_postgres::Error),
    #[error("change references unknown relation oid {0} (no Relation message seen)")]
    UnknownRelation(Oid),
    #[error("relation {relation} has {expected} columns but the row has {actual}")]
    ColumnCountMismatch {
        relation: String,
        expected: usize,
        actual: usize,
    },
    #[error("non-UTF-8 value in a text column")]
    InvalidUtf8,
    #[error("a change affected {affected} target rows, expected exactly 1 (target diverged)")]
    RowCountMismatch { affected: u64 },
    #[error("{0} is not supported yet")]
    Unsupported(&'static str),
}

type Result<T> = std::result::Result<T, ApplyError>;

/// A published relation's identity, learned from a Relation message.
struct RelationInfo {
    schema: String,
    table: String,
    columns: Vec<String>,
    /// `relreplident`: `d` default (PK), `i` index, `f` full, `n` nothing.
    replica_identity: u8,
    /// Indices into `columns` of the replica-identity key columns — the `WHERE`
    /// of an update or delete.
    key_columns: Vec<usize>,
}

/// A column value buffered for apply. `Text` is the pgoutput text form, applied
/// as an unknown-typed literal the target column coerces back to its type;
/// `Unchanged` is a TOASTed value an update did not ship (the stored value stays).
enum Cell {
    Null,
    Text(Vec<u8>),
    Unchanged,
}

/// One buffered change awaiting its transaction's commit.
enum PendingChange {
    Insert {
        rel_oid: Oid,
        new: Vec<Cell>,
    },
    Update {
        rel_oid: Oid,
        /// The tuple to read the old key from: the `K`/`O` sub-message, or the new
        /// tuple when the key did not change.
        key_source: Vec<Cell>,
        new: Vec<Cell>,
    },
    Delete {
        rel_oid: Oid,
        key_source: Vec<Cell>,
    },
}

/// Applies a decoded logical-replication stream to a target database.
pub struct Applier {
    target: Client,
    relations: HashMap<Oid, RelationInfo>,
    consumer: String,
    checkpoint: Lsn,
    /// The durable end of the last applied (or replay-skipped) transaction —
    /// the position to acknowledge to the server. Distinct from `checkpoint`
    /// (the commit LSN used for de-duplication): acknowledging only the commit
    /// LSN leaves the final transaction eternally re-sendable and pins the
    /// slot's WAL horizon behind what is actually durable here.
    ack: Lsn,
    pending: Vec<PendingChange>,
}

impl Applier {
    /// Prepare an applier against `target` for the given `consumer` id, creating
    /// the progress table if needed and loading any prior checkpoint.
    ///
    /// The `target` role must be allowed to set `session_replication_role`
    /// (superuser, or `GRANT SET ON PARAMETER session_replication_role`): the
    /// session runs under replica semantics so the target's ordinary triggers
    /// and rules do not re-fire on replicated rows. A role without that
    /// privilege fails here, before anything is applied.
    pub async fn new(target: Client, consumer: impl Into<String>) -> Result<Self> {
        let consumer = consumer.into();
        target
            .batch_execute(
                // Pin the settings the literal quoting depends on: with
                // standard_conforming_strings on, only the single quote is special
                // in a literal, so quote_literal is a complete escape. UTF-8 keeps
                // the applied text bytes matching the source's, and DateStyle=ISO
                // parses the source's pinned ISO output unambiguously.
                // session_replication_role=replica keeps the target's ordinary
                // triggers and rules from re-firing on replicated rows — the
                // source already materialized their effects, so re-running them
                // would double-apply side effects. Requires a role permitted to
                // set it (superuser / the agent's role), matching PostgreSQL's
                // own logical-replication apply workers.
                "SET session_replication_role = replica;
                 SET standard_conforming_strings = on;
                 SET client_encoding = 'UTF8';
                 SET DateStyle = 'ISO';
                 SET IntervalStyle = 'postgres';
                 CREATE SCHEMA IF NOT EXISTS pgshard;
                 CREATE TABLE IF NOT EXISTS pgshard.repl_progress (
                     consumer text PRIMARY KEY,
                     lsn bigint NOT NULL,
                     end_lsn bigint NOT NULL DEFAULT 0
                 );
                 ALTER TABLE pgshard.repl_progress
                     ADD COLUMN IF NOT EXISTS end_lsn bigint NOT NULL DEFAULT 0",
            )
            .await?;
        let (checkpoint, ack) = match target
            .query_opt(
                "SELECT lsn, end_lsn FROM pgshard.repl_progress WHERE consumer = $1",
                &[&consumer],
            )
            .await?
        {
            // try_get, not get: a pre-existing progress table with the wrong
            // column type errors rather than panicking.
            Some(row) => {
                let lsn = Lsn(row.try_get::<_, i64>(0)? as u64);
                let end = Lsn(row.try_get::<_, i64>(1)? as u64);
                // A pre-migration row has end_lsn 0; never acknowledge below the
                // commit LSN we know is durable.
                (lsn, end.max(lsn))
            }
            None => (Lsn(0), Lsn(0)),
        };
        Ok(Applier {
            target,
            relations: HashMap::new(),
            consumer,
            checkpoint,
            ack,
            pending: Vec::new(),
        })
    }

    /// The last durably-applied commit LSN — the de-duplication watermark. For
    /// what to report to the server, use [`Self::ack_lsn`].
    pub fn checkpoint(&self) -> Lsn {
        self.checkpoint
    }

    /// The position to feed [`crate::client::ReplicationClient::confirm`]: the
    /// durable end of the last applied (or already-applied and skipped)
    /// transaction. Acknowledging the end rather than the commit LSN lets the
    /// slot release the transaction's WAL and stops it re-sending an
    /// already-applied final transaction on every reconnect.
    pub fn ack_lsn(&self) -> Lsn {
        self.ack
    }

    /// Handle one decoded message: track relations, buffer changes, and apply at
    /// commit. Rejects anything it cannot apply soundly rather than skipping it.
    pub async fn handle(&mut self, msg: &LogicalRepMsg<'_>) -> Result<()> {
        match msg {
            LogicalRepMsg::Relation(r) => {
                self.relations.insert(
                    r.oid,
                    RelationInfo {
                        schema: r.namespace.to_owned(),
                        table: r.name.to_owned(),
                        columns: r.columns.iter().map(|c| c.name.to_owned()).collect(),
                        replica_identity: r.replica_identity,
                        key_columns: r
                            .columns
                            .iter()
                            .enumerate()
                            .filter(|(_, c)| c.is_key())
                            .map(|(i, _)| i)
                            .collect(),
                    },
                );
                Ok(())
            }
            LogicalRepMsg::Begin(_) => {
                self.pending.clear();
                Ok(())
            }
            LogicalRepMsg::Insert(insert) => {
                self.pending.push(PendingChange::Insert {
                    rel_oid: insert.rel_oid,
                    new: tuple_cells(&insert.new_tuple)?,
                });
                Ok(())
            }
            LogicalRepMsg::Update(update) => {
                // The old key comes from the K (index/changed identity) or O (full)
                // sub-message when present, else the new tuple (default identity,
                // key unchanged).
                let key_source = match (&update.key, &update.old) {
                    (Some(key), _) => tuple_cells(key)?,
                    (None, Some(old)) => tuple_cells(old)?,
                    (None, None) => tuple_cells(&update.new_tuple)?,
                };
                self.pending.push(PendingChange::Update {
                    rel_oid: update.rel_oid,
                    key_source,
                    new: tuple_cells(&update.new_tuple)?,
                });
                Ok(())
            }
            LogicalRepMsg::Delete(delete) => {
                let key_source = match (&delete.key, &delete.old) {
                    (Some(key), _) => tuple_cells(key)?,
                    (None, Some(old)) => tuple_cells(old)?,
                    (None, None) => {
                        return Err(ApplyError::Unsupported(
                            "delete without a replica-identity key",
                        ));
                    }
                };
                self.pending.push(PendingChange::Delete {
                    rel_oid: delete.rel_oid,
                    key_source,
                });
                Ok(())
            }
            LogicalRepMsg::Commit(commit) => self.commit(commit.commit_lsn, commit.end_lsn).await,
            // Metadata the apply path does not act on directly.
            LogicalRepMsg::Origin(_) | LogicalRepMsg::Type(_) | LogicalRepMsg::Message(_) => Ok(()),
            // Fail closed rather than silently diverge — follow-ups.
            LogicalRepMsg::Truncate(_) => Err(ApplyError::Unsupported("TRUNCATE apply")),
            _ => Err(ApplyError::Unsupported(
                "streamed or two-phase transactions",
            )),
        }
    }

    async fn commit(&mut self, commit_lsn: Lsn, end_lsn: Lsn) -> Result<()> {
        // Exactly-once: skip a commit at or before the durable checkpoint — it was
        // already applied and the slot replayed it after a restart. The comparison
        // must be `<=`, not `<`: PostgreSQL only skips a transaction whose commit
        // LSN is strictly below the slot's confirmed-flush, so the last-applied
        // transaction (LSN == checkpoint) is re-sent and must be de-duplicated here.
        if commit_lsn <= self.checkpoint {
            // Already durably applied — still safe (and necessary) to acknowledge
            // its end so the replay stops recurring.
            self.ack = self.ack.max(end_lsn);
            self.pending.clear();
            return Ok(());
        }
        // Build every statement before opening the transaction so the relation
        // map is only borrowed while the target connection is not.
        let mut statements = Vec::with_capacity(self.pending.len());
        for change in &self.pending {
            match change {
                PendingChange::Insert { rel_oid, new } => {
                    statements.push(build_insert(self.relation(*rel_oid)?, new)?);
                }
                PendingChange::Update {
                    rel_oid,
                    key_source,
                    new,
                } => {
                    // An update that changed only unshipped-TOAST columns has
                    // nothing to set; skip it.
                    if let Some(sql) = build_update(self.relation(*rel_oid)?, key_source, new)? {
                        statements.push(sql);
                    }
                }
                PendingChange::Delete {
                    rel_oid,
                    key_source,
                } => {
                    statements.push(build_delete(self.relation(*rel_oid)?, key_source)?);
                }
            }
        }

        let txn = self.target.transaction().await?;
        for sql in &statements {
            // Each built statement targets exactly one row (insert, or update/delete
            // matched by the replica-identity key). A different count means the
            // target diverged from the source — a missing row, or a duplicate under
            // a key that should be unique — so surface it instead of hiding it.
            let affected = txn.execute(sql.as_str(), &[]).await?;
            if affected != 1 {
                return Err(ApplyError::RowCountMismatch { affected });
            }
        }
        // Advance the checkpoint in the SAME transaction as the changes: either
        // both land or neither does, which is what makes the apply exactly-once.
        txn.execute(
            "INSERT INTO pgshard.repl_progress (consumer, lsn, end_lsn) VALUES ($1, $2, $3) \
             ON CONFLICT (consumer) DO UPDATE SET lsn = EXCLUDED.lsn, end_lsn = EXCLUDED.end_lsn",
            &[&self.consumer, &(commit_lsn.0 as i64), &(end_lsn.0 as i64)],
        )
        .await?;
        txn.commit().await?;

        self.checkpoint = commit_lsn;
        self.ack = self.ack.max(end_lsn);
        self.pending.clear();
        Ok(())
    }

    fn relation(&self, oid: Oid) -> Result<&RelationInfo> {
        self.relations
            .get(&oid)
            .ok_or(ApplyError::UnknownRelation(oid))
    }
}

fn tuple_cells(tuple: &crate::pgoutput::TupleData) -> Result<Vec<Cell>> {
    tuple.columns.iter().map(cell_of).collect()
}

fn cell_of(column: &TupleColumn) -> Result<Cell> {
    match column {
        TupleColumn::Null => Ok(Cell::Null),
        TupleColumn::Text(bytes) => Ok(Cell::Text(bytes.to_vec())),
        TupleColumn::UnchangedToast => Ok(Cell::Unchanged),
        TupleColumn::Binary(_) => Err(ApplyError::Unsupported("binary-format value")),
    }
}

/// Error unless `cells` has one entry per relation column.
fn check_len(relation: &RelationInfo, cells: &[Cell]) -> Result<()> {
    if cells.len() != relation.columns.len() {
        return Err(ApplyError::ColumnCountMismatch {
            relation: format!("{}.{}", relation.schema, relation.table),
            expected: relation.columns.len(),
            actual: cells.len(),
        });
    }
    Ok(())
}

fn build_insert(relation: &RelationInfo, values: &[Cell]) -> Result<String> {
    check_len(relation, values)?;
    let columns = relation
        .columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let rendered = values
        .iter()
        .map(render_cell)
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(format!(
        "INSERT INTO {}.{} ({}) VALUES ({})",
        quote_ident(&relation.schema),
        quote_ident(&relation.table),
        columns,
        rendered,
    ))
}

/// Build an `UPDATE`, or `None` when the row image shipped no changed values
/// (only unchanged-TOAST columns) and there is nothing to set.
fn build_update(
    relation: &RelationInfo,
    key_source: &[Cell],
    new: &[Cell],
) -> Result<Option<String>> {
    check_len(relation, key_source)?;
    check_len(relation, new)?;
    let mut assignments = Vec::new();
    for (name, cell) in relation.columns.iter().zip(new) {
        // A TOASTed value the update did not ship keeps its stored value.
        if matches!(cell, Cell::Unchanged) {
            continue;
        }
        assignments.push(format!("{} = {}", quote_ident(name), render_cell(cell)?));
    }
    if assignments.is_empty() {
        return Ok(None);
    }
    Ok(Some(format!(
        "UPDATE {}.{} SET {} WHERE {}",
        quote_ident(&relation.schema),
        quote_ident(&relation.table),
        assignments.join(", "),
        build_where(relation, key_source)?,
    )))
}

fn build_delete(relation: &RelationInfo, key_source: &[Cell]) -> Result<String> {
    check_len(relation, key_source)?;
    Ok(format!(
        "DELETE FROM {}.{} WHERE {}",
        quote_ident(&relation.schema),
        quote_ident(&relation.table),
        build_where(relation, key_source)?,
    ))
}

/// The `WHERE` matching a row by its replica-identity key columns.
fn build_where(relation: &RelationInfo, key_source: &[Cell]) -> Result<String> {
    // Only a key-based identity (default = PK, or an explicit index) matches a
    // single row unambiguously. REPLICA IDENTITY FULL (`f`) matches on the whole
    // old row — which double-matches duplicate rows — and NOTHING (`n`) gives no
    // key at all; reject both rather than risk changing the wrong rows.
    if !matches!(relation.replica_identity, b'd' | b'i') {
        return Err(ApplyError::Unsupported(
            "update/delete requires REPLICA IDENTITY DEFAULT or an index (full/nothing not supported)",
        ));
    }
    if relation.key_columns.is_empty() {
        return Err(ApplyError::Unsupported(
            "update/delete on a table with no replica-identity key",
        ));
    }
    let mut conditions = Vec::with_capacity(relation.key_columns.len());
    for &i in &relation.key_columns {
        let condition = match &key_source[i] {
            // A key column is never null or an unshipped TOAST value.
            Cell::Null => return Err(ApplyError::Unsupported("null replica-identity key")),
            Cell::Unchanged => {
                return Err(ApplyError::Unsupported("unshipped replica-identity key"));
            }
            cell => format!(
                "{} = {}",
                quote_ident(&relation.columns[i]),
                render_cell(cell)?
            ),
        };
        conditions.push(condition);
    }
    Ok(conditions.join(" AND "))
}

fn render_cell(cell: &Cell) -> Result<String> {
    match cell {
        Cell::Null => Ok("NULL".to_owned()),
        Cell::Text(bytes) => {
            let text = std::str::from_utf8(bytes).map_err(|_| ApplyError::InvalidUtf8)?;
            Ok(quote_literal(text))
        }
        Cell::Unchanged => Err(ApplyError::Unsupported(
            "unshipped TOAST value in a full row",
        )),
    }
}

/// Quote an identifier: wrap in double quotes and double any embedded ones.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a string literal. `standard_conforming_strings` is on by default, so
/// only the single quote needs doubling; the value is applied as an unknown-typed
/// literal the target column coerces via its input function.
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::{
        Cell, RelationInfo, build_delete, build_insert, build_update, quote_ident, quote_literal,
    };

    fn text(s: &str) -> Cell {
        Cell::Text(s.as_bytes().to_vec())
    }

    /// A two-column `public.orders(id, note)` with `id` as the key.
    fn orders(replica_identity: u8) -> RelationInfo {
        RelationInfo {
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            columns: vec!["id".to_owned(), "note".to_owned()],
            replica_identity,
            key_columns: if replica_identity == b'n' {
                vec![]
            } else {
                vec![0]
            },
        }
    }

    #[test]
    fn quoting_neutralizes_injection() {
        assert_eq!(quote_literal("hi"), "'hi'");
        assert_eq!(quote_literal("O'Brien"), "'O''Brien'");
        assert_eq!(
            quote_literal("'); DROP TABLE t; --"),
            "'''); DROP TABLE t; --'"
        );
        assert_eq!(quote_ident("orders"), "\"orders\"");
        assert_eq!(quote_ident("weird\"name"), "\"weird\"\"name\"");
    }

    #[test]
    fn insert_names_every_column() {
        let sql = build_insert(&orders(b'd'), &[text("1"), text("hi")]).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"public\".\"orders\" (\"id\", \"note\") VALUES ('1', 'hi')"
        );
    }

    #[test]
    fn update_sets_shipped_columns_and_matches_by_key() {
        // Default identity, key unchanged: key_source is the new tuple.
        let sql = build_update(
            &orders(b'd'),
            &[text("1"), text("z")],
            &[text("1"), text("z")],
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            sql,
            "UPDATE \"public\".\"orders\" SET \"id\" = '1', \"note\" = 'z' WHERE \"id\" = '1'"
        );
    }

    #[test]
    fn update_skips_unchanged_toast_and_no_op_updates() {
        // A shipped 'note' change with an unchanged-TOAST 'id' would not happen for
        // a key, but exercises SET omission on the second column.
        let sql = build_update(
            &orders(b'd'),
            &[text("1"), text("z")],
            &[text("1"), Cell::Unchanged],
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            sql,
            "UPDATE \"public\".\"orders\" SET \"id\" = '1' WHERE \"id\" = '1'"
        );
        // All columns unchanged → nothing to set → no statement.
        let none = build_update(
            &orders(b'd'),
            &[text("1"), text("z")],
            &[Cell::Unchanged, Cell::Unchanged],
        )
        .unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn delete_matches_by_key() {
        let sql = build_delete(&orders(b'd'), &[text("1"), Cell::Null]).unwrap();
        assert_eq!(sql, "DELETE FROM \"public\".\"orders\" WHERE \"id\" = '1'");
    }

    #[test]
    fn multi_column_key_is_anded() {
        let mut rel = orders(b'i');
        rel.columns = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        rel.key_columns = vec![0, 1];
        let sql = build_delete(&rel, &[text("x"), text("y"), text("z")]).unwrap();
        assert_eq!(
            sql,
            "DELETE FROM \"public\".\"orders\" WHERE \"a\" = 'x' AND \"b\" = 'y'"
        );
    }

    #[test]
    fn unsupported_or_missing_keys_fail_closed() {
        // REPLICA IDENTITY FULL / NOTHING are rejected.
        assert!(build_delete(&orders(b'f'), &[text("1"), text("hi")]).is_err());
        assert!(build_delete(&orders(b'n'), &[text("1"), text("hi")]).is_err());
        // A NULL or unshipped-TOAST key value is rejected.
        assert!(build_delete(&orders(b'd'), &[Cell::Null, text("hi")]).is_err());
        assert!(build_delete(&orders(b'd'), &[Cell::Unchanged, text("hi")]).is_err());
    }
}
