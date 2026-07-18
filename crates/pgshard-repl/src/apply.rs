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
//! Slice 1 applies inserts. Update, delete, and truncate are rejected (fail
//! closed) rather than silently skipped, so the target never diverges; they, plus
//! replication origins for loop prevention, are the next slice.

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
    #[error("{0} is not supported yet")]
    Unsupported(&'static str),
}

type Result<T> = std::result::Result<T, ApplyError>;

/// A published relation's identity, learned from a Relation message.
struct RelationInfo {
    schema: String,
    table: String,
    columns: Vec<String>,
}

/// A column value buffered for apply. Text is the pgoutput text form, applied as
/// an unknown-typed literal that the target column coerces back to its type.
enum Cell {
    Null,
    Text(Vec<u8>),
}

/// One buffered insert awaiting its transaction's commit.
struct PendingInsert {
    rel_oid: Oid,
    values: Vec<Cell>,
}

/// Applies a decoded logical-replication stream to a target database.
pub struct Applier {
    target: Client,
    relations: HashMap<Oid, RelationInfo>,
    consumer: String,
    checkpoint: Lsn,
    pending: Vec<PendingInsert>,
}

impl Applier {
    /// Prepare an applier against `target` for the given `consumer` id, creating
    /// the progress table if needed and loading any prior checkpoint.
    pub async fn new(target: Client, consumer: impl Into<String>) -> Result<Self> {
        let consumer = consumer.into();
        target
            .batch_execute(
                // Pin the settings the literal quoting depends on: with
                // standard_conforming_strings on, only the single quote is special
                // in a literal, so quote_literal is a complete escape. UTF-8 keeps
                // the applied text bytes matching the source's.
                "SET standard_conforming_strings = on;
                 SET client_encoding = 'UTF8';
                 CREATE SCHEMA IF NOT EXISTS pgshard;
                 CREATE TABLE IF NOT EXISTS pgshard.repl_progress (
                     consumer text PRIMARY KEY,
                     lsn bigint NOT NULL
                 )",
            )
            .await?;
        let checkpoint = match target
            .query_opt(
                "SELECT lsn FROM pgshard.repl_progress WHERE consumer = $1",
                &[&consumer],
            )
            .await?
        {
            // try_get, not get: a pre-existing progress table with the wrong
            // column type errors rather than panicking.
            Some(row) => Lsn(row.try_get::<_, i64>(0)? as u64),
            None => Lsn(0),
        };
        Ok(Applier {
            target,
            relations: HashMap::new(),
            consumer,
            checkpoint,
            pending: Vec::new(),
        })
    }

    /// The last durably-applied commit LSN. Feed this to
    /// [`crate::client::ReplicationClient::confirm`] so the slot only advances to
    /// what has been committed here.
    pub fn checkpoint(&self) -> Lsn {
        self.checkpoint
    }

    /// Handle one decoded message: track relations, buffer inserts, and apply at
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
                    },
                );
                Ok(())
            }
            LogicalRepMsg::Begin(_) => {
                self.pending.clear();
                Ok(())
            }
            LogicalRepMsg::Insert(insert) => {
                let values = insert
                    .new_tuple
                    .columns
                    .iter()
                    .map(cell_of)
                    .collect::<Result<Vec<_>>>()?;
                self.pending.push(PendingInsert {
                    rel_oid: insert.rel_oid,
                    values,
                });
                Ok(())
            }
            LogicalRepMsg::Commit(commit) => self.commit(commit.commit_lsn).await,
            // Metadata the apply path does not act on directly.
            LogicalRepMsg::Origin(_) | LogicalRepMsg::Type(_) | LogicalRepMsg::Message(_) => Ok(()),
            // Fail closed rather than silently diverge — next slice.
            LogicalRepMsg::Update(_) => Err(ApplyError::Unsupported("UPDATE apply")),
            LogicalRepMsg::Delete(_) => Err(ApplyError::Unsupported("DELETE apply")),
            LogicalRepMsg::Truncate(_) => Err(ApplyError::Unsupported("TRUNCATE apply")),
            _ => Err(ApplyError::Unsupported(
                "streamed or two-phase transactions",
            )),
        }
    }

    async fn commit(&mut self, commit_lsn: Lsn) -> Result<()> {
        // Exactly-once: skip a commit at or before the durable checkpoint — it was
        // already applied and the slot replayed it after a restart. The comparison
        // must be `<=`, not `<`: PostgreSQL only skips a transaction whose commit
        // LSN is strictly below the slot's confirmed-flush, so the last-applied
        // transaction (LSN == checkpoint) is re-sent and must be de-duplicated here.
        if commit_lsn <= self.checkpoint {
            self.pending.clear();
            return Ok(());
        }
        // Build every statement before opening the transaction so the relation
        // map is only borrowed while the target connection is not.
        let mut statements = Vec::with_capacity(self.pending.len());
        for insert in &self.pending {
            let relation = self
                .relations
                .get(&insert.rel_oid)
                .ok_or(ApplyError::UnknownRelation(insert.rel_oid))?;
            statements.push(build_insert(relation, &insert.values)?);
        }

        let txn = self.target.transaction().await?;
        for sql in &statements {
            txn.batch_execute(sql).await?;
        }
        // Advance the checkpoint in the SAME transaction as the changes: either
        // both land or neither does, which is what makes the apply exactly-once.
        txn.execute(
            "INSERT INTO pgshard.repl_progress (consumer, lsn) VALUES ($1, $2) \
             ON CONFLICT (consumer) DO UPDATE SET lsn = EXCLUDED.lsn",
            &[&self.consumer, &(commit_lsn.0 as i64)],
        )
        .await?;
        txn.commit().await?;

        self.checkpoint = commit_lsn;
        self.pending.clear();
        Ok(())
    }
}

fn cell_of(column: &TupleColumn) -> Result<Cell> {
    match column {
        TupleColumn::Null => Ok(Cell::Null),
        TupleColumn::Text(bytes) => Ok(Cell::Text(bytes.to_vec())),
        TupleColumn::UnchangedToast => Err(ApplyError::Unsupported("unchanged-TOAST value")),
        TupleColumn::Binary(_) => Err(ApplyError::Unsupported("binary-format value")),
    }
}

fn build_insert(relation: &RelationInfo, values: &[Cell]) -> Result<String> {
    if values.len() != relation.columns.len() {
        return Err(ApplyError::ColumnCountMismatch {
            relation: format!("{}.{}", relation.schema, relation.table),
            expected: relation.columns.len(),
            actual: values.len(),
        });
    }
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

fn render_cell(cell: &Cell) -> Result<String> {
    match cell {
        Cell::Null => Ok("NULL".to_owned()),
        Cell::Text(bytes) => {
            let text = std::str::from_utf8(bytes).map_err(|_| ApplyError::InvalidUtf8)?;
            Ok(quote_literal(text))
        }
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
    use super::{quote_ident, quote_literal};

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
}
