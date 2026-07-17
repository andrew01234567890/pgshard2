//! Idempotency for `ExecSchema`: the operator applies online-DDL and
//! database-provisioning statements through the agent, each tagged with an
//! operation id, and must be able to retry safely. This tracks operation ids
//! the same reserve/commit way the epoch guard does: an id is claimed before
//! the statement runs and only marked done after it succeeds, so a failed
//! statement is re-executed on retry rather than replaying a phantom success.
//!
//! This is the in-process core. Durable persistence of completed ids across a
//! restart (the proto's ≥7-day retention, and rejecting an expired-unknown id
//! rather than silently re-running it) needs a Postgres-backed op log and is a
//! follow-up.

use std::collections::HashMap;
use std::sync::Mutex;

/// What a claim tells the caller to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Claim {
    /// A fresh id: run the statement, then `mark_done`.
    Execute,
    /// An id whose identical statement already completed: return success without
    /// re-running.
    Replay,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("operation_id is required")]
    EmptyId,
    #[error("operation_id {0} was reused with different sql")]
    DifferentSql(String),
    #[error("operation_id {0} is already in flight")]
    InFlight(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    InFlight,
    Done,
    Failed,
}

#[derive(Clone)]
struct Op {
    sql: String,
    state: State,
}

#[derive(Default)]
pub struct SchemaLog {
    ops: Mutex<HashMap<String, Op>>,
}

impl SchemaLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserve `id` for `sql`. The `sql` binding is kept for the id's whole
    /// lifetime, so the same id with different sql is always rejected — even
    /// after a failure. A completed identical statement replays; a duplicate
    /// while the first is still running is rejected; a previously-failed
    /// identical statement re-executes.
    ///
    /// `sql` must be a single statement (the operator parses and guarantees
    /// this): a failed multi-statement batch that already committed part of its
    /// work cannot be re-executed safely, and this log cannot detect a partial
    /// commit.
    pub fn claim(&self, id: &str, sql: &str) -> Result<Claim, SchemaError> {
        if id.is_empty() {
            return Err(SchemaError::EmptyId);
        }
        let mut ops = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(op) = ops.get_mut(id) {
            if op.sql != sql {
                return Err(SchemaError::DifferentSql(id.to_owned()));
            }
            return match op.state {
                State::Done => Ok(Claim::Replay),
                State::InFlight => Err(SchemaError::InFlight(id.to_owned())),
                State::Failed => {
                    op.state = State::InFlight;
                    Ok(Claim::Execute)
                }
            };
        }
        ops.insert(
            id.to_owned(),
            Op {
                sql: sql.to_owned(),
                state: State::InFlight,
            },
        );
        Ok(Claim::Execute)
    }

    /// Record that the claimed statement completed.
    pub fn mark_done(&self, id: &str) {
        self.set_state(id, State::Done);
    }

    /// Record that the claimed statement failed, keeping the id's sql binding so
    /// a same-sql retry re-executes while a different-sql reuse is still
    /// rejected.
    pub fn mark_failed(&self, id: &str) {
        self.set_state(id, State::Failed);
    }

    fn set_state(&self, id: &str, state: State) {
        if let Some(op) = self
            .ops
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(id)
        {
            op.state = state;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_id_is_rejected() {
        assert_eq!(
            SchemaLog::new().claim("", "SELECT 1"),
            Err(SchemaError::EmptyId)
        );
    }

    #[test]
    fn completed_identical_replays_different_sql_conflicts() {
        let log = SchemaLog::new();
        assert_eq!(log.claim("op1", "CREATE TABLE t()"), Ok(Claim::Execute));
        log.mark_done("op1");
        assert_eq!(log.claim("op1", "CREATE TABLE t()"), Ok(Claim::Replay));
        assert_eq!(
            log.claim("op1", "DROP TABLE t"),
            Err(SchemaError::DifferentSql("op1".into()))
        );
    }

    #[test]
    fn concurrent_duplicate_is_rejected_while_in_flight() {
        let log = SchemaLog::new();
        assert_eq!(log.claim("op2", "CREATE INDEX ..."), Ok(Claim::Execute));
        assert_eq!(
            log.claim("op2", "CREATE INDEX ..."),
            Err(SchemaError::InFlight("op2".into()))
        );
    }

    #[test]
    fn a_failed_op_retries_same_sql_but_still_rejects_different_sql() {
        let log = SchemaLog::new();
        assert_eq!(log.claim("op3", "SELECT 1"), Ok(Claim::Execute));
        log.mark_failed("op3");
        // The retry re-executes rather than replaying a success that never happened.
        assert_eq!(log.claim("op3", "SELECT 1"), Ok(Claim::Execute));
        log.mark_failed("op3");
        // Reusing the id with different sql stays rejected even after a failure.
        assert_eq!(
            log.claim("op3", "SELECT 2"),
            Err(SchemaError::DifferentSql("op3".into()))
        );
    }
}
