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

#[derive(Clone)]
struct Op {
    sql: String,
    done: bool,
}

#[derive(Default)]
pub struct SchemaLog {
    ops: Mutex<HashMap<String, Op>>,
}

impl SchemaLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserve `id` for `sql`. A repeat of a completed identical statement
    /// replays; a repeat while the first is still running is rejected; the same
    /// id with different sql is rejected.
    pub fn claim(&self, id: &str, sql: &str) -> Result<Claim, SchemaError> {
        if id.is_empty() {
            return Err(SchemaError::EmptyId);
        }
        let mut ops = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(op) = ops.get(id) {
            if op.sql != sql {
                return Err(SchemaError::DifferentSql(id.to_owned()));
            }
            return if op.done {
                Ok(Claim::Replay)
            } else {
                Err(SchemaError::InFlight(id.to_owned()))
            };
        }
        ops.insert(
            id.to_owned(),
            Op {
                sql: sql.to_owned(),
                done: false,
            },
        );
        Ok(Claim::Execute)
    }

    /// Record that the claimed statement completed.
    pub fn mark_done(&self, id: &str) {
        if let Some(op) = self
            .ops
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(id)
        {
            op.done = true;
        }
    }

    /// Release a claim whose statement failed, so a retry re-executes it.
    pub fn rollback(&self, id: &str) {
        self.ops
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
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
    fn a_failed_op_re_executes_after_rollback() {
        let log = SchemaLog::new();
        assert_eq!(log.claim("op3", "SELECT 1"), Ok(Claim::Execute));
        log.rollback("op3");
        // The retry re-executes rather than replaying a success that never happened.
        assert_eq!(log.claim("op3", "SELECT 1"), Ok(Claim::Execute));
    }
}
