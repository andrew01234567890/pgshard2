//! The decision-epoch guard.
//!
//! The operator fences a failover with a strictly monotonic epoch carried on
//! every `Promote`/`Fence`/`RejoinAsStandby`. The agent persists the highest
//! epoch it has applied together with the request that carried it, so a delayed
//! message from an older failover can never reverse a newer decision:
//!   - epoch 0 is rejected;
//!   - a lower epoch is rejected as stale;
//!   - an equal epoch is accepted only when the request is byte-identical to the
//!     one already applied (an idempotent retry), and the caller then replays
//!     its prior result without re-executing;
//!   - a higher epoch is applied.

use std::sync::Mutex;

/// What the guard tells the caller to do for an accepted request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Fresh, higher (or first) epoch: execute the command.
    Apply,
    /// Identical equal-epoch retry: return the prior result, do not re-execute.
    Replay,
}

/// Why an epoch was refused. Mapped to gRPC status codes by the service.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EpochError {
    #[error("decision_epoch must be > 0")]
    Zero,
    #[error("stale decision epoch {got} < {applied}")]
    Stale { got: u64, applied: u64 },
    #[error("decision epoch {epoch} already applied a different request")]
    Conflict { epoch: u64 },
}

#[derive(Default)]
struct Applied {
    epoch: u64,
    key: String,
    seen: bool,
}

/// A per-instance guard. `key` uniquely identifies the request payload, e.g.
/// `"promote:<target>"` or `"fence:true"`.
#[derive(Default)]
pub struct EpochGuard {
    applied: Mutex<Applied>,
}

impl EpochGuard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn check(&self, epoch: u64, key: &str) -> Result<Outcome, EpochError> {
        if epoch == 0 {
            return Err(EpochError::Zero);
        }
        // Recover a poisoned lock: the guarded state is a few plain fields, so a
        // panic in an unrelated command can never leave it inconsistent.
        let a = self.applied.lock().unwrap_or_else(|e| e.into_inner());
        if a.seen && epoch < a.epoch {
            return Err(EpochError::Stale {
                got: epoch,
                applied: a.epoch,
            });
        }
        if a.seen && epoch == a.epoch {
            if a.key != key {
                return Err(EpochError::Conflict { epoch });
            }
            return Ok(Outcome::Replay);
        }
        // Apply is only a reservation: the epoch is not recorded until the
        // command actually succeeds (see `commit`). Otherwise a command that
        // failed after `check` would leave its epoch committed, and a retry would
        // `Replay` a success that never happened.
        Ok(Outcome::Apply)
    }

    /// Record the epoch as applied after the command succeeded, so an identical
    /// retry replays it and a stale epoch is rejected. Only ever advances.
    pub fn commit(&self, epoch: u64, key: &str) {
        let mut a = self.applied.lock().unwrap_or_else(|e| e.into_inner());
        if !a.seen || epoch > a.epoch {
            a.epoch = epoch;
            a.key = key.to_owned();
            a.seen = true;
        }
    }

    pub fn applied_epoch(&self) -> u64 {
        self.applied.lock().unwrap_or_else(|e| e.into_inner()).epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_rejected() {
        let g = EpochGuard::new();
        assert_eq!(g.check(0, "promote:a"), Err(EpochError::Zero));
    }

    #[test]
    fn commit_advances_after_success() {
        let g = EpochGuard::new();
        assert_eq!(g.check(1, "promote:a"), Ok(Outcome::Apply));
        // Not recorded until commit.
        assert_eq!(g.applied_epoch(), 0);
        g.commit(1, "promote:a");
        assert_eq!(g.applied_epoch(), 1);
        assert_eq!(g.check(5, "promote:b"), Ok(Outcome::Apply));
        g.commit(5, "promote:b");
        assert_eq!(g.applied_epoch(), 5);
    }

    #[test]
    fn an_uncommitted_apply_retries_instead_of_replaying() {
        let g = EpochGuard::new();
        // The command was reserved but failed, so it was never committed.
        assert_eq!(g.check(1, "promote:a"), Ok(Outcome::Apply));
        // The retry must Apply again (re-execute), not Replay a success.
        assert_eq!(g.check(1, "promote:a"), Ok(Outcome::Apply));
    }

    #[test]
    fn lower_is_stale() {
        let g = EpochGuard::new();
        g.commit(5, "promote:a");
        assert_eq!(
            g.check(3, "promote:a"),
            Err(EpochError::Stale { got: 3, applied: 5 })
        );
    }

    #[test]
    fn equal_same_key_replays_equal_other_key_conflicts() {
        let g = EpochGuard::new();
        g.commit(2, "promote:a");
        assert_eq!(g.check(2, "promote:a"), Ok(Outcome::Replay));
        assert_eq!(
            g.check(2, "promote:b"),
            Err(EpochError::Conflict { epoch: 2 })
        );
    }
}
