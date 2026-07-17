//! The instance abstraction: everything the agent does to the local PostgreSQL
//! it supervises. Keeping it behind a trait lets the gRPC service and its state
//! machine be tested without a running database; the real implementation
//! (`crate::pg::PgInstance`) talks to PostgreSQL over libpq.

use async_trait::async_trait;

/// A point-in-time reading of the local instance, from which the agent builds
/// the `InstanceStatus` the operator polls.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// `pg_is_in_recovery()`: true on a standby, false on a primary.
    pub in_recovery: bool,
    /// Ready to accept client connections.
    pub accepting: bool,
    pub timeline: u32,
    /// `pg_current_wal_lsn()` on a primary.
    pub write_lsn: u64,
    /// `pg_last_wal_receive_lsn()` on a standby.
    pub receive_lsn: u64,
    /// `pg_last_wal_replay_lsn()` on a standby.
    pub replay_lsn: u64,
    /// A walreceiver process is streaming.
    pub receiver_active: bool,
    pub postgres_version: String,
    pub system_id: u64,
    /// The agent is holding PostgreSQL down (fenced).
    pub fenced: bool,
}

#[async_trait]
pub trait Instance: Send + Sync + 'static {
    /// Read the current state. Errors when the instance is unreachable.
    async fn snapshot(&self) -> anyhow::Result<Snapshot>;

    /// Promote this standby to primary; returns the new timeline.
    async fn promote(&self) -> anyhow::Result<u32>;

    /// Fence (keep PostgreSQL down) or lift the fence.
    async fn set_fenced(&self, fenced: bool) -> anyhow::Result<()>;

    /// Rejoin as a standby of `upstream`, optionally running pg_rewind first;
    /// returns whether a rewind was performed.
    async fn rejoin(&self, upstream: &str, allow_rewind: bool) -> anyhow::Result<bool>;
}

/// An in-memory instance for tests: `snapshot` returns the scripted state and
/// the commands mutate it the way a real instance would.
#[cfg(test)]
pub mod fake {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct FakeInstance {
        state: Mutex<Snapshot>,
    }

    impl FakeInstance {
        pub fn primary() -> Self {
            Self {
                state: Mutex::new(Snapshot {
                    in_recovery: false,
                    accepting: true,
                    timeline: 1,
                    ..Default::default()
                }),
            }
        }
        pub fn standby() -> Self {
            Self {
                state: Mutex::new(Snapshot {
                    in_recovery: true,
                    accepting: true,
                    timeline: 1,
                    receiver_active: true,
                    ..Default::default()
                }),
            }
        }
        pub fn set<F: FnOnce(&mut Snapshot)>(&self, f: F) {
            f(&mut self.state.lock().unwrap());
        }
    }

    #[async_trait]
    impl Instance for FakeInstance {
        async fn snapshot(&self) -> anyhow::Result<Snapshot> {
            Ok(self.state.lock().unwrap().clone())
        }
        async fn promote(&self) -> anyhow::Result<u32> {
            let mut s = self.state.lock().unwrap();
            s.in_recovery = false;
            s.receiver_active = false;
            s.timeline += 1;
            Ok(s.timeline)
        }
        async fn set_fenced(&self, fenced: bool) -> anyhow::Result<()> {
            self.state.lock().unwrap().fenced = fenced;
            Ok(())
        }
        async fn rejoin(&self, _upstream: &str, allow_rewind: bool) -> anyhow::Result<bool> {
            let mut s = self.state.lock().unwrap();
            s.in_recovery = true;
            s.receiver_active = true;
            Ok(allow_rewind)
        }
    }
}
