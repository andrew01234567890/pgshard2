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

    /// Execute a schema/DDL statement (idempotency is handled by the caller).
    async fn exec_sql(&self, sql: &str) -> anyhow::Result<()>;

    /// Ensure a Postgres DATABASE named `name` exists, owned by `owner` when it
    /// is nonempty. Idempotent: succeeds when the database already exists.
    async fn create_database(&self, name: &str, owner: &str) -> anyhow::Result<()>;

    /// Ensure the Postgres DATABASE named `name` does not exist, terminating any
    /// sessions still connected to it. Idempotent: succeeds when the database is
    /// already absent.
    async fn drop_database(&self, name: &str) -> anyhow::Result<()>;
}

/// An in-memory instance for tests: `snapshot` returns the scripted state and
/// the commands mutate it the way a real instance would.
#[cfg(test)]
pub mod fake {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct FakeInstance {
        state: Mutex<Snapshot>,
        promote_fails: std::sync::atomic::AtomicBool,
        exec_fails: std::sync::atomic::AtomicBool,
        db_fails: std::sync::atomic::AtomicBool,
        executed: Mutex<Vec<String>>,
        /// Databases that exist, name -> owner (empty owner = bootstrap role).
        databases: Mutex<BTreeMap<String, String>>,
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
                ..Default::default()
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
                ..Default::default()
            }
        }
        pub fn set<F: FnOnce(&mut Snapshot)>(&self, f: F) {
            f(&mut self.state.lock().unwrap());
        }
        pub fn set_promote_fails(&self, fails: bool) {
            self.promote_fails
                .store(fails, std::sync::atomic::Ordering::SeqCst);
        }
        pub fn set_exec_fails(&self, fails: bool) {
            self.exec_fails
                .store(fails, std::sync::atomic::Ordering::SeqCst);
        }
        pub fn set_db_fails(&self, fails: bool) {
            self.db_fails
                .store(fails, std::sync::atomic::Ordering::SeqCst);
        }
        pub fn executed(&self) -> Vec<String> {
            self.executed.lock().unwrap().clone()
        }
        pub fn databases(&self) -> Vec<String> {
            self.databases.lock().unwrap().keys().cloned().collect()
        }
        pub fn owner_of(&self, name: &str) -> Option<String> {
            self.databases.lock().unwrap().get(name).cloned()
        }
    }

    #[async_trait]
    impl Instance for FakeInstance {
        async fn snapshot(&self) -> anyhow::Result<Snapshot> {
            Ok(self.state.lock().unwrap().clone())
        }
        async fn promote(&self) -> anyhow::Result<u32> {
            if self.promote_fails.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("promote failed");
            }
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
        async fn exec_sql(&self, sql: &str) -> anyhow::Result<()> {
            if self.exec_fails.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("exec failed");
            }
            self.executed.lock().unwrap().push(sql.to_owned());
            Ok(())
        }
        async fn create_database(&self, name: &str, owner: &str) -> anyhow::Result<()> {
            if self.db_fails.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("create database failed");
            }
            self.databases
                .lock()
                .unwrap()
                .entry(name.to_owned())
                .or_insert_with(|| owner.to_owned());
            Ok(())
        }
        async fn drop_database(&self, name: &str) -> anyhow::Result<()> {
            if self.db_fails.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("drop database failed");
            }
            self.databases.lock().unwrap().remove(name);
            Ok(())
        }
    }
}
