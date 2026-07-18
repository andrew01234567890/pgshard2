//! The instance abstraction: everything the agent does to the local PostgreSQL
//! it supervises. Keeping it behind a trait lets the gRPC service and its state
//! machine be tested without a running database; the real implementation
//! (`crate::pg::PgInstance`) talks to PostgreSQL over libpq.

use async_trait::async_trait;

/// A named consistency point created on the primary: the LSN it recorded and
/// the timeline it was on. A cross-shard barrier records one of these per shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestorePoint {
    pub lsn: u64,
    pub timeline: u32,
}

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

    /// Create a named restore point (`pg_create_restore_point`) and return its
    /// LSN and timeline. Only valid on a primary — the caller targets one.
    async fn create_restore_point(&self, name: &str) -> anyhow::Result<RestorePoint>;

    /// Force a WAL segment switch (`pg_switch_wal`), returning the switch LSN.
    /// When `wait_archived`, block until that segment is confirmed archived so
    /// the point is immediately restorable. Only valid on a primary.
    async fn switch_wal(&self, wait_archived: bool) -> anyhow::Result<u64>;
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
        /// Restore points created, in order.
        restore_points: Mutex<Vec<String>>,
        wal_switches: std::sync::atomic::AtomicU32,
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
        pub fn restore_points(&self) -> Vec<String> {
            self.restore_points.lock().unwrap().clone()
        }
        pub fn wal_switches(&self) -> u32 {
            self.wal_switches.load(std::sync::atomic::Ordering::SeqCst)
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
        async fn create_restore_point(&self, name: &str) -> anyhow::Result<RestorePoint> {
            let s = self.state.lock().unwrap();
            // PostgreSQL rejects restore points during recovery; model that.
            anyhow::ensure!(!s.in_recovery, "cannot create a restore point on a standby");
            self.restore_points.lock().unwrap().push(name.to_owned());
            Ok(RestorePoint {
                lsn: s.write_lsn,
                timeline: s.timeline,
            })
        }
        async fn switch_wal(&self, wait_archived: bool) -> anyhow::Result<u64> {
            // Mirror the real instance's contract so the fast unit path enforces
            // it: the archive-wait is not implemented yet.
            anyhow::ensure!(
                !wait_archived,
                "switch_wal with wait_archived is not implemented yet"
            );
            let s = self.state.lock().unwrap();
            anyhow::ensure!(!s.in_recovery, "cannot switch WAL on a standby");
            self.wal_switches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(s.write_lsn)
        }
    }
}
