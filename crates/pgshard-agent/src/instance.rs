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

/// Prefix of the database-comment marker binding a shard database to the
/// placement (shard UID) that created it.
pub const PROVENANCE_PREFIX: &str = "pgshard-provenance:";

/// The full comment marker for a provenance value.
pub fn provenance_marker(provenance: &str) -> String {
    format!("{PROVENANCE_PREFIX}{provenance}")
}

/// A same-named database exists but carries a different or missing provenance
/// marker: another placement's data (retained volume, reshard leftover), which
/// must never be adopted silently. The service maps this to
/// FAILED_PRECONDITION; taking the database over requires the request's
/// explicit `adopt` authorization.
#[derive(Debug)]
pub struct ForeignDatabase {
    pub name: String,
    /// The marker found on the existing database, if any.
    pub found: Option<String>,
}

impl std::error::Error for ForeignDatabase {}

impl std::fmt::Display for ForeignDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.found {
            Some(marker) => write!(
                f,
                "database {:?} already exists with provenance marker {marker:?}; \
                 refusing to adopt without explicit authorization",
                self.name
            ),
            None => write!(
                f,
                "database {:?} already exists without a provenance marker; \
                 refusing to adopt without explicit authorization",
                self.name
            ),
        }
    }
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
    /// is nonempty. Idempotent for the SAME placement: with a nonempty
    /// `provenance` the marker is stamped as the database comment at creation,
    /// and an already existing database must carry the matching marker — a
    /// different or missing marker fails with [`ForeignDatabase`] unless
    /// `adopt` explicitly authorizes re-stamping it. An empty `provenance`
    /// skips stamping and verification.
    async fn create_database(
        &self,
        name: &str,
        owner: &str,
        provenance: &str,
        adopt: bool,
    ) -> anyhow::Result<()>;

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

    /// Emit a transactional logical message (prefix `pgshard`, the given
    /// payload) INTO `database` and return the WAL flush position after its
    /// commit. Logical messages are database-scoped: every replication slot
    /// of that database decodes the message's commit, which makes the
    /// returned LSN a database-local barrier — a consumer whose applied
    /// position reaches it has decoded everything before it.
    async fn emit_journal(&self, database: &str, payload: &[u8]) -> anyhow::Result<u64>;

    /// Ensure `publication` exists in `database` publishing EXACTLY `tables`
    /// with every DML kind, no row filter, no column list, and no generated
    /// columns — the shape the seeding runner's preflight demands. A same-name
    /// publication already in that shape is left untouched (a live consumer's
    /// drift poll must not trip on reconcile retries); any other shape is
    /// dropped and recreated. Returns the WAL headroom for slot retention
    /// (`max_slot_wal_keep_size`), or None when unlimited.
    async fn prepare_source(
        &self,
        database: &str,
        publication: &str,
        tables: &[(String, String)],
    ) -> anyhow::Result<Option<u64>>;
}

/// An in-memory instance for tests: `snapshot` returns the scripted state and
/// the commands mutate it the way a real instance would.
#[cfg(test)]
pub mod fake {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// (database, publication) -> published (schema, table) pairs.
    pub type Publications = BTreeMap<(String, String), Vec<(String, String)>>;

    #[derive(Clone)]
    pub struct RestorePointGate {
        entered: Option<std::sync::Arc<tokio::sync::Notify>>,
        gate: std::sync::Arc<tokio::sync::Notify>,
    }

    #[derive(Default)]
    pub struct FakeInstance {
        state: Mutex<Snapshot>,
        promote_fails: std::sync::atomic::AtomicBool,
        exec_fails: std::sync::atomic::AtomicBool,
        db_fails: std::sync::atomic::AtomicBool,
        executed: Mutex<Vec<String>>,
        /// Databases that exist, name -> (owner, provenance marker). Empty
        /// owner = bootstrap role; `None` marker = no comment stamped.
        databases: Mutex<BTreeMap<String, (String, Option<String>)>>,
        /// Restore points created, in order.
        restore_points: Mutex<Vec<String>>,
        /// When set, create_restore_point signals `entered`, then parks on the
        /// gate before executing (one-shot: the gate is consumed by the first
        /// call that reaches it) — lets tests hold a call mid-flight to prove
        /// single-flight and cancellation-safety semantics deterministically.
        restore_point_gate: Mutex<Option<RestorePointGate>>,
        wal_switches: std::sync::atomic::AtomicU32,
        publications: Mutex<Publications>,
        journals: Mutex<Vec<(String, Vec<u8>)>>,
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
            self.databases
                .lock()
                .unwrap()
                .get(name)
                .map(|(owner, _)| owner.clone())
        }
        pub fn marker_of(&self, name: &str) -> Option<String> {
            self.databases
                .lock()
                .unwrap()
                .get(name)
                .and_then(|(_, marker)| marker.clone())
        }
        /// Seed a pre-existing database with an arbitrary marker (or none), as
        /// a retained volume from another placement would leave behind.
        pub fn seed_database(&self, name: &str, owner: &str, marker: Option<&str>) {
            self.databases.lock().unwrap().insert(
                name.to_owned(),
                (owner.to_owned(), marker.map(str::to_owned)),
            );
        }
        pub fn publications(&self) -> Publications {
            self.publications.lock().unwrap().clone()
        }

        pub fn journals(&self) -> Vec<(String, Vec<u8>)> {
            self.journals.lock().unwrap().clone()
        }

        pub fn restore_points(&self) -> Vec<String> {
            self.restore_points.lock().unwrap().clone()
        }
        pub fn set_restore_point_gate(&self, gate: std::sync::Arc<tokio::sync::Notify>) {
            *self.restore_point_gate.lock().unwrap() = Some(RestorePointGate {
                entered: None,
                gate,
            });
        }
        /// Like [`Self::set_restore_point_gate`], additionally signalling
        /// `entered` the moment the call reaches PostgreSQL — the handshake a
        /// cancellation test needs to abort the caller at exactly the right
        /// instant.
        pub fn set_restore_point_gate_with_entered(
            &self,
            entered: std::sync::Arc<tokio::sync::Notify>,
            gate: std::sync::Arc<tokio::sync::Notify>,
        ) {
            *self.restore_point_gate.lock().unwrap() = Some(RestorePointGate {
                entered: Some(entered),
                gate,
            });
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
        async fn create_database(
            &self,
            name: &str,
            owner: &str,
            provenance: &str,
            adopt: bool,
        ) -> anyhow::Result<()> {
            if self.db_fails.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("create database failed");
            }
            let mut dbs = self.databases.lock().unwrap();
            let want = (!provenance.is_empty()).then(|| provenance_marker(provenance));
            match dbs.get_mut(name) {
                None => {
                    dbs.insert(name.to_owned(), (owner.to_owned(), want));
                }
                Some(_) if provenance.is_empty() => {}
                Some((_, marker)) if *marker == want => {}
                Some((_, marker)) if adopt => *marker = want,
                Some((_, marker)) => {
                    return Err(ForeignDatabase {
                        name: name.to_owned(),
                        found: marker.clone(),
                    }
                    .into());
                }
            }
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
            let (in_recovery, lsn, timeline) = {
                let s = self.state.lock().unwrap();
                (s.in_recovery, s.write_lsn, s.timeline)
            };
            // PostgreSQL rejects restore points during recovery; model that.
            anyhow::ensure!(!in_recovery, "cannot create a restore point on a standby");
            let gate = self.restore_point_gate.lock().unwrap().take();
            if let Some(g) = gate {
                if let Some(entered) = &g.entered {
                    entered.notify_one();
                }
                g.gate.notified().await;
            }
            self.restore_points.lock().unwrap().push(name.to_owned());
            Ok(RestorePoint { lsn, timeline })
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

        async fn emit_journal(&self, database: &str, payload: &[u8]) -> anyhow::Result<u64> {
            anyhow::ensure!(
                self.databases.lock().unwrap().contains_key(database),
                "database {database} does not exist"
            );
            self.journals
                .lock()
                .unwrap()
                .push((database.to_owned(), payload.to_vec()));
            Ok(self.state.lock().unwrap().write_lsn)
        }

        async fn prepare_source(
            &self,
            database: &str,
            publication: &str,
            tables: &[(String, String)],
        ) -> anyhow::Result<Option<u64>> {
            anyhow::ensure!(
                self.databases.lock().unwrap().contains_key(database),
                "database {database} does not exist"
            );
            self.publications.lock().unwrap().insert(
                (database.to_owned(), publication.to_owned()),
                tables.to_vec(),
            );
            Ok(Some(1 << 30))
        }
    }
}
