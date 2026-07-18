//! Topology watching for the router and agents.
//!
//! The compiled routing view (PgShardRouting in Kubernetes, a JSON file in
//! tests and non-k8s development) is applied through a single rule:
//! an update is accepted iff its epoch is strictly greater than the last
//! applied epoch. Consumers hold a `tokio::sync::watch` receiver and always
//! see the newest accepted snapshot.

pub mod file;
pub mod model;

use std::sync::Arc;

use tokio::sync::watch;

pub use file::FileWatcher;
pub use model::{
    GateMatch, GateMode, GateSpec, Instance, Sequence, ShardEntry, ShardKeyType, ShardState,
    TableEntry, TableType, Topology,
};

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("invalid topology: {0}")]
    Invalid(String),
}

/// A source of topology snapshots. Implementations push validated,
/// epoch-ordered snapshots into the watch channel.
pub trait TopologyWatcher {
    fn subscribe(&self) -> watch::Receiver<Arc<Topology>>;
}

/// The instant a watcher last successfully read AND validated its source —
/// bumped on every successful poll, including one whose epoch was unchanged
/// (an unchanged-but-readable source still confirms the view is current), and
/// never on an error. Distinct from receiving a new snapshot: this is what
/// lets a consumer bound how stale its view can possibly be — the router's
/// write lease rejects writes once the age exceeds the topology's
/// `write_lease_seconds`, so a router cut off from its source cannot keep
/// writing against a routing world that may have moved on.
#[derive(Clone)]
pub struct Freshness(Arc<std::sync::RwLock<(std::time::Instant, std::time::SystemTime)>>);

impl Freshness {
    pub fn new() -> Self {
        Self(Arc::new(std::sync::RwLock::new((
            std::time::Instant::now(),
            std::time::SystemTime::now(),
        ))))
    }

    /// Record a successful confirmation now.
    pub fn bump(&self) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) =
            (std::time::Instant::now(), std::time::SystemTime::now());
    }

    /// Install a source-captured confirmation: the stamp is the moment the
    /// source was VALIDATED, not the moment this call runs — a delivery delayed
    /// across a suspend must not launder a pre-suspend validation into a fresh
    /// one.
    pub fn install(&self, v: &SourceValidation) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = (v.instant, v.wall);
    }

    /// How long ago the view was last confirmed. Takes the LARGER of the
    /// monotonic and wall-clock elapsed times: Linux's monotonic clock stops
    /// during system suspend, so a resumed node would otherwise think a
    /// pre-suspend confirmation is recent. A wall clock that stepped BACKWARD
    /// past the stamp is indistinguishable from a suspend-plus-step that
    /// under-reports both clocks, so it reports maximal age — a false expiry
    /// fails closed and the next renewal clears it within a poll interval.
    pub fn age(&self) -> std::time::Duration {
        let (instant, wall) = *self.0.read().unwrap_or_else(|e| e.into_inner());
        let monotonic = instant.elapsed();
        let wall_elapsed = match std::time::SystemTime::now().duration_since(wall) {
            Ok(d) => d,
            Err(_) => return std::time::Duration::MAX,
        };
        monotonic.max(wall_elapsed)
    }

    /// Test hook: pretend the last confirmation happened `by` ago.
    #[doc(hidden)]
    pub fn backdate(&self, by: std::time::Duration) {
        let mut guard = self.0.write().unwrap_or_else(|e| e.into_inner());
        if let Some(at) = std::time::Instant::now().checked_sub(by) {
            *guard = (at, std::time::SystemTime::now() - by);
        }
    }
}

impl Default for Freshness {
    fn default() -> Self {
        Self::new()
    }
}

/// One successful source read+validate: the epoch it observed and the clocks
/// captured AT VALIDATION TIME (both, for the same suspend-awareness as
/// [`Freshness::age`]). Consumers install these stamps rather than stamping
/// delivery time, so a delayed delivery never inflates freshness.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceValidation {
    pub epoch: u64,
    pub instant: std::time::Instant,
    pub wall: std::time::SystemTime,
}

impl SourceValidation {
    pub fn now(epoch: u64) -> Self {
        Self {
            epoch,
            instant: std::time::Instant::now(),
            wall: std::time::SystemTime::now(),
        }
    }
}

/// Validates a candidate snapshot: the epoch and topology generation are the
/// CRD's 1-based counters, the shard function must be known, and serving shards
/// must partition the full keyspace. Serving shards are sorted before the
/// partition check so a snapshot whose shard list is not already start-ordered
/// is not spuriously rejected.
pub fn validate(topology: &Topology) -> Result<(), TopologyError> {
    if topology.epoch == 0 {
        return Err(TopologyError::Invalid("epoch must be >= 1".into()));
    }
    // Mirrors the cluster CRD's bounds: 0 would refuse every write, and an
    // enormous value effectively disables stale-write fencing.
    if !(1..=60).contains(&topology.write_lease_seconds) {
        return Err(TopologyError::Invalid(format!(
            "writeLeaseSeconds {} must be between 1 and 60",
            topology.write_lease_seconds
        )));
    }
    if topology.topology_generation == 0 {
        return Err(TopologyError::Invalid(
            "topologyGeneration must be >= 1".into(),
        ));
    }
    pgshard_core::shard_function(&topology.hash_function)
        .map_err(|e| TopologyError::Invalid(e.to_string()))?;
    let mut serving: Vec<_> = topology
        .shards
        .iter()
        .filter(|s| s.state == ShardState::Serving)
        .map(|s| s.key_range)
        .collect();
    serving.sort_by_key(|kr| kr.start());
    pgshard_core::validate_partition(&serving)
        .map_err(|e| TopologyError::Invalid(format!("serving shards: {e}")))?;
    Ok(())
}
